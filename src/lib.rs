mod split;

use bytes::Bytes;
use futures::future::Either;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use rusoto_core::{ByteStream, RusotoError};
use rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadError, CompleteMultipartUploadOutput,
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadError, CreateMultipartUploadRequest, UploadPartError, UploadPartRequest,
    S3,
};
use std::future::Future;
use std::ops::RangeInclusive;

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub const PART_SIZE: RangeInclusive<usize> = 5 << 20..=5 << 30;

pub struct MultipartUploadRequest<B, E>
where
    B: Stream<Item = Result<Bytes, E>>,
{
    pub body: B,
    pub bucket: String,
    pub key: String,
}

pub type MultipartUploadOutput = CompleteMultipartUploadOutput;

pub async fn multipart_upload<C, B, E>(
    client: &C,
    input: MultipartUploadRequest<B, E>,
    part_size: RangeInclusive<usize>,
) -> Result<MultipartUploadOutput, E>
where
    C: S3,
    B: Stream<Item = Result<Bytes, E>>,
    E: From<RusotoError<CreateMultipartUploadError>>
        + From<RusotoError<UploadPartError>>
        + From<RusotoError<CompleteMultipartUploadError>>,
{
    let MultipartUploadRequest { body, bucket, key } = input;

    let output = client
        .create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            ..CreateMultipartUploadRequest::default()
        })
        .await?;
    let upload_id = output.upload_id.as_ref().unwrap();

    let futures = split::split(body, part_size).map_ok(|part| {
        client
            .upload_part(UploadPartRequest {
                body: Some(ByteStream::new(futures::stream::iter(
                    part.body.into_iter().map(Ok),
                ))),
                bucket: bucket.clone(),
                content_length: Some(part.content_length as _),
                content_md5: Some(base64::encode(part.content_md5)),
                key: key.clone(),
                part_number: part.part_number as _,
                upload_id: upload_id.clone(),
                ..UploadPartRequest::default()
            })
            .map_ok({
                let part_number = part.part_number;
                move |output| CompletedPart {
                    e_tag: output.e_tag,
                    part_number: Some(part_number as _),
                }
            })
            .err_into()
    });

    (async {
        let mut completed_parts = dispatch_concurrent(futures).await?;
        completed_parts.sort_by_key(|completed_part| completed_part.part_number);

        let output = client
            .complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(completed_parts),
                }),
                upload_id: upload_id.clone(),
                ..CompleteMultipartUploadRequest::default()
            })
            .await?;

        Ok(output)
    })
    .or_else(|e| {
        client
            .abort_multipart_upload(AbortMultipartUploadRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                ..AbortMultipartUploadRequest::default()
            })
            .map(|_| Err(e))
    })
    .await
}

async fn dispatch_concurrent<S, F, T, E>(stream: S) -> Result<Vec<T>, E>
where
    S: Stream<Item = Result<F, E>>,
    F: Future<Output = Result<T, E>> + Unpin,
{
    futures::pin_mut!(stream);

    let mut futures = vec![Either::Left(stream.into_future().map(Either::Left))];
    let mut outputs = Vec::new();
    while !futures.is_empty() {
        let (output, _, remaining) = futures::future::select_all(futures.drain(..)).await;
        futures = remaining;
        match output {
            Either::Left((Some(future), stream)) => {
                futures.push(Either::Left(stream.into_future().map(Either::Left)));
                futures.push(Either::Right(future?.map(Either::Right)));
            }
            Either::Left((None, _)) => (),
            Either::Right(output) => outputs.push(output?),
        }
    }
    Ok(outputs)
}

#[cfg(test)]
mod tests;
