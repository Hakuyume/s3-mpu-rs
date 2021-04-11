mod split;

use bytes::Bytes;
use futures::future::Either;
use futures::{FutureExt, Stream, StreamExt};
use rusoto_core::{ByteStream, RusotoError};
use rusoto_s3::{
    CompleteMultipartUploadError, CompleteMultipartUploadOutput, CompleteMultipartUploadRequest,
    CompletedMultipartUpload, CompletedPart, CreateMultipartUploadError,
    CreateMultipartUploadRequest, UploadPartError, UploadPartRequest, S3,
};
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
    futures::pin_mut!(body);

    let upload_id = client
        .create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            ..CreateMultipartUploadRequest::default()
        })
        .await?
        .upload_id
        .unwrap();

    let parts = split::split(body, part_size);

    let mut futures = vec![Either::Left(parts.into_future().map(Either::Left))];
    let mut completed_parts = Vec::new();
    while !futures.is_empty() {
        let (output, _, remaining) = futures::future::select_all(futures.drain(..)).await;
        futures = remaining;
        match output {
            Either::Left((Some(part), parts)) => {
                let part = part?;
                futures.push(Either::Left(parts.into_future().map(Either::Left)));
                futures.push(Either::Right(
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
                        .map({
                            let part_number = part.part_number;
                            move |output| Either::Right((output, part_number))
                        }),
                ));
            }
            Either::Left((None, _)) => (),
            Either::Right((output, part_number)) => {
                let output = output?;
                completed_parts.push(CompletedPart {
                    e_tag: output.e_tag,
                    part_number: Some(part_number as _),
                });
            }
        }
    }

    completed_parts.sort_by_key(|completed_part| completed_part.part_number);
    Ok(client
        .complete_multipart_upload(CompleteMultipartUploadRequest {
            bucket,
            key,
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(completed_parts),
            }),
            upload_id,
            ..CompleteMultipartUploadRequest::default()
        })
        .await?)
}

#[cfg(test)]
mod tests;
