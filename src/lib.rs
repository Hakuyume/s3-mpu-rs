mod split;

use bytes::Bytes;
use futures::{Stream, TryFutureExt, TryStreamExt};
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

    let upload_id = client
        .create_multipart_upload(CreateMultipartUploadRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            ..CreateMultipartUploadRequest::default()
        })
        .await?
        .upload_id
        .unwrap();

    let upload_parts = split::split(body, part_size).map_ok(|part| {
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
    });

    let mut completed_parts = Vec::new();
    futures::pin_mut!(upload_parts);
    while let Some(upload_part) = upload_parts.try_next().await? {
        completed_parts.push(upload_part.await?);
    }

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
