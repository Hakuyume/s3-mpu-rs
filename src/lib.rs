mod split;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use md5::digest::Output;
use md5::Md5;
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
    let mut multipart_upload = MultipartUpload::create(client, &input.bucket, &input.key).await?;

    let parts = split::split(input.body, part_size);
    futures::pin_mut!(parts);
    while let Some(part) = parts.next().await {
        let part = part?;
        multipart_upload
            .upload(part.body, part.content_length, part.content_md5)
            .await?;
    }

    Ok(multipart_upload.complete().await?)
}

struct MultipartUpload<'a, C> {
    client: &'a C,
    bucket: &'a str,
    key: &'a str,
    upload_id: String,
    parts: Vec<CompletedPart>,
    part_number: i64,
}

impl<'a, C> MultipartUpload<'a, C>
where
    C: S3,
{
    async fn create(
        client: &'a C,
        bucket: &'a str,
        key: &'a str,
    ) -> Result<MultipartUpload<'a, C>, RusotoError<CreateMultipartUploadError>> {
        let upload_id = client
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                ..CreateMultipartUploadRequest::default()
            })
            .await?
            .upload_id
            .unwrap();
        Ok(Self {
            client,
            bucket,
            key,
            upload_id,
            parts: Vec::new(),
            part_number: 1,
        })
    }

    async fn upload(
        &mut self,
        body: Vec<Bytes>,
        content_length: usize,
        content_md5: Output<Md5>,
    ) -> Result<(), RusotoError<UploadPartError>> {
        let e_tag = self
            .client
            .upload_part(UploadPartRequest {
                body: Some(ByteStream::new(futures::stream::iter(
                    body.into_iter().map(Ok),
                ))),
                bucket: self.bucket.to_owned(),
                content_length: Some(content_length as _),
                content_md5: Some(base64::encode(content_md5)),
                key: self.key.to_owned(),
                part_number: self.part_number,
                upload_id: self.upload_id.clone(),
                ..UploadPartRequest::default()
            })
            .await?
            .e_tag
            .unwrap();
        self.parts.push(CompletedPart {
            e_tag: Some(e_tag),
            part_number: Some(self.part_number),
        });
        self.part_number += 1;
        Ok(())
    }

    async fn complete(
        self,
    ) -> Result<CompleteMultipartUploadOutput, RusotoError<CompleteMultipartUploadError>> {
        self.client
            .complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: self.bucket.to_owned(),
                key: self.key.to_owned(),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(self.parts),
                }),
                upload_id: self.upload_id,
                ..CompleteMultipartUploadRequest::default()
            })
            .await
    }
}

#[cfg(test)]
mod tests;
