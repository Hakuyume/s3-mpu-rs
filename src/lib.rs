use bytes::Bytes;
use futures::{Stream, StreamExt};
use rusoto_core::{ByteStream, RusotoError};
use rusoto_s3::{
    CompleteMultipartUploadError, CompleteMultipartUploadOutput, CompleteMultipartUploadRequest,
    CompletedMultipartUpload, CompletedPart, CreateMultipartUploadError,
    CreateMultipartUploadRequest, UploadPartError, UploadPartRequest, S3,
};
use std::cmp;
use std::mem;
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
    part_size: &RangeInclusive<usize>,
) -> Result<MultipartUploadOutput, E>
where
    C: S3,
    B: Stream<Item = Result<Bytes, E>>,
    E: From<RusotoError<CreateMultipartUploadError>>
        + From<RusotoError<UploadPartError>>
        + From<RusotoError<CompleteMultipartUploadError>>,
{
    let body = input.body;
    futures::pin_mut!(body);

    let mut multipart_upload = MultipartUpload::create(client, &input.bucket, &input.key).await?;

    let mut chunks = Vec::new();
    let mut size = 0;
    while let Some(chunk) = body.next().await {
        let mut chunk = chunk?;
        while size + chunk.len() >= *part_size.start() {
            let len = cmp::min(chunk.len(), *part_size.end() - size);
            chunks.push(chunk.split_to(len));
            size += len;
            multipart_upload
                .upload(
                    mem::replace(&mut chunks, Vec::new()),
                    mem::replace(&mut size, 0) as _,
                )
                .await?;
        }
        if !chunk.is_empty() {
            let len = chunk.len();
            chunks.push(chunk);
            size += len;
        }
    }
    multipart_upload.upload(chunks, size as _).await?;

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
        content_length: i64,
    ) -> Result<(), RusotoError<UploadPartError>> {
        let e_tag = self
            .client
            .upload_part(UploadPartRequest {
                body: Some(ByteStream::new(futures::stream::iter(
                    body.into_iter().map(Ok),
                ))),
                bucket: self.bucket.to_owned(),
                content_length: Some(content_length),
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
