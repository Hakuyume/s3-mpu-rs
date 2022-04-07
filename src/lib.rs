mod into_byte_stream;
mod split;

use aws_sdk_s3::client::fluent_builders::AbortMultipartUpload;
use aws_sdk_s3::error::{
    CompleteMultipartUploadError, CreateMultipartUploadError, UploadPartError,
};
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::output::CompleteMultipartUploadOutput;
use aws_sdk_s3::types::{ByteStream, SdkError};
use aws_sdk_s3::Client;
use futures::{TryFutureExt, TryStreamExt};
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub const PART_SIZE: RangeInclusive<usize> = 5 << 20..=5 << 30;

pub struct MultipartUpload {
    client: Client,
    body: ByteStream,
    bucket: Option<String>,
    key: Option<String>,
}

pub type MultipartUploadOutput = CompleteMultipartUploadOutput;

impl MultipartUpload {
    pub fn new(client: &Client) -> Self {
        Self {
            client: client.clone(),
            body: ByteStream::default(),
            bucket: None,
            key: None,
        }
    }

    pub fn body(mut self, inp: ByteStream) -> Self {
        self.body = inp;
        self
    }

    pub fn bucket<S>(mut self, inp: S) -> Self
    where
        S: Into<String>,
    {
        self.bucket = Some(inp.into());
        self
    }

    pub fn key<S>(mut self, inp: S) -> Self
    where
        S: Into<String>,
    {
        self.key = Some(inp.into());
        self
    }

    pub async fn send<E>(
        self,
        part_size: RangeInclusive<usize>,
        concurrency_limit: Option<NonZeroUsize>,
    ) -> Result<MultipartUploadOutput, (E, Option<AbortMultipartUpload>)>
    where
        E: From<aws_smithy_http::byte_stream::Error>
            + From<SdkError<CreateMultipartUploadError>>
            + From<SdkError<UploadPartError>>
            + From<SdkError<CompleteMultipartUploadError>>,
    {
        let output = self
            .client
            .create_multipart_upload()
            .set_bucket(self.bucket.clone())
            .set_key(self.key.clone())
            .send()
            .map_err(|err| (err.into(), None))
            .await?;
        let upload_id = output.upload_id;

        let abort = || {
            self.client
                .abort_multipart_upload()
                .set_bucket(self.bucket.clone())
                .set_key(self.key.clone())
                .set_upload_id(upload_id.clone())
        };

        let parts = split::split(self.body, part_size)
            .map_ok(|part| {
                self.client
                    .upload_part()
                    .body(into_byte_stream::into_byte_stream(part.body))
                    .set_bucket(self.bucket.clone())
                    .content_length(part.content_length as _)
                    .content_md5(base64::encode(part.content_md5))
                    .set_key(self.key.clone())
                    .part_number(part.part_number as _)
                    .set_upload_id(upload_id.clone())
                    .send()
                    .map_ok({
                        move |output| {
                            CompletedPart::builder()
                                .set_e_tag(output.e_tag)
                                .part_number(part.part_number as _)
                                .build()
                        }
                    })
                    .err_into()
            })
            .err_into();

        let mut completed_parts = parts
            .try_buffer_unordered(concurrency_limit.map_or(usize::MAX, NonZeroUsize::get))
            .try_collect::<Vec<_>>()
            .map_err(|err| (err, Some(abort())))
            .await?;

        completed_parts.sort_by_key(|completed_part| completed_part.part_number);

        self.client
            .complete_multipart_upload()
            .set_bucket(self.bucket.clone())
            .set_key(self.key.clone())
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .set_upload_id(upload_id.clone())
            .send()
            .map_err(|err| (err.into(), Some(abort())))
            .await
    }
}

#[cfg(test)]
mod tests;
