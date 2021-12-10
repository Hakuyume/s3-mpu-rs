mod dispatch;
mod into_byte_stream;
mod split;

use aws_http::AwsErrorRetryPolicy;
use aws_sdk_s3::error::{
    AbortMultipartUploadError, CompleteMultipartUploadError, CreateMultipartUploadError,
    UploadPartError,
};
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::operation::{
    AbortMultipartUpload, CompleteMultipartUpload, CreateMultipartUpload, UploadPart,
};
use aws_sdk_s3::output::{
    AbortMultipartUploadOutput, CompleteMultipartUploadOutput, CreateMultipartUploadOutput,
    UploadPartOutput,
};
use aws_sdk_s3::{ByteStream, Client, SdkError};
use aws_smithy_client::bounds::{SmithyConnector, SmithyMiddleware, SmithyRetryPolicy};
use aws_smithy_client::retry::NewRequestPolicy;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub const PART_SIZE: RangeInclusive<usize> = 5 << 20..=5 << 30;

pub struct MultipartUpload<'a, C, M, R> {
    client: &'a Client<C, M, R>,
    body: ByteStream,
    bucket: Option<String>,
    key: Option<String>,
}

pub type MultipartUploadOutput = CompleteMultipartUploadOutput;

impl<'a, C, M, R> MultipartUpload<'a, C, M, R> {
    pub fn new(client: &'a Client<C, M, R>) -> Self {
        Self {
            client,
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
    ) -> Result<MultipartUploadOutput, E>
    where
        C: SmithyConnector,
        M: SmithyMiddleware<C>,
        R: NewRequestPolicy,
        R::Policy: SmithyRetryPolicy<
                CreateMultipartUpload,
                CreateMultipartUploadOutput,
                CreateMultipartUploadError,
                AwsErrorRetryPolicy,
            > + SmithyRetryPolicy<UploadPart, UploadPartOutput, UploadPartError, AwsErrorRetryPolicy>
            + SmithyRetryPolicy<
                CompleteMultipartUpload,
                CompleteMultipartUploadOutput,
                CompleteMultipartUploadError,
                AwsErrorRetryPolicy,
            > + SmithyRetryPolicy<
                AbortMultipartUpload,
                AbortMultipartUploadOutput,
                AbortMultipartUploadError,
                AwsErrorRetryPolicy,
            >,
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
            .await?;
        let upload_id = output.upload_id.as_ref().unwrap();

        let stream = split::split(self.body, part_size)
            .map_ok(|part| {
                Box::pin(
                    self.client
                        .upload_part()
                        .body(into_byte_stream::into_byte_stream(part.body))
                        .set_bucket(self.bucket.clone())
                        .content_length(part.content_length as _)
                        .content_md5(base64::encode(part.content_md5))
                        .set_key(self.key.clone())
                        .part_number(part.part_number as _)
                        .upload_id(upload_id)
                        .send()
                        .map_ok({
                            move |output| {
                                CompletedPart::builder()
                                    .e_tag(output.e_tag.unwrap())
                                    .part_number(part.part_number as _)
                                    .build()
                            }
                        })
                        .err_into(),
                )
            })
            .map_err(E::from);

        (async {
            let mut completed_parts =
                dispatch::dispatch_concurrent(stream, concurrency_limit).await?;
            completed_parts.sort_by_key(|completed_part| completed_part.part_number);

            let output = self
                .client
                .complete_multipart_upload()
                .set_bucket(self.bucket.clone())
                .set_key(self.key.clone())
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(completed_parts))
                        .build(),
                )
                .upload_id(upload_id)
                .send()
                .await?;

            Ok(output)
        })
        .or_else(|e| {
            self.client
                .abort_multipart_upload()
                .set_bucket(self.bucket.clone())
                .set_key(self.key.clone())
                .upload_id(upload_id)
                .send()
                .map(|_| Err(e))
        })
        .await
    }
}

#[cfg(test)]
mod tests;
