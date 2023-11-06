mod into_byte_stream;
mod split;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::abort_multipart_upload::builders::AbortMultipartUploadFluentBuilder;
use aws_sdk_s3::operation::complete_multipart_upload::{
    CompleteMultipartUploadError, CompleteMultipartUploadOutput,
};
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use futures::{Stream, TryFutureExt, TryStreamExt};
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::task::{Context, Poll};

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub const PART_SIZE: RangeInclusive<usize> = 5 << 20..=5 << 30;

pub struct MultipartUpload {
    client: Client,
    body: ByteStream,
    bucket: Option<String>,
    key: Option<String>,
}

pub(crate) struct WrappedByteStream(ByteStream);

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
    ) -> Result<MultipartUploadOutput, (E, Option<AbortMultipartUploadFluentBuilder>)>
    where
        E: From<aws_smithy_types::byte_stream::error::Error>
            + From<SdkError<CreateMultipartUploadError, http::Response<SdkBody>>>
            + From<SdkError<UploadPartError, http::Response<SdkBody>>>
            + From<SdkError<CompleteMultipartUploadError, http::Response<SdkBody>>>,
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

        let parts = split::split(WrappedByteStream::new(self.body), part_size)
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

impl WrappedByteStream {
    fn new(stream: ByteStream) -> Self {
        Self(stream)
    }
}

impl Unpin for WrappedByteStream {}

impl Stream for WrappedByteStream {
    type Item = Result<Bytes, ByteStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

#[cfg(test)]
mod tests;
