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
use aws_sdk_s3::{Client, SdkError};
use aws_smithy_client::bounds::{SmithyConnector, SmithyMiddleware, SmithyRetryPolicy};
use aws_smithy_client::retry::NewRequestPolicy;
use bytes::Bytes;
use futures::{FutureExt, Stream, TryFutureExt, TryStreamExt};
use std::num::NonZeroUsize;
use std::ops::RangeInclusive;

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub const PART_SIZE: RangeInclusive<usize> = 5 << 20..=5 << 30;

pub struct MultipartUploadRequest<B> {
    pub body: B,
    pub bucket: String,
    pub key: String,
}

pub type MultipartUploadOutput = CompleteMultipartUploadOutput;

pub async fn multipart_upload<C, M, R, B, E>(
    client: &Client<C, M, R>,
    input: MultipartUploadRequest<B>,
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
    B: Stream<Item = Result<Bytes, E>>,
    E: From<SdkError<CreateMultipartUploadError>>
        + From<SdkError<UploadPartError>>
        + From<SdkError<CompleteMultipartUploadError>>,
{
    let MultipartUploadRequest { body, bucket, key } = input;

    let output = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await?;
    let upload_id = output.upload_id.as_ref().unwrap();

    let stream = split::split(body, part_size).map_ok(|part| {
        Box::pin(
            client
                .upload_part()
                .body(into_byte_stream::into_byte_stream(part.body))
                .bucket(&bucket)
                .content_length(part.content_length as _)
                .content_md5(base64::encode(part.content_md5))
                .key(&key)
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
    });

    (async {
        let mut completed_parts = dispatch::dispatch_concurrent(stream, concurrency_limit).await?;
        completed_parts.sort_by_key(|completed_part| completed_part.part_number);

        let output = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(&key)
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
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .send()
            .map(|_| Err(e))
    })
    .await
}

#[cfg(test)]
mod tests;
