use bytes::Bytes;
use futures::{Stream, StreamExt};
use rusoto_core::{ByteStream, RusotoError};
use rusoto_s3::{
    CompleteMultipartUploadError, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CreateMultipartUploadError, CreateMultipartUploadRequest, UploadPartError,
    UploadPartRequest, S3,
};
use std::cmp;
use std::mem;
use std::ops::RangeInclusive;

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub const PART_SIZE: RangeInclusive<usize> = 5 << 20..=5 << 30;

pub async fn multipart_upload<C, B, E>(
    client: &C,
    body: B,
    bucket: &str,
    key: &str,
    part_size: &RangeInclusive<usize>,
) -> Result<(), E>
where
    C: S3,
    B: Stream<Item = Result<Bytes, E>>,
    E: From<RusotoError<CreateMultipartUploadError>>
        + From<RusotoError<UploadPartError>>
        + From<RusotoError<CompleteMultipartUploadError>>,
{
    futures::pin_mut!(body);

    let mut multipart_upload = MultipartUpload::create(client, bucket, key).await?;

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

    multipart_upload.complete().await?;
    Ok(())
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

    async fn complete(self) -> Result<(), RusotoError<CompleteMultipartUploadError>> {
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
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{multipart_upload, PART_SIZE};
    use bytes::Bytes;
    use rand::seq::SliceRandom;
    use rand::Rng;
    use rusoto_core::Region;
    use rusoto_s3::{GetObjectRequest, S3Client, S3};
    use std::env;
    use std::error::Error;
    use tokio::io::AsyncReadExt;

    async fn check(size: usize) {
        let client = S3Client::new(Region::Custom {
            name: "custom".to_owned(),
            endpoint: env::var("ENDPOINT").unwrap(),
        });
        let mut rng = rand::thread_rng();

        let bucket = env::var("BUCKET").unwrap();
        let key = format!("test-{}", size);
        let data = (0..size).map(|_| rng.gen()).collect::<Bytes>();
        let chunks = {
            let mut sizes = Vec::new();
            let mut total = 0;
            while total < data.len() {
                let size = rng.gen_range(0..=data.len() - total);
                sizes.push(size);
                total += size;
            }
            sizes.shuffle(&mut rng);
            let mut data = data.clone();
            futures::stream::iter(sizes.into_iter().map(move |size| Ok(data.split_to(size))))
        };

        multipart_upload::<_, _, Box<dyn Error>>(&client, chunks, &bucket, &key, &PART_SIZE)
            .await
            .unwrap();

        let mut downloaded = Vec::new();
        client
            .get_object(GetObjectRequest {
                bucket,
                key,
                ..GetObjectRequest::default()
            })
            .await
            .unwrap()
            .body
            .unwrap()
            .into_async_read()
            .read_to_end(&mut downloaded)
            .await
            .unwrap();
        assert_eq!(&downloaded, &data);
    }

    #[tokio::test]
    async fn test_small() {
        check(*PART_SIZE.start() / 2).await;
    }

    #[tokio::test]
    async fn test_exact() {
        check(*PART_SIZE.start() * 2).await;
    }

    #[tokio::test]
    async fn test_large() {
        check(*PART_SIZE.start() / 2 * 5 / 2).await;
    }
}
