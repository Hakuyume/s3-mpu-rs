use rusoto_core::RusotoError;
use rusoto_s3::{
    CompleteMultipartUploadError, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CreateMultipartUploadError, CreateMultipartUploadRequest, UploadPartError,
    UploadPartRequest, S3,
};
use tokio::io::{self, AsyncRead, AsyncReadExt};

const MIN_PART_SIZE: usize = 5 << 20;

pub async fn multipart_upload<C, B, E>(
    client: &C,
    mut body: B,
    bucket: &str,
    key: &str,
) -> Result<(), E>
where
    C: S3,
    B: Unpin + AsyncRead,
    E: From<io::Error>
        + From<RusotoError<CreateMultipartUploadError>>
        + From<RusotoError<UploadPartError>>
        + From<RusotoError<CompleteMultipartUploadError>>,
{
    let mut multipart_upload = MultipartUpload::create(client, bucket, key).await?;

    let mut buf = Vec::with_capacity(MIN_PART_SIZE * 2);
    while body.read_buf(&mut buf).await? > 0 {
        if buf.len() >= MIN_PART_SIZE {
            multipart_upload.upload(buf.split_off(0)).await?;
        }
    }
    multipart_upload.upload(buf).await?;

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

    async fn upload(&mut self, body: Vec<u8>) -> Result<(), RusotoError<UploadPartError>> {
        let e_tag = self
            .client
            .upload_part(UploadPartRequest {
                body: Some(body.into()),
                bucket: self.bucket.to_owned(),
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
    use super::{multipart_upload, MIN_PART_SIZE};
    use rand::Rng;
    use rusoto_core::request::HttpClient;
    use rusoto_core::Region;
    use rusoto_credential::StaticProvider;
    use rusoto_s3::{GetObjectRequest, S3Client, S3};
    use std::env;
    use std::error::Error;
    use tokio::io::AsyncReadExt;

    async fn check(size: usize) {
        let client = S3Client::new_with(
            HttpClient::new().unwrap(),
            StaticProvider::new(
                env::var("AWS_ACCESS_KEY_ID").unwrap(),
                env::var("AWS_SECRET_ACCESS_KEY").unwrap(),
                None,
                None,
            ),
            Region::Custom {
                name: "custom".to_owned(),
                endpoint: env::var("ENDPOINT").unwrap(),
            },
        );
        let mut rng = rand::thread_rng();

        let bucket = env::var("BUCKET").unwrap();
        let key = format!("test-{}", size);
        let data = (0..size).map(|_| rng.gen()).collect::<Vec<u8>>();

        multipart_upload::<_, _, Box<dyn Error>>(&client, &*data, &bucket, &key)
            .await
            .unwrap();

        let mut downloaded = Vec::new();
        client
            .get_object(GetObjectRequest {
                bucket: bucket,
                key: key,
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
        check(MIN_PART_SIZE / 2).await;
    }

    #[tokio::test]
    async fn test_exact() {
        check(MIN_PART_SIZE * 2).await;
    }

    #[tokio::test]
    async fn test_large() {
        check(MIN_PART_SIZE * 5 / 2).await;
    }
}
