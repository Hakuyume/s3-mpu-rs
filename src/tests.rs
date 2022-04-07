use super::{MultipartUpload, PART_SIZE};
use crate::into_byte_stream;
use aws_config::default_provider::credentials;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Config, Endpoint, Region};
use aws_smithy_http::body::{self, SdkBody};
use bytes::Bytes;
use http::header::HeaderMap;
use http_body::combinators::BoxBody;
use http_body::Body;
use rand::seq::SliceRandom;
use rand::Rng;
use std::array;
use std::env;
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::Uuid;

async fn context() -> (Client, String, String) {
    let client = Client::from_conf(
        Config::builder()
            .credentials_provider(credentials::default_provider().await)
            .endpoint_resolver(Endpoint::immutable(
                env::var("ENDPOINT").unwrap().parse().unwrap(),
            ))
            .region(Region::from_static("custom"))
            .build(),
    );
    let bucket = env::var("BUCKET").unwrap();
    let key = Uuid::new_v4().to_string();
    (client, bucket, key)
}

fn into_chunks<R>(mut data: Bytes, rng: &mut R) -> impl Iterator<Item = Bytes>
where
    R: Rng,
{
    let mut sizes = Vec::new();
    let mut total = 0;
    while total < data.len() {
        let size = rng.gen_range(0..=data.len() - total);
        sizes.push(size);
        total += size;
    }
    sizes.shuffle(rng);
    sizes.into_iter().map(move |size| data.split_to(size))
}

async fn check(size: usize, concurrency_limit: Option<usize>) {
    let mut rng = rand::thread_rng();

    let (client, bucket, key) = context().await;
    let body = (0..size).map(|_| rng.gen()).collect::<Bytes>();

    let output = MultipartUpload::new(&client)
        .body(into_byte_stream::into_byte_stream(
            into_chunks(body.clone(), &mut rng).collect(),
        ))
        .bucket(&bucket)
        .key(&key)
        .send::<anyhow::Error>(
            PART_SIZE,
            concurrency_limit.map(|limit| limit.try_into().unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(output.bucket.as_ref().unwrap(), &bucket);
    assert_eq!(output.key.as_ref().unwrap(), &key);

    let output = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .unwrap();
    assert_eq!(output.body.collect().await.unwrap().into_bytes(), body);
}

#[test]
fn test_into_chunks() {
    let mut rng = rand::thread_rng();
    let data = (0..65536).map(|_| rng.gen()).collect::<Bytes>();
    assert_eq!(
        into_chunks(data.clone(), &mut rng)
            .flatten()
            .collect::<Bytes>(),
        data
    );
}

#[tokio::test]
async fn test_empty() {
    check(0, None).await;
}

#[tokio::test]
async fn test_small() {
    check(*PART_SIZE.start() / 2, None).await;
}

#[tokio::test]
async fn test_exact() {
    check(*PART_SIZE.start() * 2, None).await;
}

#[tokio::test]
async fn test_large() {
    check(*PART_SIZE.start() * 5 / 2, None).await;
}

#[tokio::test]
async fn test_sequential() {
    check(*PART_SIZE.start() * 5, Some(1)).await;
}

#[tokio::test]
async fn test_concurrent() {
    check(*PART_SIZE.start() * 5, Some(2)).await;
}

#[tokio::test]
async fn test_concurrent_unlimited() {
    check(*PART_SIZE.start() * 5, None).await;
}

#[tokio::test]
async fn test_abort() {
    struct B<const N: usize>(array::IntoIter<Result<Bytes, body::Error>, N>);

    impl<const N: usize> Body for B<N> {
        type Data = Bytes;
        type Error = body::Error;

        fn poll_data(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
            Poll::Ready(self.get_mut().0.next())
        }

        fn poll_trailers(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
            Poll::Ready(Ok(None))
        }
    }

    let mut rng = rand::thread_rng();

    let (client, bucket, key) = context().await;
    let body = [
        Ok((0..*PART_SIZE.start() * 3 / 4)
            .map(|_| rng.gen())
            .collect::<Bytes>()),
        Ok((0..*PART_SIZE.start() * 5 / 4)
            .map(|_| rng.gen())
            .collect::<Bytes>()),
        Err("error".into()),
    ];

    let (_, abort) = MultipartUpload::new(&client)
        .body(ByteStream::new(SdkBody::from_dyn(BoxBody::new(B(
            body.into_iter()
        )))))
        .bucket(&bucket)
        .key(&key)
        .send::<anyhow::Error>(PART_SIZE, None)
        .await
        .unwrap_err();
    abort.unwrap().send().await.unwrap();

    let output = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();

    assert!(output
        .uploads
        .unwrap_or_default()
        .iter()
        .find(|upload| upload.key.as_ref() == Some(&key))
        .is_none());
}
