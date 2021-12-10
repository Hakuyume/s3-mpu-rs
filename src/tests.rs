use super::{multipart_upload, MultipartUploadRequest, PART_SIZE};
use aws_config::default_provider::credentials;
use aws_sdk_s3::{Client, Config, Endpoint, Region};
use bytes::Bytes;
use rand::seq::SliceRandom;
use rand::Rng;
use std::convert::TryInto;
use std::env;
use std::error::Error;
use std::io;
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

    let output = multipart_upload::<_, _, _, _, Box<dyn Error>>(
        &client,
        MultipartUploadRequest {
            body: futures::stream::iter(into_chunks(body.clone(), &mut rng).map(Ok)),
            bucket: bucket.clone(),
            key: key.clone(),
        },
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
    let mut rng = rand::thread_rng();

    let (client, bucket, key) = context().await;
    let body = [
        Ok((0..*PART_SIZE.start() * 3 / 4)
            .map(|_| rng.gen())
            .collect::<Bytes>()),
        Ok((0..*PART_SIZE.start() * 5 / 4)
            .map(|_| rng.gen())
            .collect::<Bytes>()),
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "error").into()),
    ];

    let e = multipart_upload::<_, _, _, _, Box<dyn Error>>(
        &client,
        MultipartUploadRequest {
            body: futures::stream::iter(body),
            bucket: bucket.clone(),
            key: key.clone(),
        },
        PART_SIZE,
        None,
    )
    .await
    .unwrap_err();

    assert_eq!(
        e.downcast::<io::Error>().unwrap().kind(),
        io::ErrorKind::BrokenPipe
    );

    let output = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();

    if let Some(uploads) = &output.uploads {
        assert!(!uploads
            .iter()
            .any(|upload| upload.key.as_ref() == Some(&key)));
    }
}
