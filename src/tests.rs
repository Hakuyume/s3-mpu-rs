use super::{multipart_upload, MultipartUploadRequest, PART_SIZE};
use bytes::Bytes;
use rand::seq::SliceRandom;
use rand::Rng;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, ListMultipartUploadsRequest, S3Client, S3};
use std::env;
use std::error::Error;
use std::io;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

fn context() -> (S3Client, String, String) {
    let client = S3Client::new(Region::Custom {
        name: "custom".to_owned(),
        endpoint: env::var("ENDPOINT").unwrap(),
    });
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

    let (client, bucket, key) = context();
    let body = (0..size).map(|_| rng.gen()).collect::<Bytes>();

    let output = multipart_upload::<_, _, Box<dyn Error>>(
        &client,
        MultipartUploadRequest {
            body: futures::stream::iter(into_chunks(body.clone(), &mut rng).map(Ok)),
            bucket: bucket.clone(),
            key: key.clone(),
        },
        PART_SIZE,
        concurrency_limit,
    )
    .await
    .unwrap();

    assert_eq!(output.bucket.as_ref().unwrap(), &bucket);
    assert_eq!(output.key.as_ref().unwrap(), &key);

    let output = client
        .get_object(GetObjectRequest {
            bucket,
            key,
            ..GetObjectRequest::default()
        })
        .await
        .unwrap();

    let mut buf = Vec::new();
    output
        .body
        .unwrap()
        .into_async_read()
        .read_to_end(&mut buf)
        .await
        .unwrap();
    assert_eq!(buf, body);
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

    let (client, bucket, key) = context();
    let body = vec![
        Ok((0..*PART_SIZE.start() * 3 / 4)
            .map(|_| rng.gen())
            .collect::<Bytes>()),
        Ok((0..*PART_SIZE.start() * 5 / 4)
            .map(|_| rng.gen())
            .collect::<Bytes>()),
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "error").into()),
    ];

    let e = multipart_upload::<_, _, Box<dyn Error>>(
        &client,
        MultipartUploadRequest {
            body: futures::stream::iter(body),
            bucket: bucket.clone(),
            key: key.clone(),
        },
        PART_SIZE,
        Some(8),
    )
    .await
    .unwrap_err();

    assert_eq!(
        e.downcast::<io::Error>().unwrap().kind(),
        io::ErrorKind::BrokenPipe
    );

    let output = client
        .list_multipart_uploads(ListMultipartUploadsRequest {
            bucket,
            ..ListMultipartUploadsRequest::default()
        })
        .await
        .unwrap();

    if let Some(uploads) = &output.uploads {
        assert!(!uploads
            .iter()
            .any(|upload| upload.key.as_ref() == Some(&key)));
    }
}
