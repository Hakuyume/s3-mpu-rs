use super::{multipart_upload, PART_SIZE};
use bytes::Bytes;
use rand::seq::SliceRandom;
use rand::Rng;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, S3Client, S3};
use std::env;
use std::error::Error;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

async fn check(size: usize) {
    let client = S3Client::new(Region::Custom {
        name: "custom".to_owned(),
        endpoint: env::var("ENDPOINT").unwrap(),
    });
    let mut rng = rand::thread_rng();

    let bucket = env::var("BUCKET").unwrap();
    let key = format!("test-{}", Uuid::new_v4());
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
    check(*PART_SIZE.start() * 5 / 2).await;
}
