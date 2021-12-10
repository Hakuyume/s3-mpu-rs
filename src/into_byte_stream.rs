use aws_sdk_s3::ByteStream;
use aws_smithy_http::body::SdkBody;
use bytes::Bytes;
use http::header::HeaderMap;
use http_body::combinators::BoxBody;
use http_body::Body;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub fn into_byte_stream(body: Vec<Bytes>) -> ByteStream {
    struct B(Arc<[Bytes]>, usize);

    impl Body for B {
        type Data = Bytes;
        type Error = Box<dyn 'static + Error + Send + Sync>;

        fn poll_data(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
            let this = self.get_mut();
            let bytes = this.0.get(this.1);
            this.1 += 1;
            Poll::Ready(bytes.cloned().map(Ok))
        }

        fn poll_trailers(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
            Poll::Ready(Ok(None))
        }
    }

    let body = Arc::<[_]>::from(Box::from(body));
    ByteStream::new(SdkBody::retryable(move || {
        SdkBody::from_dyn(BoxBody::new(B(body.clone(), 0)))
    }))
}

#[cfg(test)]
mod tests {
    use super::into_byte_stream;
    use aws_sdk_s3::ByteStream;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_into_byte_stream() {
        let inner = into_byte_stream(vec![
            Bytes::from_static(&[0, 1, 2]),
            Bytes::from_static(&[3, 4]),
        ])
        .into_inner();

        let body = ByteStream::new(inner.try_clone().unwrap());
        assert_eq!(
            body.collect().await.unwrap().into_bytes(),
            Bytes::from_static(&[0, 1, 2, 3, 4])
        );

        let body = ByteStream::new(inner);
        assert_eq!(
            body.collect().await.unwrap().into_bytes(),
            Bytes::from_static(&[0, 1, 2, 3, 4])
        );
    }
}
