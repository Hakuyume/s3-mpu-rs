use bytes::Bytes;
use futures::stream::{Fuse, Stream, StreamExt};
use md5::digest::Output;
use md5::{Digest, Md5};
use std::cmp;
use std::collections::VecDeque;
use std::mem;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct Part {
    pub body: Vec<Bytes>,
    pub content_length: usize,
    pub content_md5: Output<Md5>,
    pub part_number: usize,
}

pub fn split<B, E>(body: B, part_size: RangeInclusive<usize>) -> impl Stream<Item = Result<Part, E>>
where
    B: Stream<Item = Result<Bytes, E>>,
{
    Split {
        body: body.fuse(),
        part_size,
        inner: Inner::default(),
        parts: VecDeque::new(),
    }
}

#[pin_project::pin_project]
struct Split<B> {
    #[pin]
    body: Fuse<B>,
    part_size: RangeInclusive<usize>,
    inner: Inner,
    parts: VecDeque<Part>,
}

impl<B, E> Stream for Split<B>
where
    B: Stream<Item = Result<Bytes, E>>,
{
    type Item = Result<Part, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(part) = this.parts.pop_front() {
            Poll::Ready(Some(Ok(part)))
        } else if this.body.is_done() {
            Poll::Ready(None)
        } else {
            loop {
                match this.body.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(mut chunk))) => {
                        while this.inner.part_content_length + chunk.len()
                            >= *this.part_size.start()
                        {
                            let chunk = chunk.split_to(cmp::min(
                                chunk.len(),
                                *this.part_size.end() - this.inner.part_content_length,
                            ));
                            this.inner.push(chunk);
                            this.parts.push_back(this.inner.pop());
                        }
                        this.inner.push(chunk);

                        if let Some(part) = this.parts.pop_front() {
                            break Poll::Ready(Some(Ok(part)));
                        }
                    }
                    Poll::Ready(Some(Err(e))) => break Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => break Poll::Ready(Some(Ok(this.inner.pop()))),
                    Poll::Pending => break Poll::Pending,
                }
            }
        }
    }
}

#[derive(Default)]
struct Inner {
    part_body: Vec<Bytes>,
    part_content_length: usize,
    part_content_md5: Md5,
    part_number: usize,
}

impl Inner {
    fn push(&mut self, chunk: Bytes) {
        self.part_content_length += chunk.len();
        self.part_content_md5.update(&chunk);
        self.part_body.push(chunk);
    }

    fn pop(&mut self) -> Part {
        self.part_number += 1;
        Part {
            body: mem::replace(&mut self.part_body, Vec::new()),
            content_length: mem::replace(&mut self.part_content_length, 0),
            content_md5: self.part_content_md5.finalize_reset(),
            part_number: self.part_number,
        }
    }
}
