use bytes::Bytes;
use futures::Stream;
use md5::digest::Output;
use md5::{Digest, Md5};
use std::cmp;
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
        body,
        inner: Some(Inner::new(part_size)),
    }
}

#[pin_project::pin_project]
struct Split<B> {
    #[pin]
    body: B,
    inner: Option<Inner>,
}

impl<B, E> Stream for Split<B>
where
    B: Stream<Item = Result<Bytes, E>>,
{
    type Item = Result<Part, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(inner) = this.inner.as_mut() {
            loop {
                if let Some(part) = inner.pop() {
                    break Poll::Ready(Some(Ok(part)));
                }
                match this.body.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(chunk))) => inner.push(chunk),
                    Poll::Ready(Some(Err(e))) => break Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        break Poll::Ready(Some(Ok(this.inner.take().unwrap().finish())))
                    }
                    Poll::Pending => break Poll::Pending,
                }
            }
        } else {
            Poll::Ready(None)
        }
    }
}

struct Inner {
    remaining: Bytes,
    part_size: RangeInclusive<usize>,
    part_body: Vec<Bytes>,
    part_content_length: usize,
    part_content_md5: Md5,
    part_number: usize,
}

impl Inner {
    fn new(part_size: RangeInclusive<usize>) -> Self {
        Self {
            remaining: Bytes::new(),
            part_size,
            part_body: Vec::new(),
            part_content_length: 0,
            part_content_md5: Md5::new(),
            part_number: 0,
        }
    }

    fn push_part(&mut self, chunk: Bytes) {
        self.part_content_length += chunk.len();
        self.part_content_md5.update(&chunk);
        self.part_body.push(chunk);
    }

    fn push(&mut self, chunk: Bytes) {
        let chunk = mem::replace(&mut self.remaining, chunk);
        self.push_part(chunk);
    }

    fn pop(&mut self) -> Option<Part> {
        if self.part_content_length + self.remaining.len() >= *self.part_size.start() {
            let chunk = self.remaining.split_to(cmp::min(
                self.remaining.len(),
                *self.part_size.end() - self.part_content_length,
            ));
            self.push_part(chunk);

            self.part_number += 1;
            Some(Part {
                body: mem::replace(&mut self.part_body, Vec::new()),
                content_length: mem::replace(&mut self.part_content_length, 0),
                content_md5: self.part_content_md5.finalize_reset(),
                part_number: self.part_number,
            })
        } else {
            None
        }
    }

    fn finish(mut self) -> Part {
        let chunk = self.remaining.split_off(0);
        self.push_part(chunk);
        Part {
            body: self.part_body,
            content_length: self.part_content_length,
            content_md5: self.part_content_md5.finalize(),
            part_number: self.part_number + 1,
        }
    }
}
