use futures::{FutureExt, Stream, StreamExt};
use std::future::Future;
use std::num::NonZeroUsize;
use std::task::Poll;

pub async fn dispatch_concurrent<S, F, T, E>(
    stream: S,
    limit: Option<NonZeroUsize>,
) -> Result<Vec<T>, E>
where
    S: Stream<Item = Result<F, E>>,
    F: Future<Output = Result<T, E>> + Unpin,
{
    futures::pin_mut!(stream);

    let mut stream = stream.fuse();
    let mut futures = Vec::new();
    let mut outputs = Vec::new();

    futures::future::poll_fn(|cx| loop {
        while limit.map_or(true, |limit| limit.get() > futures.len()) {
            if let Poll::Ready(Some(future)) = stream.poll_next_unpin(cx)? {
                futures.push(future);
            } else {
                break;
            }
        }

        let a = futures.len();

        let mut i = 0;
        while i < futures.len() {
            if let Poll::Ready(output) = futures[i].poll_unpin(cx)? {
                futures.swap_remove(i);
                outputs.push(output);
            } else {
                i += 1;
            }
        }

        let b = futures.len();

        if stream.is_done() && futures.is_empty() {
            break Poll::Ready(Ok(()));
        } else if a == b {
            break Poll::Pending;
        }
    })
    .await?;

    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::dispatch_concurrent;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::future::Future;
    use std::rc::Rc;
    use std::task::{Context, Poll};

    #[test]
    fn test_dispatch_concurrent() {
        let queue = RefCell::new(VecDeque::<Option<Rc<Cell<Option<usize>>>>>::new());
        let running = Rc::new(Cell::new(0));

        let future = dispatch_concurrent(
            futures::stream::poll_fn(|_| match queue.borrow_mut().pop_front() {
                Some(Some(cell)) => {
                    let running = running.clone();
                    Poll::Ready(Some(Ok(Box::pin(async move {
                        running.set(running.get() + 1);
                        let output = futures::future::poll_fn(|_| {
                            if let Some(output) = cell.take() {
                                Poll::Ready(output)
                            } else {
                                Poll::Pending
                            }
                        })
                        .await;
                        running.set(running.get() - 1);
                        Ok::<_, ()>(output)
                    }))))
                }
                Some(None) => Poll::Ready(None),
                None => Poll::Pending,
            }),
            Some(2.try_into().unwrap()),
        );

        futures::pin_mut!(future);
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());

        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 0);
        assert_eq!(running.get(), 0);

        let cell0 = Rc::new(Cell::new(None));
        queue.borrow_mut().push_back(Some(cell0.clone()));
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 0);
        assert_eq!(running.get(), 1);

        let cell1 = Rc::new(Cell::new(None));
        queue.borrow_mut().push_back(Some(cell1.clone()));
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 0);
        assert_eq!(running.get(), 2);

        let cell2 = Rc::new(Cell::new(None));
        queue.borrow_mut().push_back(Some(cell2.clone()));
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 1);
        assert_eq!(running.get(), 2);

        let cell3 = Rc::new(Cell::new(None));
        queue.borrow_mut().push_back(Some(cell3.clone()));
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 2);
        assert_eq!(running.get(), 2);

        cell1.set(Some(1));
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 1);
        assert_eq!(running.get(), 2);

        cell3.set(Some(3));
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 1);
        assert_eq!(running.get(), 2);

        cell0.set(Some(0));
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 0);
        assert_eq!(running.get(), 1);

        let cell4 = Rc::new(Cell::new(None));
        queue.borrow_mut().push_back(Some(cell4.clone()));
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 0);
        assert_eq!(running.get(), 2);

        cell2.set(Some(2));
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 0);
        assert_eq!(running.get(), 1);

        queue.borrow_mut().push_back(None);
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(queue.borrow().len(), 0);
        assert_eq!(running.get(), 1);

        cell4.set(Some(4));
        assert_eq!(
            future.as_mut().poll(&mut cx),
            Poll::Ready(Ok(vec![1, 0, 3, 2, 4]))
        );
        assert_eq!(queue.borrow().len(), 0);
        assert_eq!(running.get(), 0);
    }
}
