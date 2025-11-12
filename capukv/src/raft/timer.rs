use std::time::Duration;

use tokio::{sync::oneshot, time::Instant};

#[derive(Debug)]
pub(crate) struct Timer {
    tx: Option<oneshot::Sender<()>>,
}

impl Timer {
    #[must_use]
    pub(crate) fn new<F, Fut>(duration: Duration, on_expire: F) -> Self
    where
        F: FnOnce() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + Sync,
    {
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let deadline = Instant::now().checked_add(duration).unwrap();

            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    on_expire().await;
                    return;
                }
                _ = rx => {
                    return;
                }
            }
        });

        Self { tx: Some(tx) }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}
