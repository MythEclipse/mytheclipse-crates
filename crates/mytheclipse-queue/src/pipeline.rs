//! Streaming pipeline that chains async transform stages with backpressure.
//!
//! Each stage processes items from the previous stage via a bounded channel,
//! providing natural backpressure between stages.

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A single transform in the pipeline.
#[async_trait]
pub trait Stage<I: Send + 'static, O: Send + 'static>: Send + Sync {
    /// Transforms one input into one output, or fails the stage.
    async fn process(&self, input: I) -> Result<O, StageError>;
}

/// Errors from pipeline stages.
#[derive(Debug)]
pub enum StageError {
    /// The stage itself failed, with a message from the implementation.
    Processing(String),
    /// The pipeline shut down before this item could be processed.
    ChannelClosed,
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StageError::Processing(s) => write!(f, "stage error: {s}"),
            StageError::ChannelClosed => write!(f, "channel closed"),
        }
    }
}

impl std::error::Error for StageError {}

/// Runs a single stage as a background task, consuming from input and
/// forwarding results to output.
pub struct StageRunner<S, I, O>
where
    S: Stage<I, O>,
    I: Send + 'static,
    O: Send + 'static,
{
    stage: Arc<S>,
    _phantom: PhantomData<(I, O)>,
}

impl<S, I, O> StageRunner<S, I, O>
where
    S: Stage<I, O> + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    /// Creates a runner for a single stage with the given channel capacity.
    pub fn new(stage: S) -> Self {
        Self {
            stage: Arc::new(stage),
            _phantom: PhantomData,
        }
    }

    /// Consumes items from `input`, applies the stage, sends to `output`.
    /// Completes when the input stream ends.
    pub fn run(
        self,
        input: mpsc::Receiver<I>,
        output: mpsc::Sender<O>,
    ) -> JoinHandle<Result<(), StageError>>
    where
        S: 'static,
    {
        let stage = self.stage;
        tokio::spawn(async move {
            let mut input = input;
            loop {
                match input.recv().await {
                    Some(item) => match stage.process(item).await {
                        Ok(out) => {
                            if output.send(out).await.is_err() {
                                return Err(StageError::ChannelClosed);
                            }
                        }
                        Err(e) => return Err(e),
                    },
                    None => return Ok(()),
                }
            }
        })
    }
}

impl<S, I, O> Default for StageRunner<S, I, O>
where
    S: Stage<I, O> + Default + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    fn default() -> Self {
        Self::new(S::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DoubleStage;

    #[async_trait]
    impl Stage<u32, u32> for DoubleStage {
        async fn process(&self, input: u32) -> Result<u32, StageError> {
            Ok(input * 2)
        }
    }

    #[tokio::test]
    async fn stage_runner_doubles_values() {
        let (tx, rx_in) = mpsc::channel::<u32>(16);
        let (tx_out, mut rx_out) = mpsc::channel::<u32>(16);

        let runner = StageRunner::new(DoubleStage);
        let handle = runner.run(rx_in, tx_out);

        tx.send(5).await.unwrap();
        tx.send(7).await.unwrap();
        drop(tx);

        assert_eq!(rx_out.recv().await, Some(10));
        assert_eq!(rx_out.recv().await, Some(14));
        assert_eq!(rx_out.recv().await, None);

        assert!(handle.await.unwrap().is_ok());
    }
}
