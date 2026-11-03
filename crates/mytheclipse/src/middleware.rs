//! Composable middleware pipeline (feature `observability` + `resiliency`).
//!
//! [`MiddlewarePipeline`] is an ordered stack of boxed async functions. Each
//! stage receives the state, may transform or reject it, and returns control
//! to the next stage. The final state is delivered to a caller-supplied
//! service closure.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Error returned by [`MiddlewarePipeline::apply`].
#[derive(Debug)]
pub struct PipelineError {
    /// What the failing layer reported.
    pub msg: String,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pipeline error: {}", self.msg)
    }
}

impl std::error::Error for PipelineError {}

/// A single async middleware stage.
pub type BoxMiddleware<S> =
    Arc<dyn Fn(S) -> Pin<Box<dyn Future<Output = Result<S, PipelineError>> + Send>> + Send + Sync>;

/// Helper to box any `async fn` middleware.
pub fn mw<S, F, Fut>(f: F) -> BoxMiddleware<S>
where
    S: Send + 'static,
    F: Fn(S) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<S, PipelineError>> + Send + 'static,
{
    Arc::new(move |state| Box::pin(f(state)))
}

/// A stack of ordered middleware stages.
#[derive(Clone, Default)]
pub struct MiddlewarePipeline<S> {
    layers: Arc<Mutex<Vec<BoxMiddleware<S>>>>,
}

impl<S: Send + 'static> MiddlewarePipeline<S> {
    /// Creates a pipeline with no layers.
    pub fn new() -> Self {
        Self {
            layers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Appends a middleware stage.
    pub fn add(&self, m: BoxMiddleware<S>) {
        self.layers.lock().unwrap().push(m);
    }

    /// Applies every layer in order, short-circuiting on the first error.
    pub async fn apply(&self, state: S) -> Result<S, PipelineError> {
        // Clone the Arc'd layers out so the MutexGuard is dropped before any
        // await point (holding it across `layer(current).await` is unsound —
        // a re-entrant layer could deadlock).
        let layers: Vec<BoxMiddleware<S>> = self.layers.lock().unwrap().clone();
        let mut current = state;
        for layer in layers.iter() {
            current = layer(current).await?;
        }
        Ok(current)
    }

    /// Applies every layer, then runs `svc` with the final state.
    pub async fn run<F, Fut, R, E>(&self, state: S, svc: F) -> Result<R, E>
    where
        F: Fn(S) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<R, E>> + Send,
        R: Send + 'static,
        E: From<PipelineError> + Send,
    {
        match self.apply(state).await {
            Ok(s) => svc(s).await,
            Err(e) => Err(E::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn applies_two_layers_in_order() {
        let p: MiddlewarePipeline<u32> = MiddlewarePipeline::new();
        let inc = mw(|s: u32| async move { Ok::<_, PipelineError>(s + 1) });
        let double = mw(|s: u32| async move { Ok::<_, PipelineError>(s * 2) });
        p.add(inc);
        p.add(double);
        let out = p.apply(1).await.unwrap();
        assert_eq!(out, 4); // (1+1)*2
    }

    #[tokio::test]
    async fn short_circuits_on_error() {
        let p: MiddlewarePipeline<String> = MiddlewarePipeline::new();
        let reject = mw(|_s: String| async {
            Err::<_, PipelineError>(PipelineError {
                msg: "rejected".into(),
            })
        });
        p.add(reject);
        assert!(p.apply("x".to_string()).await.is_err());
    }
}
