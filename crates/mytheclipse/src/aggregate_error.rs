//! Error aggregation for parallel/fan-out work (feature `resiliency`).
//!
//! [`AggregateError`] collects multiple `E: std::error::Error` values produced
//! by concurrently executed tasks into one error, so a caller awaiting `N`
//! tasks via `join_all` can surface *every* failure at once instead of
//! stopping at the first. This is the natural failure type for
//! `futures::future::join_all(vec![...])` transactions, batch operations, and
//! fan-out resilience.

use std::fmt;

/// An error that groups one or more underlying errors.
///
/// ```
/// use mytheclipse::aggregate_error::AggregateError;
///
/// // Collect errors from N parallel results — all of them, not just the first:
/// let results = vec![
///     Ok::<_, std::io::Error>(1),
///     Err(std::io::Error::other("boom")),
///     Ok::<_, std::io::Error>(3),
///     Err(std::io::Error::other("bam")),
/// ];
/// let out = AggregateError::from_results(results);
/// let err = out.unwrap_err();
/// assert_eq!(err.len(), 2); // both errors collected
///
/// // Build one incrementally too:
/// let mut agg = AggregateError::with_context("fan-out");
/// agg.push(std::io::Error::other("first"));
/// agg.push(std::io::Error::other("second"));
/// assert_eq!(agg.len(), 2);
/// ```
#[derive(Debug)]
pub struct AggregateError {
    errors: Vec<Box<dyn std::error::Error + Send + Sync>>,
    /// Optional label describing the operation that failed.
    context: Option<String>,
}

impl AggregateError {
    /// Creates an empty aggregate (no errors yet).
    pub fn empty() -> Self {
        Self {
            errors: Vec::new(),
            context: None,
        }
    }

    /// Creates a labeled aggregate with an operation context.
    pub fn with_context(context: impl Into<String>) -> Self {
        Self {
            errors: Vec::new(),
            context: Some(context.into()),
        }
    }

    /// Adds an error to the aggregate.
    pub fn push<E: Into<Box<dyn std::error::Error + Send + Sync>>>(&mut self, error: E) {
        self.errors.push(error.into());
    }

    /// Returns `true` if the aggregate holds no errors.
    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    /// Number of collected errors.
    pub fn len(&self) -> usize {
        self.errors.len()
    }

    /// Iterator over the collected errors.
    pub fn iter(&self) -> impl Iterator<Item = &(dyn std::error::Error + Send + Sync)> {
        self.errors.iter().map(|b| b.as_ref())
    }

    /// Builds a [`Result`] from a collection of [`Result`]s, aggregating the
    /// errors from every `Err` branch.
    ///
    /// If all inputs are `Ok`, the `V` values are collected and returned.
    pub fn from_results<V, E>(results: Vec<Result<V, E>>) -> Result<Vec<V>, AggregateError>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut values = Vec::with_capacity(results.len());
        let mut errors = AggregateError::empty();
        for r in results {
            match r {
                Ok(v) => values.push(v),
                Err(e) => errors.push(Box::new(e)),
            }
        }
        if errors.is_empty() {
            Ok(values)
        } else {
            Err(errors)
        }
    }
}

impl fmt::Display for AggregateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ctx) = &self.context {
            write!(f, "{ctx}: {} error(s)", self.errors.len())?;
        } else {
            write!(f, "{} error(s)", self.errors.len())?;
        }
        if !self.errors.is_empty() {
            write!(f, " — first: {}", self.errors[0])?;
        }
        Ok(())
    }
}

impl std::error::Error for AggregateError {}

impl From<Vec<Box<dyn std::error::Error + Send + Sync>>> for AggregateError {
    fn from(errors: Vec<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self {
            errors,
            context: None,
        }
    }
}

impl Extend<Box<dyn std::error::Error + Send + Sync>> for AggregateError {
    fn extend<T: IntoIterator<Item = Box<dyn std::error::Error + Send + Sync>>>(
        &mut self,
        iter: T,
    ) {
        self.errors.extend(iter);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregates_multiple_errors() {
        let mut agg = AggregateError::with_context("batch_delete");
        agg.push(std::io::Error::other("row 1"));
        agg.push(std::io::Error::other("row 2"));
        assert_eq!(agg.len(), 2);
        assert!(!agg.is_empty());
        let s = agg.to_string();
        assert!(s.contains("batch_delete"));
        assert!(s.contains("2 error(s)"));
    }

    #[test]
    fn extracts_errors_from_results() {
        let results: Vec<Result<u32, std::io::Error>> = vec![
            Ok(1),
            Err(std::io::Error::other("a")),
            Ok(2),
            Err(std::io::Error::other("b")),
        ];
        let out = AggregateError::from_results(results);
        assert!(out.is_err());
        let err = out.unwrap_err();
        assert_eq!(err.len(), 2);
        assert_eq!(err.iter().count(), 2);
    }

    #[test]
    fn collects_values_when_all_ok() {
        let results: Vec<Result<u32, std::io::Error>> = vec![Ok(1), Ok(2), Ok(3)];
        let out = AggregateError::from_results(results).unwrap();
        assert_eq!(out, vec![1, 2, 3]);
    }
}
