use {
    crate::{transaction::Transaction, utils::IteratorTryFlatMapExt},
    anyhow::{Context, Result},
    futures::{Stream, StreamExt},
    std::{io::Read, iter::once, path::PathBuf},
    tap::Pipe,
};

/// Determins where to load [Transaction]s from
pub enum TransactionSource {
    /// Load transactions from a CSV file, useful for testing
    Csv(PathBuf),
    // more coming soon...
}

pub fn load_transactions_csv_reader(source: impl Read) -> impl Iterator<Item = Result<Transaction>> {
    csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(source)
        .pipe(once)
        .flat_map(|reader| {
            reader
                .into_deserialize::<Transaction>()
                .map(|transaction| transaction.context("deserializing transaction"))
        })
}

pub fn load_transactions_csv_str<'a>(source: &'a str) -> impl Iterator<Item = Result<Transaction>> + 'a {
    std::io::Cursor::new(source).pipe(load_transactions_csv_reader)
}

pub fn load_transactions_csv_path(source: PathBuf) -> impl Iterator<Item = Result<Transaction>> {
    std::fs::File::open(&source)
        .with_context(|| format!("opening file at {source:?}"))
        .pipe(once)
        .try_flat_map(load_transactions_csv_reader)
}

pub fn stream_transactions(source: impl Into<TransactionSource>) -> impl Stream<Item = Result<Transaction>> {
    match source.into() {
        TransactionSource::Csv(csv) => load_transactions_csv_path(csv)
            .pipe(futures::stream::iter)
            // important, because when there's more sources each will be a distinct type
            .boxed_local(),
    }
}
