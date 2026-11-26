use {
    crate::{transaction::Transaction, utils::IteratorTryFlatMapExt},
    anyhow::{Context, Result},
    std::{io::Read, iter::once, path::PathBuf},
    tap::Pipe,
};

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

pub fn load_transactions_csv_path(source: &PathBuf) -> impl Iterator<Item = Result<Transaction>> + '_ {
    std::fs::File::open(source)
        .context("opening file")
        .pipe(once)
        .try_flat_map(load_transactions_csv_reader)
        .map(move |e| e.with_context(|| format!("loading transaction at {source:?}")))
}
