#![allow(clippy::unit_arg)]

use {
    crate::{
        account::{AccountFunds, AccountState},
        transaction::Amount,
        transaction_source::load_transactions_csv_path,
    },
    anyhow::Context,
    clap::Parser,
    std::path::PathBuf,
    tap::Pipe,
};

mod account;
mod transaction;
mod transaction_source;
mod utils;

/// simple toy payments engine that reads a series of
/// transactions from a CSV, updates client accounts, handles
/// disputes and chargebacks, and then outputs the state of
/// clients accounts as a CSV
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// path to transactions file, for example:
    ///   transactions.csv
    input: PathBuf,
}

/// used due to a limitation of [csv] crate - it doesn't support `#[serde::flatten]`
/// `https://github.com/BurntSushi/rust-csv/issues/239`
#[derive(serde::Serialize, Debug)]
struct AccountRow {
    /// client ID
    pub client: transaction::Client,
    /// The total funds that are mut mut mut available for trading, staking,
    /// withdrawal, etc. This should be equal to the total - held
    /// amounts
    pub available: transaction::Amount,
    /// The total funds that are held for dispute. This should be
    /// equal to total - available amounts
    pub held: transaction::Amount,
    /// The total funds that are available or held. This should be
    /// equal to available + held
    pub total: transaction::Amount,
    /// Whether the account is locked. An account is locked if a
    /// charge back occurs
    pub locked: bool,
}

impl AccountRow {
    pub fn new(
        client: transaction::Client,
        AccountState {
            history: _,
            disputes: _,
            chargeback,
            funds: AccountFunds { available, held, total },
        }: &AccountState,
    ) -> Self {
        fn round(v: &Amount) -> Amount {
            v.round_dp(4)
        }

        Self {
            client,
            available: round(available),
            held: round(held),
            total: round(total),
            locked: chargeback.is_some(),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let Cli { input } = Cli::parse();

    load_transactions_csv_path(&input)
        .fold(crate::account::Accounts::default(), |mut accounts, next| {
            next.context("reading transaction from source")
                .and_then(|next| {
                    accounts
                        .next_transaction(next.clone())
                        .context("applying transaction")
                        .with_context(|| format!("{next:#?}: transaction failed"))
                })
                .pipe(|res| {
                    if let Err(reason) = res {
                        // if error occurs, the system should continue as normal - no mutation occurred
                        eprintln!("{reason:?}: could not apply transaction");
                    }
                    accounts
                })
        })
        .pipe(|accounts| {
            accounts
                .accounts()
                .map(|(client, funds)| AccountRow::new(*client, funds))
                .pipe(|mut entries| {
                    csv::WriterBuilder::new()
                        .from_writer(std::io::stdout())
                        .pipe(|mut writer| {
                            entries
                                .try_for_each(|entry| {
                                    writer
                                        .serialize(&entry)
                                        .with_context(|| format!("serializing entry: {entry:#?}"))
                                })
                                .and_then(|()| writer.flush().context("flushing"))
                                .context("writing entries to stdout")
                        })
                })
        })
}

#[cfg(test)]
mod integration_tests;
