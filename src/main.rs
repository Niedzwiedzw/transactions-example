// it looks prettier IMO
#![allow(clippy::unit_arg)]

use {
    crate::{transaction::Amount, transaction_source::stream_transactions},
    anyhow::Context,
    clap::Parser,
    futures::StreamExt,
    std::{future::ready, ops::Not, path::PathBuf},
    tap::{Pipe, Tap},
};

pub mod account;
pub mod transaction;
pub mod transaction_source;
pub mod utils;

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

fn main() -> anyhow::Result<()> {
    let Cli { input } = Cli::parse();

    let mut accounts = crate::account::Accounts::default();

    futures::executor::block_on(
        stream_transactions(transaction_source::TransactionSource::Csv(input)).fold(&mut accounts, |acc, next| {
            next.context("reading transaction from source")
                .and_then(|next| {
                    acc.next_transaction(next.clone())
                        .context("applying transaction")
                        .with_context(|| format!("transaction failed:\n{next:#?}"))
                })
                .pipe(|res| {
                    if let Err(reason) = res {
                        // if error occurs, the system should continue as normal - no mutation occurred
                        eprintln!("[ERR] could not apply transaction:\n{reason:?}");
                    }
                    acc
                })
                .pipe(ready)
        }),
    )
    .pipe(|accounts| {
        /// used due to a limitation of [csv] crate - it doesn't support `#[serde::flatten]`
        /// `https://github.com/BurntSushi/rust-csv/issues/239`
        #[derive(serde::Serialize, Debug)]
        struct AccountRow {
            /// client ID
            pub client: transaction::Client,
            /// The total funds that are available for trading, staking,
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

        accounts
            .accounts()
            .map(|(client, funds)| (*client, funds))
            .map(
                |(
                    client,
                    account::AccountState {
                        history: _,
                        disputes,
                        funds: account::AccountFunds { available, held, total },
                    },
                )| {
                    AccountRow {
                        client,
                        available: *available,
                        held: *held,
                        total: *total,
                        locked: disputes.is_empty().not(),
                    }
                },
            )
            .pipe(|entries| {
                csv::WriterBuilder::new()
                    .from_writer(std::io::stdout())
                    .pipe(|mut writer| {
                        entries
                            .map(|entry| {
                                entry.tap_mut(|entry| {
                                    let round_4 = |v: &mut Amount| {
                                        *v = v.round_dp(4);
                                    };
                                    round_4(&mut entry.available);
                                    round_4(&mut entry.held);
                                    round_4(&mut entry.total);
                                })
                            })
                            .try_for_each(|entry| {
                                writer
                                    .serialize(&entry)
                                    .with_context(|| format!("serializing entry: {entry:#?}"))
                            })
                            .context("writing entries to stdout")
                    })
            })
    })
}

#[cfg(test)]
mod integration_tests;
