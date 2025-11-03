use super::*;

mod csv {
    use super::*;
    use crate::{
        account::{AccountFunds, Accounts},
        transaction::Client,
        transaction_source::load_transactions_csv_str,
    };
    use anyhow::Result;
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;

    fn after_transactions(csv: &str) -> Result<Accounts> {
        load_transactions_csv_str(csv.trim()).try_fold(Accounts::default(), |mut acc, next| {
            next.and_then(|next| {
                let next_debug = format!("{next:#?}");
                acc.next_transaction(next)
                    .with_context(|| format!("applying transaction:\n{next_debug}"))
            })
            .map(|_| acc)
        })
    }

    #[test]
    fn test_empty() -> Result<()> {
        const TRANSACTIONS: &str = r#"
            "#;

        after_transactions(TRANSACTIONS).map(|accounts| {
            assert_eq!(Accounts::default(), accounts);
        })
    }

    #[test]
    fn test_single_deposit() -> Result<()> {
        const TRANSACTIONS: &str = r#"
type, client, tx, amount
deposit, 1, 1, 1.0
            "#;

        after_transactions(TRANSACTIONS).map(|accounts| {
            assert_eq!(
                vec![(
                    Client(1),
                    AccountFunds {
                        available: dec!(1.),
                        held: dec!(0.),
                        total: dec!(1)
                    }
                )],
                accounts
                    .accounts()
                    .map(|(a, s)| (*a, s.funds))
                    .collect::<Vec<_>>()
            );
        })
    }

    #[test]
    fn test_init_dispute() -> Result<()> {
        const TRANSACTIONS: &str = r#"
type, client, tx, amount
deposit, 1, 1, 1.0
dispute, 1, 1, 
            "#;

        after_transactions(TRANSACTIONS).map(|accounts| {
            assert_eq!(
                vec![(
                    Client(1),
                    AccountFunds {
                        available: dec!(0.),
                        held: dec!(1.),
                        total: dec!(1)
                    }
                )],
                accounts
                    .accounts()
                    .map(|(a, s)| (*a, s.funds))
                    .collect::<Vec<_>>()
            );
        })
    }

    #[test]
    fn test_resolve_dispute() -> Result<()> {
        const TRANSACTIONS: &str = r#"
type, client, tx, amount
deposit, 1, 1, 1.0
dispute, 1, 1,
resolve, 1, 1,

            "#;

        after_transactions(TRANSACTIONS).map(|accounts| {
            assert_eq!(
                vec![(
                    Client(1),
                    AccountFunds {
                        available: dec!(1.),
                        held: dec!(0.),
                        total: dec!(1)
                    }
                )],
                accounts
                    .accounts()
                    .map(|(a, s)| (*a, s.funds))
                    .collect::<Vec<_>>()
            );
        })
    }

    #[test]
    fn test_chargeback_dispute() -> Result<()> {
        const TRANSACTIONS: &str = r#"
type, client, tx, amount
deposit, 1, 1, 1.0
dispute, 1, 1,
chargeback, 1, 1,

            "#;

        after_transactions(TRANSACTIONS).map(|accounts| {
            assert_eq!(
                vec![(
                    Client(1),
                    AccountFunds {
                        available: dec!(0.),
                        held: dec!(0.),
                        total: dec!(0)
                    }
                )],
                accounts
                    .accounts()
                    .map(|(a, s)| (*a, s.funds))
                    .collect::<Vec<_>>()
            );
        })
    }
    #[test]

    fn test_withdraw_over_limit() {
        const TRANSACTIONS: &str = r#"
type, client, tx, amount
deposit, 1, 1, 1.0
withdrawal, 1, 2, 2.0
            "#;
        assert!(
            after_transactions(TRANSACTIONS)
                .err()
                .unwrap()
                .pipe(|e| format!("{e:?}"))
                .contains("Insufficient funds"),
            "expected insufficient funds error"
        )
    }
}
