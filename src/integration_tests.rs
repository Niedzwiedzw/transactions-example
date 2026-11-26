use super::*;

mod csv {
    use super::*;
    use crate::{
        account::{AccountFunds, Accounts},
        transaction::{Client, Transaction},
        transaction_source::load_transactions_csv_reader,
    };
    use anyhow::Result;
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;

    pub fn load_transactions_csv_str<'a>(source: &'a str) -> impl Iterator<Item = Result<Transaction>> + 'a {
        std::io::Cursor::new(source).pipe(load_transactions_csv_reader)
    }

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

    fn apply_row(accounts: &mut Accounts, row: &str) -> Result<()> {
        let input = format!("type,client,tx,amount\n{row}");
        let transaction = load_transactions_csv_str(&input)
            .next()
            .context("expected a transaction")??;

        accounts.next_transaction(transaction)?;
        Ok(())
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

    #[test]
    fn no_chargeback_does_not_lock_account() -> Result<()> {
        let accounts = after_transactions(
            "type,client,tx,amount
deposit,1,1,10
dispute,1,1,",
        )?;

        let (client, state) = accounts.accounts().next().unwrap();

        assert!(!AccountRow::new(*client, state).locked);
        Ok(())
    }

    #[test]
    fn chargeback_locks_account() -> Result<()> {
        let accounts = after_transactions(
            "type,client,tx,amount
deposit,1,1,10
dispute,1,1,
chargeback,1,1,",
        )?;

        let (client, state) = accounts.accounts().next().unwrap();

        assert!(AccountRow::new(*client, state).locked);
        assert_eq!(
            state.funds,
            AccountFunds {
                available: dec!(0),
                held: dec!(0),
                total: dec!(0),
            },
        );
        Ok(())
    }

    #[test]
    fn chargeback_blocks_subsequent_deposits_and_withdrawals() -> Result<()> {
        ["deposit,1,3,5", "withdrawal,1,3,5"]
            .into_iter()
            .try_for_each(|operation| -> Result<()> {
                let mut accounts = after_transactions(
                    "type,client,tx,amount
deposit,1,1,10
deposit,1,2,20
dispute,1,1,
chargeback,1,1,",
                )?;

                let input = format!("type,client,tx,amount\n{operation}");
                let transaction = load_transactions_csv_str(&input).next().unwrap()?;

                let _ = accounts.next_transaction(transaction);

                let (client, state) = accounts.accounts().next().unwrap();

                assert_eq!(
                    state.funds,
                    AccountFunds {
                        available: dec!(20),
                        held: dec!(0),
                        total: dec!(20),
                    },
                    "{operation} changed a frozen account",
                );
                assert!(AccountRow::new(*client, state).locked, "{operation} left the account unlocked",);
                Ok(())
            })
    }

    #[test]
    fn chargeback_does_not_lock_other_clients() -> Result<()> {
        let accounts = after_transactions(
            "type,client,tx,amount
deposit,1,1,10
deposit,2,2,20
dispute,1,1,
chargeback,1,1,
withdrawal,2,3,5",
        )?;

        let state = accounts.accounts.get(&Client(2)).unwrap();

        assert!(!AccountRow::new(Client(2), state).locked);
        assert_eq!(
            state.funds,
            AccountFunds {
                available: dec!(15),
                held: dec!(0),
                total: dec!(15),
            },
        );
        Ok(())
    }

    #[test]
    fn withdrawal_can_exhaust_available_funds() -> Result<()> {
        let accounts = after_transactions(
            "type,client,tx,amount
deposit,1,1,10
withdrawal,1,2,10",
        )?;

        let state = accounts.accounts.get(&Client(1)).unwrap();

        assert_eq!(AccountFunds::default(), state.funds);
        Ok(())
    }

    #[test]
    fn zero_withdrawal_succeeds_with_zero_available_funds() -> Result<()> {
        let mut accounts = after_transactions(
            "type,client,tx,amount
deposit,1,1,10
dispute,1,1,",
        )?;

        apply_row(&mut accounts, "withdrawal,1,2,0")?;

        assert_eq!(
            AccountFunds {
                available: dec!(0),
                held: dec!(10),
                total: dec!(10),
            },
            accounts.accounts.get(&Client(1)).unwrap().funds,
        );
        Ok(())
    }

    #[test]
    fn withdrawal_dispute_leaves_account_unchanged() -> Result<()> {
        let input = "type,client,tx,amount
deposit,1,1,10
withdrawal,1,2,3";

        let expected = after_transactions(input)?;
        let mut actual = after_transactions(input)?;

        let dispute = load_transactions_csv_str(
            "type,client,tx,amount
dispute,1,2,",
        )
        .next()
        .context("expected a dispute")??;

        let _ = actual.next_transaction(dispute);

        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn withdrawal_chargeback_restores_withdrawn_funds() -> Result<()> {
        let mut accounts = after_transactions(
            "type,client,tx,amount
deposit,1,1,10
withdrawal,1,2,3",
        )?;

        apply_row(&mut accounts, "dispute,1,2,")?;

        let state = accounts.accounts.get(&Client(1)).unwrap();

        assert_eq!(
            state.funds,
            AccountFunds {
                available: dec!(10),
                held: dec!(-3),
                total: dec!(7),
            },
        );

        apply_row(&mut accounts, "chargeback,1,2,")?;

        let state = accounts.accounts.get(&Client(1)).unwrap();

        assert_eq!(
            state.funds,
            AccountFunds {
                available: dec!(10),
                held: dec!(0),
                total: dec!(10),
            },
        );
        Ok(())
    }
}
