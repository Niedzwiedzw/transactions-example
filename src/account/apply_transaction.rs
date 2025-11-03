use std::{
    collections::btree_map,
    iter::{empty, once},
    ops::Not,
};

use anyhow::Context;
use tap::Pipe;

use crate::{
    account::{
        AccountFunds, AccountFundsMutation, AccountState, AccountStateMutation, Accounts, AmountMutation,
        BalanceTransactionHistoryMutation, CurrentDisputesMutation, apply_transaction::mutation::TransactionState,
    },
    transaction::{
        Amount, Client, Transaction, TransactionKind, Tx, chargeback::Chargeback, deposit::Deposit, dispute::Dispute, resolve::Resolve,
        withdrawal::Withdrawal,
    },
};

pub mod mutation;

/// Error that can occur when applying a transaction
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("No such client: {0:?}")]
    NoSuchClient(Client),
    #[error("Account is not locked")]
    AccountIsNotLocked,
    #[error("Account is not unlocked")]
    AccountIsNotUnlocked,
    #[error("Insufficient funds. Tried amount: {0}")]
    InsufficientFunds(Amount),
    #[error("Transaction would have introduced an illegal state: {0:?}")]
    IllegalState(AccountFunds),
    #[error("Transaction with id [{0:?}] does not exist")]
    NoSuchTx(Tx),
    #[error("You cannot dispute transaction of kind: '{0}'")]
    BadDispute(TransactionKind),
    #[error("Transaction {0:?} is already disputed.")]
    AlreadyDisputed(Tx),
    #[error("No dispute for transaction [{0:?}]")]
    NoSuchDispute(Tx),
    #[error("Could not apply the transaction")]
    ApplyingTransaction(#[source] anyhow::Error),
}

/// Possible [self::Error] that can occur when applying a [Transaction]
type Result<T> = std::result::Result<T, self::Error>;

impl AccountFunds {
    fn validate(self) -> Result<Self> {
        {
            if self.available + self.held == self.total {
                Ok(self)
            } else {
                Err(self::Error::IllegalState(self))
            }
        }
    }
    fn deposit(&self, amount: Amount) -> [AccountFundsMutation; 2] {
        [
            AccountFundsMutation::Available(AmountMutation::IncreaseBy(amount)),
            AccountFundsMutation::Total(AmountMutation::IncreaseBy(amount)),
        ]
    }
    fn withdraw(&self, amount: Amount) -> Result<[AccountFundsMutation; 2]> {
        (self.available > amount)
            .then_some(())
            .ok_or(self::Error::InsufficientFunds(amount))
            .map(|_| {
                [
                    AccountFundsMutation::Available(AmountMutation::DecreaseBy(amount)),
                    AccountFundsMutation::Total(AmountMutation::DecreaseBy(amount)),
                ]
            })
    }
    fn dispute_amount(&self, disputed: Amount) -> [AccountFundsMutation; 2] {
        [
            AccountFundsMutation::Available(AmountMutation::DecreaseBy(disputed)),
            AccountFundsMutation::Held(AmountMutation::IncreaseBy(disputed)),
        ]
    }
    fn resolve_amount(&self, disputed: Amount) -> [AccountFundsMutation; 2] {
        [
            AccountFundsMutation::Available(AmountMutation::IncreaseBy(disputed)),
            AccountFundsMutation::Held(AmountMutation::DecreaseBy(disputed)),
        ]
    }

    fn chargeback_amount(&self, disputed: Amount) -> [AccountFundsMutation; 2] {
        [
            AccountFundsMutation::Total(AmountMutation::DecreaseBy(disputed)),
            AccountFundsMutation::Held(AmountMutation::DecreaseBy(disputed)),
        ]
    }
}

/// the kind of transaction that should end up in actual transaction history,
/// rather than being an event around for transactions
#[derive(derive_more::From, Debug, PartialEq, Eq)]
pub enum BalanceTransaction {
    Withdrawal(Withdrawal),
    Deposit(Deposit),
}

#[derive(derive_more::From, Debug)]
pub enum BalanceTransactionRef<'a> {
    Withdrawal(&'a Withdrawal),
    Deposit(&'a Deposit),
}

impl<'a> BalanceTransactionRef<'a> {
    /// Not really needed, but will make sure it's in sync with [BalanceTransaction]
    pub fn into_owned(&self) -> BalanceTransaction {
        match self {
            BalanceTransactionRef::Withdrawal(withdrawal) => BalanceTransaction::Withdrawal((*withdrawal).clone()),
            BalanceTransactionRef::Deposit(deposit) => BalanceTransaction::Deposit((*deposit).clone()),
        }
    }

    pub fn from_transaction(transaction: &'a Transaction) -> std::result::Result<Self, &'a Transaction> {
        match transaction {
            Transaction::Deposit(deposit) => Ok(BalanceTransactionRef::Deposit(deposit)),
            Transaction::Withdrawal(withdrawal) => Ok(BalanceTransactionRef::Withdrawal(withdrawal)),
            other => Err(other),
        }
    }
}

impl BalanceTransaction {
    /// Not really needed, but will make sure it's in sync with [BalanceTransactionRef]
    pub fn as_ref(&self) -> BalanceTransactionRef<'_> {
        match self {
            BalanceTransaction::Withdrawal(withdrawal) => BalanceTransactionRef::Withdrawal(withdrawal),
            BalanceTransaction::Deposit(deposit) => BalanceTransactionRef::Deposit(deposit),
        }
    }
    pub fn from_transaction(transaction: Transaction) -> std::result::Result<Self, Transaction> {
        match transaction {
            Transaction::Deposit(deposit) => Ok(BalanceTransaction::Deposit(deposit)),
            Transaction::Withdrawal(withdrawal) => Ok(BalanceTransaction::Withdrawal(withdrawal)),
            other => Err(other),
        }
    }

    pub fn into_transaction(self) -> Transaction {
        match self {
            BalanceTransaction::Withdrawal(i) => Transaction::from(i),
            BalanceTransaction::Deposit(i) => Transaction::from(i),
        }
    }
}

impl Accounts {
    /// Takes care of account creation and modification of the user acount. No change is saved in case an error occurs.
    fn with_account_funds(&mut self, client: Client, with: impl FnOnce(&AccountState) -> Result<Vec<AccountStateMutation>>) -> Result<()> {
        /// applies mutations and performs validation
        fn mutate_account(
            account_state: &mut AccountState,
            with: impl FnOnce(&AccountState) -> Result<Vec<AccountStateMutation>>,
        ) -> Result<()> {
            Ok(TransactionState::new(account_state))
                // apply necessary mutations
                .and_then(|transaction| transaction.mutate(|a| with(a).context("generating mutations for account")))
                // validate the state before save
                .and_then(|transaction| {
                    transaction.mutate(|account_state| {
                        account_state
                            .funds
                            .validate()
                            .context("validating account before save")
                            .map(|_| vec![])
                    })
                })
                .map(|_| ())
                .map_err(self::Error::ApplyingTransaction)
        }

        self.pipe(|Self { accounts }| match accounts.entry(client) {
            btree_map::Entry::Vacant(vacant_entry) => AccountState::default().pipe(|mut account_state| {
                mutate_account(&mut account_state, with).map(|()| {
                    vacant_entry.insert(account_state);
                })
            }),
            btree_map::Entry::Occupied(occupied_entry) => occupied_entry
                .into_mut()
                .pipe(|current| mutate_account(current, with)),
        })
    }
    pub fn next_transaction(&mut self, transaction: Transaction) -> Result<()> {
        self.with_account_funds(
            transaction.client(),
            |AccountState { history, disputes, funds }| match &transaction {
                Transaction::Deposit(deposit @ Deposit { client: _, tx: _, amount }) => empty()
                    // calculate the delta
                    .chain(
                        funds
                            .deposit(*amount)
                            .into_iter()
                            .map(AccountStateMutation::Funds),
                    )
                    // insert into history
                    .chain(
                        BalanceTransactionHistoryMutation::Insert(deposit.tx, deposit.clone().pipe(BalanceTransaction::from))
                            .pipe(AccountStateMutation::History)
                            .pipe(once),
                    )
                    .collect::<Vec<_>>()
                    .pipe(Ok),
                Transaction::Withdrawal(withdrawal @ Withdrawal { client: _, tx: _, amount }) => {
                    funds.withdraw(*amount).map(|withdrawal_delta| {
                        empty()
                            // calculate the delta
                            .chain(
                                withdrawal_delta
                                    .into_iter()
                                    .map(AccountStateMutation::Funds),
                            )
                            // insert into history
                            .chain(
                                BalanceTransactionHistoryMutation::Insert(withdrawal.tx, withdrawal.clone().pipe(BalanceTransaction::from))
                                    .pipe(AccountStateMutation::History)
                                    .pipe(once),
                            )
                            .collect::<Vec<_>>()
                    })
                }
                Transaction::Dispute(
                    dispute @ Dispute {
                        client: _,
                        tx: disputed_tx,
                    },
                ) => disputes
                    .contains_key(disputed_tx)
                    .not()
                    .then_some(*disputed_tx)
                    .ok_or(self::Error::AlreadyDisputed(*disputed_tx))
                    .and_then(|disputed_tx| {
                        history
                            .get(&disputed_tx)
                            .ok_or(self::Error::NoSuchTx(disputed_tx))
                    })
                    .map(|transaction| match transaction {
                        BalanceTransaction::Deposit(Deposit { client: _, tx, amount }) => empty()
                            .chain(
                                funds
                                    .dispute_amount(*amount)
                                    .map(AccountStateMutation::Funds),
                            )
                            .chain(
                                CurrentDisputesMutation::Insert(*tx, dispute.clone())
                                    .pipe(AccountStateMutation::Disputes)
                                    .pipe(once),
                            )
                            .collect::<Vec<_>>(),
                        BalanceTransaction::Withdrawal(Withdrawal { client: _, tx, amount }) => empty()
                            .chain(
                                funds
                                    .dispute_amount(-*amount)
                                    .map(AccountStateMutation::Funds),
                            )
                            .chain(
                                CurrentDisputesMutation::Insert(*tx, dispute.clone())
                                    .pipe(AccountStateMutation::Disputes)
                                    .pipe(once),
                            )
                            .collect::<Vec<_>>(),
                    }),
                Transaction::Resolve(Resolve { client: _, tx }) => disputes
                    .get(tx)
                    .ok_or_else(|| self::Error::NoSuchDispute(*tx))
                    .and_then(|dispute| {
                        history
                            .get(tx)
                            .ok_or(self::Error::NoSuchTx(*tx))
                            .map(|trasnaction| (dispute, trasnaction))
                    })
                    .map(|(_dispute, transaction)| {
                        //
                        empty()
                            .chain(
                                match transaction {
                                    BalanceTransaction::Withdrawal(withdrawal) => -withdrawal.amount,
                                    BalanceTransaction::Deposit(deposit) => deposit.amount,
                                }
                                .pipe(|disputed| {
                                    funds
                                        .resolve_amount(disputed)
                                        .map(AccountStateMutation::Funds)
                                }),
                            )
                            .chain(
                                CurrentDisputesMutation::Remove(*tx)
                                    .pipe(AccountStateMutation::Disputes)
                                    .pipe(once),
                            )
                            .collect::<Vec<_>>()
                    }),
                Transaction::Chargeback(Chargeback { client: _, tx }) => disputes
                    .get(tx)
                    .ok_or_else(|| self::Error::NoSuchDispute(*tx))
                    .and_then(|dispute| {
                        history
                            .get(tx)
                            .ok_or(self::Error::NoSuchTx(*tx))
                            .map(|trasnaction| (dispute, trasnaction))
                    })
                    .map(|(_dispute, transaction)| {
                        //
                        empty()
                            .chain(
                                match transaction {
                                    BalanceTransaction::Withdrawal(withdrawal) => -withdrawal.amount,
                                    BalanceTransaction::Deposit(deposit) => deposit.amount,
                                }
                                .pipe(|disputed| {
                                    funds
                                        .chargeback_amount(disputed)
                                        .map(AccountStateMutation::Funds)
                                }),
                            )
                            .chain(
                                CurrentDisputesMutation::Remove(*tx)
                                    .pipe(AccountStateMutation::Disputes)
                                    .pipe(once),
                            )
                            .collect::<Vec<_>>()
                    }),
            },
        )
    }
}
