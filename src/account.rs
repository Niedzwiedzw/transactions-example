use std::collections::BTreeMap;

use anyhow::Context;
use tap::Pipe;

use crate::{
    account::apply_transaction::{BalanceTransaction, mutation::Mutate},
    transaction::{Amount, Client, Tx, dispute::Dispute},
};

pub mod apply_transaction;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
/// [crate::transaction::Client]'s account state
pub struct AccountFunds {
    /// The total funds that are available for trading, staking,
    /// withdrawal, etc.
    /// This should be equal to the total - held amounts
    pub available: Amount,
    /// The total funds that are held for [crate::transaction::dispute::Dispute]. This should be
    /// equal to total - available amounts
    pub held: Amount,
    /// The total funds that are available or held. This should be
    ///  equal to available + held
    pub total: Amount,
}

#[derive(Debug)]
pub enum AccountFundsMutation {
    Available(AmountMutation),
    Held(AmountMutation),
    Total(AmountMutation),
}

#[derive(Debug)]
pub enum AmountMutation {
    IncreaseBy(Amount),
    DecreaseBy(Amount),
}

impl Mutate<Amount> for AmountMutation {
    fn mutate(self, mut value: &mut Amount) -> anyhow::Result<Self> {
        match self {
            AmountMutation::IncreaseBy(by) => {
                value += by;
                Self::DecreaseBy(by)
            }
            AmountMutation::DecreaseBy(by) => {
                value -= by;
                Self::IncreaseBy(by)
            }
        }
        .pipe(Ok)
    }
}

impl Mutate<AccountFunds> for AccountFundsMutation {
    fn mutate(self, value: &mut AccountFunds) -> anyhow::Result<Self> {
        match self {
            AccountFundsMutation::Available(available) => available.mutate(&mut value.available).map(Self::Available),
            AccountFundsMutation::Held(held) => held.mutate(&mut value.held).map(Self::Held),
            AccountFundsMutation::Total(total) => total.mutate(&mut value.total).map(Self::Total),
        }
    }
}

pub type BalanceTransactionHistory = BTreeMap<Tx, BalanceTransaction>;
#[derive(Debug)]
pub enum BalanceTransactionHistoryMutation {
    Insert(Tx, BalanceTransaction),
    Remove(Tx),
}
pub type CurrentDisputes = BTreeMap<Tx, Dispute>;

impl Mutate<BalanceTransactionHistory> for BalanceTransactionHistoryMutation {
    fn mutate(self, value: &mut BalanceTransactionHistory) -> anyhow::Result<Self> {
        match self {
            BalanceTransactionHistoryMutation::Insert(tx, transaction) => match value.contains_key(&tx) {
                false => value
                    .insert(tx, transaction)
                    .pipe(|v| assert_is_none(v, "expected key to be checked"))
                    .pipe(|()| Self::Remove(tx))
                    .pipe(Ok),
                true => Err(anyhow::anyhow!("Dispute for {tx:?} already exists")),
            },
            BalanceTransactionHistoryMutation::Remove(tx) => value
                .remove(&tx)
                .with_context(|| format!("Expected transaction at {tx:?}"))
                .map(|dispute| Self::Insert(tx, dispute)),
        }
    }
}

#[derive(Debug)]
pub enum CurrentDisputesMutation {
    Insert(Tx, Dispute),
    Remove(Tx),
}

fn assert_is_none<T>(v: Option<T>, context: &'static str) {
    if let Some(_v) = v {
        unreachable!("expected value to be None: {context}")
    }
}
impl Mutate<CurrentDisputes> for CurrentDisputesMutation {
    fn mutate(self, value: &mut CurrentDisputes) -> anyhow::Result<Self> {
        match self {
            CurrentDisputesMutation::Insert(tx, dispute) => match value.contains_key(&tx) {
                false => value
                    .insert(tx, dispute)
                    .pipe(|v| assert_is_none(v, "expected key to be checked"))
                    .pipe(|()| Self::Remove(tx))
                    .pipe(Ok),
                true => Err(anyhow::anyhow!("Dispute for {tx:?} already exists")),
            },
            CurrentDisputesMutation::Remove(tx) => value
                .remove(&tx)
                .with_context(|| format!("Expected transaction at {tx:?}"))
                .map(|dispute| Self::Insert(tx, dispute)),
        }
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct AccountState {
    pub history: BalanceTransactionHistory,
    pub disputes: CurrentDisputes,
    pub funds: AccountFunds,
}

#[derive(Debug, derive_more::From)]
pub enum AccountStateMutation {
    Disputes(CurrentDisputesMutation),
    Funds(AccountFundsMutation),
    History(BalanceTransactionHistoryMutation),
}

impl Mutate<AccountState> for AccountStateMutation {
    fn mutate(self, value: &mut AccountState) -> anyhow::Result<Self> {
        match self {
            AccountStateMutation::Disputes(m) => m.mutate(&mut value.disputes).map(Self::Disputes),
            AccountStateMutation::History(m) => m.mutate(&mut value.history).map(Self::History),
            AccountStateMutation::Funds(m) => m.mutate(&mut value.funds).map(Self::Funds),
        }
    }
}

impl AccountState {
    pub fn with_funds(funds: AccountFunds) -> Self {
        Self {
            history: Default::default(),
            disputes: Default::default(),
            funds,
        }
    }
}

/// Top level storage for all account state
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Accounts {
    pub accounts: BTreeMap<Client, AccountState>,
}

impl Accounts {
    /// impl Iterator<...> would also suffice, but would suffer from
    /// not having some useful traits implemented, like [std::iter::DoubleEndedIterator]
    /// which [std::collections::btree_map::IntoIter] implements
    pub fn accounts<'a>(&'a self) -> std::collections::btree_map::Iter<'a, Client, AccountState> {
        self.accounts.iter()
    }
}
