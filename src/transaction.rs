use anyhow::Context;
use serde::Deserialize;
use tap::Pipe;

use crate::transaction::{chargeback::Chargeback, deposit::Deposit, dispute::Dispute, resolve::Resolve, withdrawal::Withdrawal};

/// Client ID - unique per client, though not guaranteed to
/// be ordered: Transactions to the client account 2 could
/// occur before transactions to the client account 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, serde::Serialize)]
pub struct Client(pub(crate) u16);

/// Transaction IDs (tx) are globally unique, though are
/// also not guaranteed to be ordered. You can assume the
/// transactions occur chronologically in the file, so if
/// transaction b appears after a in the input file then you can
/// assume b occurred chronologically after a.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub struct Tx(u32);

/// A decimal value with a precision of up to four places past the decimal.
/// TODO: Make an actual newtype for safer maths:
///    [crate::transaction::withdrawal::Withdrawal] amounts whould not be a negative number
///    [crate::transaction::deposit::Deposit] amounts whould not be a negative number either
pub type Amount = rust_decimal::Decimal;

pub mod deposit {
    use {
        super::{Amount, Client, Tx},
        serde::Deserialize,
    };

    /// A deposit is a credit to the client's asset account, meaning
    /// it should increase the available and total funds of the
    /// client account
    #[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
    pub struct Deposit {
        pub client: Client,
        pub tx: Tx,
        pub amount: Amount,
    }
}

pub mod withdrawal {
    use {
        super::{Amount, Client, Tx},
        serde::Deserialize,
    };

    /// A withdraw is a debit to the client's asset account,
    /// meaning it should decrease the available and total funds
    /// of the client account.
    ///
    /// If a client does not have sufficient
    /// available funds the withdrawal should fail and the total
    /// amount of funds should not change
    #[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
    pub struct Withdrawal {
        pub client: Client,
        pub tx: Tx,
        pub amount: Amount,
    }
}

pub mod dispute {
    use {
        super::{Client, Tx},
        serde::Deserialize,
    };

    /// A dispute represents a client's claim that a [super::Transaction] was
    /// erroneous and should be reversed. The [super::Transaction] shouldn't
    /// be reversed yet but the associated funds should be held.
    /// This means that the clients available funds should decrease
    /// by the amount disputed, their held funds should increase by
    /// the amount disputed, while their total funds should remain
    /// the same.
    ///
    /// Notice that a dispute does not state the amount
    /// disputed. Instead a dispute references the transaction
    /// that is disputed by [Tx]. If the tx specified by the dispute
    /// doesn't exist you can ignore it and assume this is an error
    /// on our partners side.
    #[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
    pub struct Dispute {
        pub client: Client,
        pub tx: Tx,
    }
}

pub mod resolve {
    use {
        super::{Client, Tx},
        serde::Deserialize,
    };

    /// A resolve represents a resolution to a [super::dispute::Dispute], releasing
    /// the associated held funds. Funds that were previously
    /// disputed are no longer disputed. This means that the [Client]
    /// held funds should decrease by the amount no longer disputed,
    /// their available funds should increase by the amount no
    /// longer disputed, and their total funds should remain the
    /// same.
    /// Like [super::dispute::Dispute]s, resolves do not specify an amount.
    /// Instead they refer to a transaction that was under dispute
    /// by [Tx]. If the [Tx] specified doesn't exist, or the [Tx] isn't
    /// under dispute, you can ignore the resolve and assume this is
    /// an error on our partner's side.
    #[derive(Debug, Deserialize, Clone)]
    pub struct Resolve {
        pub client: Client,
        pub tx: Tx,
    }
}

pub mod chargeback {
    use {
        super::{Client, Tx},
        serde::Deserialize,
    };

    /// A chargeback is the final state of a [super::dispute::Dispute] and represents
    /// the [Client] reversing a [super::Transaction].
    /// Funds that were held have now been withdrawn. This means
    /// that the [Client]'s held funds and total
    /// funds should decrease by the amount previously disputed. If
    /// a chargeback occurs the client's
    /// account should be immediately frozen.
    ///
    /// Like a dispute and a resolve a chargeback refers to the
    /// transaction by ID ([Tx]) and does not
    /// specify an amount. Like a resolve, if the tx specified
    /// doesn't exist, or the tx isn't under dispute,
    /// you can ignore chargeback and assume this is an error on our
    /// partner's side.
    #[derive(Debug, Deserialize, Clone)]
    pub struct Chargeback {
        pub client: Client,
        pub tx: Tx,
    }
}

#[derive(Clone, Copy, Debug, derive_more::Display, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransactionKind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl TransactionKind {
    pub fn from(transaction: &Transaction) -> Self {
        match transaction {
            Transaction::Deposit(_) => TransactionKind::Deposit,
            Transaction::Withdrawal(_) => TransactionKind::Withdrawal,
            Transaction::Dispute(_) => TransactionKind::Dispute,
            Transaction::Resolve(_) => TransactionKind::Resolve,
            Transaction::Chargeback(_) => TransactionKind::Chargeback,
        }
    }
}

impl<'de> serde::Deserialize<'de> for Transaction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Row {
            #[serde(rename = "type")]
            pub kind: TransactionKind,
            pub client: Client,
            pub tx: Tx,
            pub amount: Option<Amount>,
        }
        Row::deserialize(deserializer).and_then(|Row { kind, client, tx, amount }| {
            match kind {
                TransactionKind::Deposit => Self::Deposit(Deposit {
                    client,
                    tx,
                    amount: amount
                        .context("no amount")
                        .map_err(serde::de::Error::custom)?,
                }),
                TransactionKind::Withdrawal => Self::Withdrawal(Withdrawal {
                    client,
                    tx,
                    amount: amount
                        .context("no amount")
                        .map_err(serde::de::Error::custom)?,
                }),
                TransactionKind::Dispute => Self::Dispute(Dispute { client, tx }),
                TransactionKind::Resolve => Self::Resolve(Resolve { client, tx }),
                TransactionKind::Chargeback => Self::Chargeback(Chargeback { client, tx }),
            }
            .pipe(Ok)
        })
    }
}
#[derive(Debug, Clone, derive_more::From)]
pub enum Transaction {
    Deposit(Deposit),
    Withdrawal(withdrawal::Withdrawal),
    Dispute(dispute::Dispute),
    Resolve(resolve::Resolve),
    Chargeback(chargeback::Chargeback),
}

impl Transaction {
    pub fn id(&self) -> Tx {
        match self {
            Transaction::Deposit(i) => i.tx,
            Transaction::Withdrawal(i) => i.tx,
            Transaction::Dispute(i) => i.tx,
            Transaction::Resolve(i) => i.tx,
            Transaction::Chargeback(i) => i.tx,
        }
    }
    pub fn kind(&self) -> TransactionKind {
        TransactionKind::from(self)
    }
    pub fn client(&self) -> Client {
        match self {
            Transaction::Deposit(i) => i.client,
            Transaction::Withdrawal(i) => i.client,
            Transaction::Dispute(i) => i.client,
            Transaction::Resolve(i) => i.client,
            Transaction::Chargeback(i) => i.client,
        }
    }
}
