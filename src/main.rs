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

pub mod utils {
    use std::iter::once;

    use tap::Pipe;

    pub fn boxed_iter<'a, T: 'a>(iter: impl Iterator<Item = T> + 'a) -> Box<dyn Iterator<Item = T> + 'a> {
        Box::new(iter)
    }

    #[extension_traits::extension(pub trait IteratorTryFlatMapExt)]
    impl<'iter, T, E, I> I
    where
        E: 'iter,
        T: 'iter,
        I: Iterator<Item = Result<T, E>> + 'iter,
    {
        /// same as [Iterator::flat_map], but automatically handles errors
        fn try_flat_map<U, NewIterator, F>(self, mut try_flat_map: F) -> impl Iterator<Item = Result<U, E>> + 'iter
        where
            U: 'iter,
            NewIterator: Iterator<Item = Result<U, E>> + 'iter,
            F: FnMut(T) -> NewIterator + 'iter,
        {
            self.flat_map(move |e| match e {
                Ok(value) => value.pipe(&mut try_flat_map).pipe(boxed_iter),
                Err(e) => e.pipe(Err).pipe(once).pipe(boxed_iter),
            })
        }
    }
}

pub mod transaction {
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
}

pub mod transaction_source {
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
}

pub mod account {
    use std::collections::BTreeMap;

    use anyhow::Context;
    use tap::Pipe;

    use crate::{
        account::apply_transaction::{BalanceTransaction, mutation::Mutate},
        transaction::{Amount, Client, Tx, dispute::Dispute},
    };

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

    pub mod apply_transaction {
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
                Amount, Client, Transaction, TransactionKind, Tx, chargeback::Chargeback, deposit::Deposit, dispute::Dispute,
                resolve::Resolve, withdrawal::Withdrawal,
            },
        };

        pub mod mutation {
            use tap::Pipe;

            /// a simple contract - this type must mutate the element and
            /// return a reverse mutation
            pub trait Mutate<T>: Sized {
                /// must return reverse mutation
                fn mutate(self, value: &mut T) -> anyhow::Result<Self>;
            }

            /// simplest implementation
            #[diagnostic::do_not_recommend]
            impl<T> Mutate<T> for T {
                fn mutate(self, value: &mut T) -> anyhow::Result<Self> {
                    Ok(std::mem::replace(value, self))
                }
            }

            #[derive(Debug)]
            pub struct Undo<M>(Vec<M>);

            impl<M> std::default::Default for Undo<M> {
                fn default() -> Self {
                    Self(Vec::new())
                }
            }

            impl<M> Undo<M> {
                pub fn with_capacity(capacity: usize) -> Self {
                    Self(Vec::with_capacity(capacity))
                }

                pub fn push(mut self, mutation: M) -> Self {
                    self.0.push(mutation);
                    self
                }

                pub fn undo<V>(self, value: &mut V, reason: &impl std::fmt::Debug)
                where
                    M: Mutate<V> + std::fmt::Debug,
                {
                    self.0
                        .into_iter()
                        // undo mutations are pushed onto a stack, so total undo needs to go in reverse order
                        .rev()
                        .for_each(|undo| {
                            // this would produce an illegal state, so I guess it's best to panic
                            let debug = format!("{undo:#?}");
                            undo.mutate(value).unwrap_or_else(|could_not_rollback| {
                                panic!(
                                    "Could not undo a mutation: {debug}\n\
                                within a transaction:\n\n\
                                reason: {could_not_rollback:?}\n\
                                rolling back because:{reason:?}"
                                )
                            });
                        });
                }
            }

            /// Performs mutations `M` in one transaction, guaranteeing that in case one of the mutations fails, they are all reverted,
            /// so the state of `T` is unchanged
            pub fn in_one_transaction<M, T>(value: &mut T, mutations: Vec<M>) -> anyhow::Result<Undo<M>>
            where
                M: Mutate<T> + std::fmt::Debug,
            {
                let count = mutations.len();
                mutations
                    .into_iter()
                    .try_fold(Undo::<M>::with_capacity(count), |to_undo, next| match next.mutate(value) {
                        Ok(undo) => to_undo.push(undo).pipe(Ok),
                        Err(reason) => {
                            tracing::warn!("ROLLING BACK:\n{reason:?}");
                            to_undo.undo(value, &reason);
                            Err(reason)
                        }
                    })
            }

            /// using the mutation api guarantees no illegal state will be saved
            pub struct TransactionState<'a, T, M> {
                pub undo: Undo<M>,
                pub state: &'a mut T,
            }

            impl<'a, T, M> TransactionState<'a, T, M>
            where
                M: Mutate<T> + std::fmt::Debug,
            {
                /// using the mutation api guarantees no illegal state will be saved
                pub fn new(value: &'a mut T) -> Self {
                    Self {
                        undo: Undo::<M>::default(),
                        state: value,
                    }
                }
                pub fn mutate(mut self, mutate: impl FnOnce(&T) -> anyhow::Result<Vec<M>>) -> anyhow::Result<Self> {
                    mutate(self.state)
                        .and_then(|mutations| in_one_transaction(self.state, mutations))
                        .map(|undo| {
                            self.undo.0.extend(undo.0);
                            self
                        })
                }
                pub fn finish(self) -> &'a T {
                    self.state
                }
            }
        }

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
            fn with_account_funds(
                &mut self,
                client: Client,
                with: impl FnOnce(&AccountState) -> Result<Vec<AccountStateMutation>>,
            ) -> Result<()> {
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
                                        BalanceTransactionHistoryMutation::Insert(
                                            withdrawal.tx,
                                            withdrawal.clone().pipe(BalanceTransaction::from),
                                        )
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
    }
}

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
                        tracing::warn!("could not apply transaction:\n{reason:?}");
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
mod integration_tests {
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
}
