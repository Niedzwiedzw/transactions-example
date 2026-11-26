use tap::{Pipe, TapFallible};

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
fn in_one_transaction<M, T>(value: &mut T, mutations: Vec<M>) -> anyhow::Result<Undo<M>>
where
    M: Mutate<T> + std::fmt::Debug,
{
    let count = mutations.len();
    mutations
        .into_iter()
        .try_fold(Undo::<M>::with_capacity(count), |to_undo, next| match next.mutate(value) {
            Ok(undo) => to_undo.push(undo).pipe(Ok),
            Err(reason) => {
                eprintln!("[ERR] ROLLING BACK:\n{reason:?}");
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
            .tap_err(|reason| std::mem::take(&mut self.undo).undo(self.state, &reason))
            .map(|undo| {
                self.undo.0.extend(undo.0);
                self
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Value(u32);

    #[derive(Debug)]
    enum Mutation {
        Set(Value),
        Fail,
    }

    impl Mutate<Value> for Mutation {
        fn mutate(self, value: &mut Value) -> anyhow::Result<Self> {
            match self {
                Self::Set(next) => Ok(Self::Set(std::mem::replace(value, next))),
                Self::Fail => anyhow::bail!("mutation failed"),
            }
        }
    }

    #[test]
    fn failed_batch_undoes_mutations_in_reverse_order() {
        let mut value = Value(7);

        let result = in_one_transaction(&mut value, vec![Mutation::Set(Value(11)), Mutation::Set(Value(19)), Mutation::Fail]);

        assert!(result.is_err());
        assert_eq!(value, Value(7));
    }

    #[test]
    fn validation_failure_undoes_previous_stage() {
        let mut value = Value(7);

        let result = TransactionState::<_, Mutation>::new(&mut value)
            .mutate(|_| Ok(vec![Mutation::Set(Value(11))]))
            .and_then(|transaction| transaction.mutate(|_| anyhow::bail!("validation failed")))
            .map(|_| ());

        assert!(result.is_err());
        assert_eq!(value, Value(7));
    }

    #[test]
    fn failed_later_batch_undoes_all_stages() {
        let mut value = Value(7);

        let result = TransactionState::<_, Mutation>::new(&mut value)
            .mutate(|_| Ok(vec![Mutation::Set(Value(11))]))
            .and_then(|transaction| transaction.mutate(|_| Ok(vec![Mutation::Set(Value(19)), Mutation::Fail])))
            .map(|_| ());

        assert!(result.is_err());
        assert_eq!(value, Value(7));
    }

    #[test]
    fn successful_stages_preserve_changes() {
        let mut value = Value(7);

        let result = TransactionState::<_, Mutation>::new(&mut value)
            .mutate(|_| Ok(vec![Mutation::Set(Value(11))]))
            .and_then(|transaction| transaction.mutate(|_| Ok(vec![Mutation::Set(Value(19))])))
            .map(|_| ());

        assert!(result.is_ok());
        assert_eq!(value, Value(19));
    }
}
