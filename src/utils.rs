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
