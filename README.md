# Example transactions system
Here is a showcase of example transaction system implemented in Rust. Atomicity is ensured using Trasactions API: discrete mutations, each taking care of it's own rollback (in case of error).
Mutations have low overhead as they operate on mutable references and and only what's changed is allocated. Thanks to this we don't have to snapshot entire Account state. In a system where <Client -> Client> transactions exist, this would shine even more. For now it just makes it harder to make a mistake when rolling back on error.

## TODO:
 - Create more fine-grained Amount type. Amount should never be negative, while this system allows it.
 - Write more tests
