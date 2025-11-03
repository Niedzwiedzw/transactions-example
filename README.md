# Example transactions system
Here is a showcase of example transaction system implemented in Rust. Atomicity is at compile time using Trasactions pattern with discrete mutation. Mutations are rolled-back in case of error. 


## TODO:
 - Create more fine-grained Amount type. Amount should never be negative, while this system allows it.
 - Write more tests
