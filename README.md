# concurrency-control

## How to run

Navigate to one of the folders implementing the concurrency control method that you want to use:
- mvto for Multiversion Timestamp Ordering Protocol
- occ for Serial Optimistic Concurrency Control
- simple_locking for Simple Locking (Exclusive Locks Only)

then run `python main.py`.

Our program accepts input in the form of `R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;`
