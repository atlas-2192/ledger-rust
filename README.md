
Blockchain implementation with PoW and network interaction with tokio

1. Build project `cargo build`
2. Start peer `run --package peer --bin peer`
3. Send transactions via client: `node::tests::send_transactions_to_network()`
4. Check transactions onchain `node::node::tests::receive_blockchain_request_and_response_ok()`

