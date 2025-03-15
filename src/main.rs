#[tokio::main(flavor = "current_thread")]
async fn main() {
    orderbook::run().await;
}
