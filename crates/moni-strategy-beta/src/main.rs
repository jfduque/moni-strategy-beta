#[tokio::main]
async fn main() {
    if let Err(error) = moni_strategy_beta::run_cli(std::env::args()).await {
        tracing::error!(error = %error, "strategy-beta stopped");
        std::process::exit(1);
    }
}
