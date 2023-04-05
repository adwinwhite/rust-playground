use std::env;

use orchestrator::worker::listen;

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    // Panics messages are written to stderr.
    let project_dir = env::args().nth(1).expect("Please specify project directory as the first argument");
    listen(project_dir.into()).await.unwrap();
}
