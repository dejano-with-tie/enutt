use tracing::{info, info_span, Level};
use tracing_subscriber::FmtSubscriber;

use enutt::cluster::Cluster;
use enutt::config::ConfigBuilder;

#[tokio::main]
async fn main() -> enutt::Result<()> {
    config_tracing(Level::INFO);

    info_span!("app");

    let cluster = Cluster::new(ConfigBuilder::default().finish()?).await?;

    cluster.bootstrap().await?;

    tokio::signal::ctrl_c().await?;
    info!("Got ctrl-c");
    Ok(())
}

fn config_tracing(level: Level) {
    let format = tracing_subscriber::fmt::format()
        .with_target(false) // Don't include event targets.
        .with_level(true) // Don't include event levels.
        .with_thread_ids(true);

    let _ = FmtSubscriber::builder()
        .with_max_level(level)
        .event_format(format)
        .init();
    // completes the builder.
    //     .finish();
    // tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
