use enutt::cluster::Cluster;
use enutt::config::ConfigBuilder;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

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

#[tokio::test(core_threads = 4)]
async fn join() -> enutt::Result<()> {
    config_tracing(Level::INFO);
    let mut peers = Vec::with_capacity(11);

    let boostrap_peer = Cluster::new(ConfigBuilder::default().finish()?).await?;
    boostrap_peer.bootstrap().await?;
    peers.push(boostrap_peer);

    // run peers
    let start = 9011;
    let end = 9031;
    for peer_port in start..end {
        let peer = Cluster::new(ConfigBuilder::default().port(peer_port as u16).finish()?).await?;
        peer.bootstrap().await?;
        peers.push(peer);
    }

    // give them few seconds to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(10)).await;

    // validate membership lists
    peers.into_iter().enumerate().for_each(|(i, peer)| {
        let node = peer.context().node();
        let peers = node.peers().read();
        assert_eq!((end - start) + 1, peers.len());
        drop(peers);
    });

    Ok(())
}
