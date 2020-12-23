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
}

async fn bootstrap_peers(n: usize) -> enutt::Result<Vec<Cluster>> {
    let start = 9011;
    let end = start + (n - 1);
    let mut peers = Vec::with_capacity(n);

    let boostrap_peer = Cluster::new(ConfigBuilder::default().finish()?).await?;
    boostrap_peer.bootstrap().await?;
    peers.push(boostrap_peer);

    // run peers
    for peer_port in start..end {
        let peer = Cluster::new(ConfigBuilder::default().port(peer_port as u16).finish()?).await?;
        peer.bootstrap().await?;
        peers.push(peer);
        // give it time for each peer so we don't over flood the cluster
        tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
    }

    Ok(peers)
}

#[tokio::test(core_threads = 4)]
async fn join() -> enutt::Result<()> {
    // config_tracing(Level::INFO);
    let expected_peers = 40;
    let peers = bootstrap_peers(expected_peers).await?;

    // give them few seconds to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(5)).await;

    // validate membership lists
    peers.into_iter().enumerate().for_each(|(_i, peer)| {
        let node = peer.context().node();
        let peers = node.peers().read();
        assert_eq!(expected_peers, peers.len());
        // TODO: Validate content of membership lists
        drop(peers);
    });

    Ok(())
}

#[tokio::test]
async fn leave() -> enutt::Result<()> {
    config_tracing(Level::INFO);

    let expected_peers = 40;
    let mut peers = bootstrap_peers(expected_peers).await?;

    // give them few seconds to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(2)).await;

    // validate membership lists
    peers.iter().enumerate().for_each(|(_i, peer)| {
        let node = peer.context().node();
        let peers = node.peers().read();
        assert_eq!(expected_peers, peers.len());
        // TODO: Validate content of membership lists
        drop(peers);
    });

    let mut to_shutdown = peers.remove(0);

    to_shutdown.shutdown().await;

    // give them few secs to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;

    // validate membership lists
    peers.iter().enumerate().for_each(|(_i, peer)| {
        let node = peer.context().node();
        let peers = node.peers().read();
        assert_eq!(expected_peers - 1, peers.len());
        // TODO: Validate content of membership lists
        drop(peers);
    });

    Ok(())
}
