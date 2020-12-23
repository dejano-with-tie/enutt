#[cfg(test)]
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use enutt::cluster::Cluster;
use enutt::config::{Config, ConfigBuilder};

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

    peers.push(bootstrap_peer(ConfigBuilder::default().finish()?).await?);

    // run peers
    for peer_port in start..end {
        peers
            .push(bootstrap_peer(ConfigBuilder::default().port(peer_port as u16).finish()?).await?);
        // give it time for each peer so we don't over flood the cluster
        // tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
    }

    Ok(peers)
}

async fn bootstrap_peer(config: Config) -> enutt::Result<Cluster> {
    let peer = Cluster::new(config).await?;
    peer.bootstrap().await?;
    Ok(peer)
}

#[tokio::test]
async fn join() -> enutt::Result<()> {
    let bootstrap_node = bootstrap_peer(ConfigBuilder::default().finish()?).await?;
    let bootstrap_node = bootstrap_node.context();
    let second = bootstrap_peer(ConfigBuilder::default().port(8082).finish()?).await?;
    let second = second.context();

    // let bootstrap_node = bootstrap_node.context().membership();
    // let second = second.context().membership();

    let bootstrap_node_id = bootstrap_node.node().id();
    let second_id = second.node().id();

    // they know about each other
    {
        assert!(bootstrap_node
            .membership()
            .peers()
            .read()
            .contains_key(&second_id));
    }
    {
        assert!(second
            .membership()
            .peers()
            .read()
            .contains_key(&bootstrap_node_id));
    }
    Ok(())
}

#[tokio::test(core_threads = 4)]
async fn eventually_consistent_membership() -> enutt::Result<()> {
    // config_tracing(Level::INFO);
    let expected_peers = 30;
    let peers = bootstrap_peers(expected_peers).await?;
    // TODO: Here I should test for eventual consistency:
    // 1. Union of all nodes peers should be equal to complete list of peers
    // 2. sleep few secs to give them time to converge
    // 3. validate each node has complete list of peers
    // give them few seconds to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(2)).await;

    // validate membership lists
    peers.into_iter().enumerate().for_each(|(_i, peer)| {
        assert_eq!(expected_peers, peer.context().membership().len());
        // TODO: Validate content of membership lists
    });

    Ok(())
}

#[tokio::test]
async fn leave() -> enutt::Result<()> {
    // config_tracing(Level::INFO);

    let expected_peers = 10;
    let mut peers = bootstrap_peers(expected_peers).await?;

    // give them few seconds to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(2)).await;

    // validate membership lists
    peers.iter().enumerate().for_each(|(_i, peer)| {
        assert_eq!(expected_peers, peer.context().membership().len());
        // TODO: Validate content of membership lists
    });

    let to_shutdown = peers.remove(0);

    to_shutdown.shutdown().await?;

    // give them few secs to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;

    // validate membership lists
    peers.iter().enumerate().for_each(|(_i, peer)| {
        assert_eq!(expected_peers - 1, peer.context().membership().len());
        // TODO: Validate content of membership lists
    });

    Ok(())
}
