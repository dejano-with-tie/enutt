use std::collections::HashSet;
use std::time::Duration;

use futures::future::Pending;
use futures::StreamExt;
#[cfg(test)]
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use enutt::cluster::{Cluster, ClusterHandle};
use enutt::config::{Config, ConfigBuilder};
use enutt::membership::{Peer, PeerId};

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

async fn bootstrap_nodes(n: usize) -> enutt::Result<Vec<ClusterHandle>> {
    let start = 9011;
    let end = start + (n - 1);
    let mut nodes = Vec::with_capacity(n);

    nodes.push(bootstrap_node(ConfigBuilder::default().finish()?).await?);

    // run peers
    for peer_port in start..end {
        nodes.push(
            bootstrap_node(
                ConfigBuilder::default()
                    .server_port(peer_port as u16)
                    .client_port((peer_port as u16) + 2000)
                    .name(peer_port.to_string())
                    .finish()?,
            )
            .await?,
        );
        // give it time for each peer so we don't over flood the cluster
        // tokio::time::delay_for(tokio::time::Duration::from_secs(1)).await;
    }

    Ok(nodes)
}

async fn bootstrap_node(config: Config) -> enutt::Result<ClusterHandle> {
    enutt::cluster::run(config).await
}

#[tokio::test]
async fn join() -> enutt::Result<()> {
    let booting_peer = bootstrap_node(ConfigBuilder::default().finish()?).await?;
    let another_peer = bootstrap_node(
        ConfigBuilder::default()
            .server_port(8001)
            .client_port(10001)
            .name(8001.to_string())
            .finish()?,
    )
    .await?;

    // let bootstrap_node = bootstrap_node.context().membership();
    // let second = second.context().membership();

    let bootstrap_node_id = booting_peer.node().id();
    let another_peer_id = another_peer.node().id();

    // they know about each other
    assert!(booting_peer.peers().contains_key(&another_peer_id));
    assert!(another_peer.peers().contains_key(&bootstrap_node_id));
    assert_ne!(&another_peer_id, &bootstrap_node_id);
    Ok(())
}

/// 1. Union of all nodes peers should be equal to complete list of peers
/// 2. sleep few secs to give them time to converge
/// 3. validate each node has complete list of peers
#[tokio::test(core_threads = 4)]
async fn eventually_consistent_membership() -> enutt::Result<()> {
    config_tracing(Level::INFO);
    let expected_nodes = 30;
    let nodes = bootstrap_nodes(expected_nodes).await?;
    let mut cluster_members: HashSet<PeerId> = HashSet::new();

    // all members inside a cluster are known
    for node in &nodes {
        let peers = node.peers();
        let ids: Vec<&PeerId> = peers.keys().collect();
        cluster_members.extend(ids);
    }
    assert_eq!(nodes.len(), cluster_members.len());

    // give them few seconds to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(5)).await;

    // each node should know about all other members
    for node in &nodes {
        let peers = node.peers();
        cluster_members.iter().for_each(|peer_id| {
            assert!(peers.contains_key(peer_id));
        })
    }

    Ok(())
}

#[tokio::test]
async fn leave() -> enutt::Result<()> {
    // config_tracing(Level::INFO);

    let expected_peers = 10;
    let mut peers = bootstrap_nodes(expected_peers).await?;

    // give them few seconds to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(2)).await;

    // validate membership lists
    peers.iter().enumerate().for_each(|(_i, peer)| {
        assert_eq!(expected_peers, peer.peers_len());
    });

    peers.remove(0).leave(true).await?;
    peers.remove(1).leave(true).await?;
    peers.remove(2).leave(true).await?;

    // validate membership lists
    peers.iter().enumerate().for_each(|(_i, peer)| {
        assert_eq!(expected_peers - 3, peer.peers_len());
    });

    Ok(())
}

#[tokio::test]
async fn rejoin() -> enutt::Result<()> {
    // config_tracing(Level::INFO);

    let expected_peers = 10;
    let mut peers = bootstrap_nodes(expected_peers).await?;

    // give them few seconds to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(2)).await;

    // validate membership lists
    peers.iter().enumerate().for_each(|(_i, peer)| {
        assert_eq!(expected_peers, peer.peers_len());
    });

    peers.remove(0).leave(true).await?;

    // validate membership lists
    peers.iter().enumerate().for_each(|(_i, peer)| {
        assert_eq!(expected_peers - 1, peer.peers_len());
    });

    bootstrap_node(
        ConfigBuilder::default()
            .server_port(9000 as u16)
            .client_port((9000 as u16) + 2000)
            .name(9000.to_string())
            .finish()?,
    )
    .await?;

    // give them few seconds to gossip
    tokio::time::delay_for(tokio::time::Duration::from_secs(2)).await;

    // validate membership lists
    peers.iter().enumerate().for_each(|(_i, peer)| {
        assert_eq!(expected_peers, peer.peers_len());
    });

    Ok(())
}
