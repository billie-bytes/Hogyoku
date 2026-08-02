use std::net::SocketAddr;

// Fix 1: Add a Raft-local, actor-aware HTTP probe interface on the server
// binary only; this does not modify the API Gateway or its HTML.
use axum::{
    extract::State,
    http::StatusCode,
    routing::get,
    Router,
};
use clap::Parser;
use raft_core::domain::{Command, LogEntry};
use raft_core::error::NodeError;
use raft_core::rpc::{AppendEntriesReply, ApplyMembershipResponse, RaftService, RequestVoteReply, InstallSnapshotReply};
use futures::StreamExt;
use tarpc::server::{self, Channel};
use tarpc::{context, tokio_serde::formats::Json};
use tokio::sync::{mpsc, oneshot};
// Fix 1: Import the actor health message alongside existing Raft messages so
// probes verify the actor rather than merely checking an open TCP socket.
use raft_core::raft::actor::{ActorMsg, RaftActor};
use raft_core::raft::state::RaftState;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

use tarpc::client;
use raft_core::rpc::RaftServiceClient;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    ip: std::net::IpAddr,
    #[arg(long)]
    port: u16,
    #[arg(long)]
    id: u64,
    #[arg(long, default_value = "")]
    peers: String,
    // Fix 2: Require the ConfigMap-resolved address that peers and redirects
    // must use instead of reconstructing Kubernetes DNS inside RaftState.
    #[arg(long)]
    advertise_addr: String,
    #[arg(long)]
    contact_node_address: Option<String>,
    // Fix 1: Reserve a separate internal port for Kubernetes health probes so
    // the API Gateway and Raft RPC protocols remain unchanged.
    #[arg(long, default_value_t = 8081)]
    health_port: u16,
}

#[derive(Clone, Debug)]
struct RaftNodeHandle {
    tx: mpsc::Sender<ActorMsg>
}

// Fix 1: Share only the local actor channel with health routes; probes never
// execute replicated commands and therefore cannot grow the Raft log.
#[derive(Clone)]
struct HealthAppState {
    tx: mpsc::Sender<ActorMsg>,
}

// Fix 1: Treat the node as live only when the Raft actor answers within a
// bounded interval, which detects an unresponsive actor behind an open socket.
async fn health_live(State(state): State<HealthAppState>) -> StatusCode {
    let (reply_to, reply) = oneshot::channel();
    if state.tx.send(ActorMsg::Health { reply_to }).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    match tokio::time::timeout(Duration::from_secs(1), reply).await {
        Ok(Ok(_)) => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

// Fix 1: Mark the pod ready only after it wins leadership or hears from a
// current leader, exactly matching the cluster-connection readiness requirement.
async fn health_ready(State(state): State<HealthAppState>) -> StatusCode {
    let (reply_to, reply) = oneshot::channel();
    if state.tx.send(ActorMsg::Health { reply_to }).await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    match tokio::time::timeout(Duration::from_secs(1), reply).await {
        Ok(Ok(status)) if status.ready => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

impl RaftService for RaftNodeHandle {
    async fn execute(
        self, 
        _: context::Context, 
        cmd: Command
    ) -> Result<String, NodeError> {
        let (reply_to, rx) = oneshot::channel();

        let msg = ActorMsg::ClientRequest { 
            cmd, 
            reply_to 
        };

        if self.tx.send(msg).await.is_err() {
            return Err(NodeError::Internal("Actor is dead".into()));
        }

        rx.await.map_err(|_| NodeError::Internal("Actor did not reply".into()))?
    }

    async fn request_vote(
        self, 
        _: context::Context,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64
    ) -> RequestVoteReply {
        let (reply_to, rx) = oneshot::channel();

        let msg = ActorMsg::RequestVote { 
            term, 
            candidate_id, 
            last_log_index, 
            last_log_term, 
            reply_to 
        };

        let _ = self.tx.send(msg).await;

        rx.await.unwrap_or(RequestVoteReply { 
            term: 0, 
            vote_granted: false 
        })
    }

    async fn append_entries(
        self,
        _: context::Context,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry> ,
        leader_commit: u64
    ) -> AppendEntriesReply {
        let (reply_to, rx) = oneshot::channel();

        let msg = ActorMsg::AppendEntries { 
            term, 
            leader_id, 
            prev_log_index, 
            prev_log_term, 
            entries, 
            leader_commit, 
            reply_to 
        };

        let _ = self.tx.send(msg).await;

        rx.await.unwrap_or(AppendEntriesReply { 
            term: 0, 
            success: false 
        })
    }

    async fn install_snapshot(
        self,
        _: context::Context,
        term: u64,
        leader_id: u64,
        last_included_index : u64,
        last_included_term: u64,
        data: Vec<u8>,
        done: bool
    ) -> InstallSnapshotReply {
        let (reply_to, rx) = oneshot::channel();

        let msg = ActorMsg::InstallSnapshot {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            data,
            done,
            reply_to
        };

        let _ = self.tx.send(msg).await;

        rx.await.unwrap_or(InstallSnapshotReply {
            term: 0,
            success: false
        })
    }

    async fn request_log(self, _: context::Context) -> Vec<LogEntry> {
        let (reply_to, rx) = oneshot::channel();

        let msg = ActorMsg::RequestLog { reply_to };

        let _ = self.tx.send(msg).await;

        rx.await.unwrap_or_default()
    }

    async fn apply_membership(
        self,
        _: context::Context,
        node_id: u64,
        node_addr: String,
    ) -> Result<ApplyMembershipResponse, NodeError> {
        let (reply_to, rx) = oneshot::channel();

        let msg = ActorMsg::ApplyMembership {
            node_id,
            node_addr,
            reply_to,
        };

        if self.tx.send(msg).await.is_err() {
            return Err(NodeError::Internal("Actor is dead".into()));
        }

        rx.await.map_err(|_| NodeError::Internal("Actor did not reply".into()))?
    }

    async fn remove_membership(
        self,
        _: context::Context,
        node_id: u64,
    ) -> Result<(), NodeError> {
        let (reply_to, rx) = oneshot::channel();
    
        let msg = ActorMsg::RemoveMembership {
            node_id,
            reply_to,
        };
    
        if self.tx.send(msg).await.is_err() {
            return Err(NodeError::Internal("Actor is dead".into()));
        }
    
        rx.await.map_err(|_| NodeError::Internal("Actor did not reply".into()))?
    }
}

fn parse_peers(peers_str: &str) -> std::collections::HashMap<u64, String> {
    let mut peer_map = std::collections::HashMap::new();
    if peers_str.is_empty() {
        return peer_map;
    }
    
    for s in peers_str.split(',') {
        if let Some((id_str, addr)) = s.split_once('=') {
            if let Ok(id) = id_str.parse::<u64>() {
                peer_map.insert(id, addr.to_string());
            }
        }
    }
    peer_map
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // --- 1. Basic Setup ---
    let args = Args::parse();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive("tarpc=warn".parse()?);
    tracing_subscriber::fmt().with_env_filter(filter).init();
    tracing::info!("Node {} starting on port {}", args.id, args.port);

    // Channel for RPCs to talk to the Actor
    let (tx, rx) = mpsc::channel::<ActorMsg>(100);

    // --- 2. Start RPC Server Listener (in background) ---
    let server_addr = SocketAddr::new(args.ip, args.port);
    let mut listener = tarpc::serde_transport::tcp::listen(server_addr, Json::default).await?;
    listener.config_mut().max_frame_length(usize::MAX);
    
    let listener_tx = tx.clone();
    tokio::spawn(async move {
        tracing::info!("RPC Server listening on {}", server_addr);
        // Accept connections and spawn a handler for each one
        while let Some(accept_result) = listener.next().await {
            match accept_result {
                Ok(transport) => {
                    let handle = RaftNodeHandle { tx: listener_tx.clone() };
                    tokio::spawn(async move {
                        server::BaseChannel::with_defaults(transport)
                            .execute(handle.serve())
                            .for_each(|response_future| async move {
                                tokio::spawn(response_future);
                            })
                            .await; 
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to accept connection: {}", e);
                }
            }
        }
    });

    // Give the listener a moment to bind before we try to bootstrap.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- 3. Connect to Peers ---
    let peers_map = parse_peers(&args.peers);
    tracing::info!("Peers Config: {:?}", peers_map);
    let mut rpc_clients = std::collections::HashMap::new();
    for (id, addr_str) in &peers_map {
        // Fix 3: The local node already participates directly and must not get
        // an RPC client to itself, which could produce a duplicate self-vote.
        if *id == args.id {
            continue;
        }

        tracing::info!("Connecting to peer {} at {}...", id, addr_str);
        // Fix 4: Pass ConfigMap addresses directly to tarpc so Headless Service
        // DNS names work during initial peer discovery.
        if let Ok(transport) =
            tarpc::serde_transport::tcp::connect(addr_str.as_str(), Json::default).await
        {
            let client = RaftServiceClient::new(client::Config::default(), transport).spawn();
            rpc_clients.insert(*id, client);
            tracing::info!("Connected to peer {}", id);
        } else {
            tracing::warn!("Failed to connect to peer {} initially: {}", id, addr_str);
        }
    }

    // --- 4. Create Actor and Bootstrap (in foreground) ---
    // Fix 5: Use the ConfigMap-derived advertised address while preserving the
    // value-returning constructor API required by the supplied tests.
    let state = RaftState::new_with_addr(
        args.id,
        peers_map.clone(),
        args.advertise_addr,
    );

    // Fix 6: A fresh scaled pod is absent from initial membership, whereas an
    // initial or durably rejoined pod already contains its own ID.
    let needs_bootstrap = !state.peers.contains_key(&args.id);
    let mut actor = RaftActor::new(state, rx, tx.clone(), rpc_clients);

    // Fix 6: Bootstrap only a genuinely new member, avoiding unnecessary join
    // failure when an existing PVC-backed member restarts.
    if needs_bootstrap {
        let contact_node_address = args.contact_node_address.ok_or_else(|| {
            anyhow::anyhow!("A new node requires --contact-node-address")
        })?;
        tracing::info!("Bootstrapping to {}", contact_node_address);
        if let Err(e) = actor.bootstrap(contact_node_address).await {
            tracing::error!("Bootstrap failed: {}. Shutting down.", e);
            // Fix 6: Return bootstrap failure to the container runtime so
            // Kubernetes retries instead of treating an unjoined exit as success.
            return Err(e);
        }
        tracing::info!("Bootstrap successful.");
    }

    // Fix 1: Start the private health listener immediately before the actor loop;
    // both endpoints remain unavailable until the actor can process their messages.
    let health_addr = SocketAddr::new("0.0.0.0".parse()?, args.health_port);
    let health_state = HealthAppState { tx: tx.clone() };
    let health_app = Router::new()
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .with_state(health_state);
    let health_listener = tokio::net::TcpListener::bind(health_addr).await?;
    tokio::spawn(async move {
        tracing::info!("Health server listening on {}", health_addr);
        if let Err(error) = axum::serve(health_listener, health_app).await {
            tracing::error!("Health server stopped: {}", error);
        }
    });

    // --- 5. Run Actor's main loop (blocks forever) ---
    tracing::info!("Starting actor main loop.");
    actor.run().await;

    Ok(())
}