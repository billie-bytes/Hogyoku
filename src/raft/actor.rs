use rand::{Rng, rng};
use tarpc::context;
use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_blocking;
use tokio::time::{Interval, MissedTickBehavior, interval};
use tracing::{debug, info, warn, error};

use crate::utils::client::execute_with_redirect;
use crate::domain::{Command, LogEntry, Role, Snapshot};
use crate::error::NodeError;
use crate::rpc::{AppendEntriesReply, ApplyMembershipResponse, RequestVoteReply, RaftServiceClient, InstallSnapshotReply};

use super::state::RaftState;
use std::cmp::min;
use std::time::Duration;
use std::collections::{HashMap, BTreeMap};
// Fix 21: Remove numeric-only SocketAddr usage because every peer connection
// now resolves the ConfigMap-provided Kubernetes DNS string directly.
use crate::raft::machine::StateMachine;
use tarpc::{client, tokio_serde::formats::Json};

// consts for time (in ms)
const ELECTION_TIMEOUT_MIN: u64 = 1500;
const ELECTION_TIMEOUT_MAX: u64 = 3000;
const HEARTBEAT_INTERVAL: u64 = 500;
const RAFT_LOG_SIZE_LIMIT: u64 = 10;

// Fix 1: Expose actor-owned health state without involving the API Gateway,
// HTML, or replicated commands, so Kubernetes probes cannot grow the Raft log.
#[derive(Debug, Clone, Copy)]
pub struct HealthStatus {
    pub ready: bool,
}

// Pesan yang bisa dikirim ke Actor
pub enum ActorMsg {
    ClientRequest { 
        cmd: Command, 
        reply_to: oneshot::Sender<Result<String, NodeError>> 
    },
    RequestLog {
        reply_to: oneshot::Sender<Vec<LogEntry>>
    },
    // Membership Change Message
    ApplyMembership {
        node_id: u64,
        node_addr: String,
        reply_to: oneshot::Sender<Result<ApplyMembershipResponse, NodeError>>,
    },
    RemoveMembership {
        node_id: u64,
        reply_to: oneshot::Sender<Result<(), NodeError>>,
    },
    UpdatePeerClient { 
        node_id: u64,
        client: RaftServiceClient,
    },
    PeerDisconnected {
        peer_id: u64,
    },
    // Pesan Internal Raft (RPC masuk di-convert jadi pesan ini)
    RequestVote { 
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
        reply_to: oneshot::Sender<RequestVoteReply>
    },
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        reply_to: oneshot::Sender<AppendEntriesReply>
    },
    AppendEntriesResult {
        peer_id: u64,
        term: u64,
        success: bool,
        last_log_index: u64, 
    },
    InstallSnapshot {
        term: u64,
        leader_id: u64,
        last_included_index : u64,
        last_included_term: u64,
        data: Vec<u8>,
        done: bool,
        reply_to: oneshot::Sender<InstallSnapshotReply>
    },
    InstallSnapshotReply {
        term: u64,
        success: bool
    },
    TriggerSnapshot,
    // Fix 1: Let the local Raft server verify actor responsiveness and cluster
    // readiness through a non-mutating message with a bounded reply timeout.
    Health {
        reply_to: oneshot::Sender<HealthStatus>,
    },
}

pub struct RaftActor {
    state: RaftState,
    inbox: mpsc::Receiver<ActorMsg>,
    msg_sender: mpsc::Sender<ActorMsg>,
    peers: HashMap<u64, RaftServiceClient>,
    state_machine: StateMachine,
    // Mapping: Log Index -> Channel untuk balas ke Client
    pending_requests: BTreeMap<u64, oneshot::Sender<Result<String, NodeError>>>,
}

impl RaftActor {
    // Fix 2: Keep the constructor signature used by the supplied tests while
    // reconciling any completed snapshot with the separately persisted HardState.
    pub fn new(
        mut state: RaftState,
        inbox: mpsc::Receiver<ActorMsg>,
        msg_sender: mpsc::Sender<ActorMsg>,
        peers: HashMap<u64, RaftServiceClient>,
    ) -> Self {
        let mut state_machine = StateMachine::new();
        let snapshot_path = format!("snapshot_{}.json", state.my_id);

        // Fix 2: Treat snapshot and HardState as one recoverable state. A newer
        // atomic snapshot is adopted after a crash between the two replacements;
        // an older or conflicting snapshot cannot safely reconstruct compacted data.
        match std::fs::read_to_string(&snapshot_path) {
            Ok(content) => {
                let snapshot = serde_json::from_str::<Snapshot>(&content)
                    .unwrap_or_else(|error| {
                        panic!("Failed to deserialize required snapshot {}: {}", snapshot_path, error)
                    });

                if snapshot.last_included_index < state.last_included_index {
                    panic!(
                        "Snapshot {} is older than HardState ({} < {})",
                        snapshot_path,
                        snapshot.last_included_index,
                        state.last_included_index
                    );
                }

                if snapshot.last_included_index == state.last_included_index
                    && snapshot.last_included_term != state.last_included_term
                {
                    panic!(
                        "Snapshot {} conflicts with HardState term at index {}",
                        snapshot_path,
                        snapshot.last_included_index
                    );
                }

                if snapshot.last_included_index > state.last_included_index {
                    let mut retained_log: Vec<LogEntry> = state
                        .log
                        .iter()
                        .filter(|entry| entry.index > snapshot.last_included_index)
                        .cloned()
                        .collect();
                    retained_log.insert(
                        0,
                        LogEntry {
                            term: snapshot.last_included_term,
                            index: snapshot.last_included_index,
                            command: Command::Ping,
                        },
                    );

                    state.log = retained_log;
                    state.last_included_index = snapshot.last_included_index;
                    state.last_included_term = snapshot.last_included_term;
                    state.commit_index = state.commit_index.max(snapshot.last_included_index);
                    state.last_applied = state.last_applied.max(snapshot.last_included_index);

                    RaftState::save_hs_to_disk(state.get_hs(), state.my_id)
                        .unwrap_or_else(|error| {
                            panic!("Failed to reconcile snapshot with HardState: {}", error)
                        });
                }

                info!(
                    "Loaded snapshot from {} (last_included_index={})",
                    snapshot_path,
                    snapshot.last_included_index
                );
                state_machine.data = snapshot.data;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if state.last_included_index > 0 {
                    panic!(
                        "HardState requires snapshot index {}, but {} is missing",
                        state.last_included_index,
                        snapshot_path
                    );
                }
                info!("No snapshot found at {}, starting with empty state machine.", snapshot_path);
            }
            Err(error) => {
                panic!("Failed to read snapshot {}: {}", snapshot_path, error);
            }
        }

        Self { state, inbox, msg_sender, peers, state_machine, pending_requests: BTreeMap::new() }
    }

    // Fix 3: Atomically replace the snapshot file before publishing matching
    // HardState, allowing startup reconciliation after interruption at either step.
    async fn save_snapshot_to_disk(node_id: u64, data: Vec<u8>) -> Result<(), NodeError> {
        spawn_blocking(move || {
            let path = format!("snapshot_{}.json", node_id);
            let tmp_path = format!("{}.tmp", path);
            std::fs::write(&tmp_path, data)
                .map_err(|error| NodeError::Internal(format!("Snapshot write error: {}", error)))?;
            std::fs::rename(&tmp_path, &path)
                .map_err(|error| NodeError::Internal(format!("Snapshot rename error: {}", error)))?;
            Ok::<(), NodeError>(())
        })
        .await
        .map_err(|error| NodeError::Internal(format!("Snapshot task join error: {}", error)))?
    }

    fn random_election_timeout() -> Duration {
        let ms = rng().random_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX);
        Duration::from_millis(ms)
    }

    async fn persist_state(&self) -> Result<(), NodeError> {
        let hs = self.state.get_hs();
        let id = self.state.my_id;

        spawn_blocking(move || {
            RaftState::save_hs_to_disk(hs, id)
        })
        .await
        .map_err(|e| NodeError::Internal(format!("Join error: {}", e)))??;

        Ok(())
    }

    pub async fn run(mut self) {
        let mut election_timer = interval(Self::random_election_timeout());
        election_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Fix 4: Tokio intervals tick immediately once; reset the election timer
        // so all pods wait for their randomized timeout instead of campaigning together.
        election_timer.reset();
        let mut heartbeat_timer = interval(Duration::from_millis(HEARTBEAT_INTERVAL));
        heartbeat_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!("RaftNode {} started as Follower", self.state.my_id);

        loop {
            tokio::select! {
                // 1. Handle Pesan Masuk
                Some(msg) = self.inbox.recv() => {
                    self.handle_message(msg, &mut election_timer).await;
                }

                // 2. Election Timeout (Jika Follower/Candidate)
                _ = election_timer.tick() => {
                    if self.state.role != Role::Leader {
                        warn!("Election timeout reached! Starting election...");
                        self.start_election().await;

                        election_timer = interval(Self::random_election_timeout());
                        election_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        election_timer.reset();
                    }
                }

                // 3. Heartbeat (Jika Leader)
                _ = heartbeat_timer.tick() => {
                    if self.state.role == Role::Leader {
                        self.send_heartbeats().await;
                    }
                }
            }
        }
    }

    /* Message Handlers */
    async fn handle_message(
        &mut self,
        msg: ActorMsg,
        election_timer: &mut Interval
    ) {
        match msg {
            ActorMsg::ClientRequest { cmd, reply_to } => {
                self.handle_client_request(cmd, reply_to).await;
            }
            ActorMsg::RequestLog { reply_to } => {
                let _ = reply_to.send(self.state.log.clone());
            }
            ActorMsg::ApplyMembership { node_id, node_addr, reply_to } => {
                self.handle_apply_membership(node_id, node_addr, reply_to).await;
            }
            ActorMsg::RemoveMembership { node_id, reply_to } => {
                self.handle_remove_membership(node_id, reply_to).await;
            }
            ActorMsg::UpdatePeerClient { node_id, client } => {
                self.peers.insert(node_id, client);
                info!("Successfully connected to peer {}", node_id);
            }
            ActorMsg::PeerDisconnected { peer_id } => {
                if self.peers.remove(&peer_id).is_some() {
                    warn!("Removed dead client for peer {}", peer_id);
                }
            }
            ActorMsg::RequestVote { term, candidate_id, last_log_index, last_log_term, reply_to } => {
                self.handle_request_vote(term, candidate_id, last_log_index, last_log_term, reply_to, election_timer).await;
            }
            ActorMsg::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit, reply_to } => {
                self.handle_append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit, reply_to, election_timer).await;
            }
            ActorMsg::AppendEntriesResult { peer_id, term, success, last_log_index } => {
                // Fix 19: Persist a higher replication-response term before
                // stepping down so restart cannot resume leadership in an old term.
                if term > self.state.current_term {
                    self.state.become_follower(term);
                    self.persist_state().await.unwrap_or_else(|error| {
                        panic!("Failed to persist higher replication term: {}", error)
                    });
                    return;
                }

                // Fix 20: Ignore replies from older leader tasks or replies that
                // arrive after this actor has already stepped down.
                if term < self.state.current_term || self.state.role != Role::Leader {
                    return;
                }
                
                if success {
                    self.state.update_match_index(peer_id, last_log_index);
                    self.state.update_next_index(peer_id, last_log_index + 1);
                    if let Some(new_commit) = self.state.advance_commit_index() {
                        self.apply_committed_entries().await;
                        info!("Commit index advanced to {}", new_commit);
                    }
                } else {
                    let current_next = *self.state.next_index.get(&peer_id).unwrap_or(&1);
                    let new_next = if current_next > 1 { current_next - 1 } else { 1 };
                    self.state.update_next_index(peer_id, new_next);
                }
            }
            ActorMsg::InstallSnapshot { term, leader_id, last_included_index, last_included_term, data, done, reply_to } => {
                self.handle_install_snapshot(term, leader_id, last_included_index, last_included_term, data, done, reply_to, election_timer).await;
            }
            ActorMsg::InstallSnapshotReply { .. } => {
                // Currently handled via direct async call in send_heartbeats logic or could be expanded.
            }
            ActorMsg::TriggerSnapshot => {
                self.handle_trigger_snapshot().await;
            }
            // Fix 1: Report readiness from actor-owned Raft state. A leader has
            // won a quorum, while a ready follower has heard from a current leader.
            ActorMsg::Health { reply_to } => {
                let ready =
                    self.state.role == Role::Leader || self.state.current_leader.is_some();
                let _ = reply_to.send(HealthStatus { ready });
            }
        }
    }

    async fn handle_trigger_snapshot(&mut self) {
        let last_applied = self.state.last_applied;
        
        // Cannot snapshot if applied index hasn't moved past existing snapshot
        if last_applied <= self.state.last_included_index {
            return;
        }

        info!("Triggering snapshot at index {}", last_applied);

        // 1. Capture State Machine
        let snapshot_data = self.state_machine.data.clone();
        
        // 2. Get last included term
        let last_term = self.state.get_log_term(last_applied);

        let snapshot = Snapshot {
            last_included_index: last_applied,
            last_included_term: last_term,
            data: snapshot_data
        };

        // Fix 5: Serialize and atomically persist the complete committed snapshot
        // before compacting its represented log prefix.
        let node_id = self.state.my_id;
        let snapshot_bytes = match serde_json::to_vec_pretty(&snapshot) {
            Ok(bytes) => bytes,
            Err(error) => {
                error!("Failed to serialize snapshot: {}", error);
                return;
            }
        };

        // Fix 5: Leave the existing log untouched when the new snapshot cannot
        // be stored, preserving the only recoverable copy of committed commands.
        if Self::save_snapshot_to_disk(node_id, snapshot_bytes)
            .await
            .is_ok()
        {
            info!("Snapshot saved successfully.");
            
            // 4. Truncate Log in Memory
            // We need to keep the entry at last_applied as the new dummy entry (index 0)
            // Calculate how many to remove from front.
            // Current log: [last_included_index, ..., last_applied, ...]
            // New log: [last_applied, ...]
            
            let remove_count = (last_applied - self.state.last_included_index) as usize;
            
            // Safety check
            if remove_count < self.state.log.len() {
                self.state.log.drain(0..remove_count);
                // The new first element is now at `last_applied`
                self.state.log[0].term = last_term; 
                self.state.log[0].index = last_applied;
                self.state.log[0].command = Command::Ping; // Dummy command
                
                self.state.last_included_index = last_applied;
                self.state.last_included_term = last_term;
                
                // Fix 6: Fail closed if the compacted HardState cannot follow
                // the snapshot; restart reconciliation will safely adopt the snapshot.
                self.persist_state()
                    .await
                    .unwrap_or_else(|error| panic!("Failed to persist compacted HardState: {}", error));
                info!("Log truncated. New start index: {}", self.state.last_included_index);
            } else {
                error!("Log truncation error: remove_count {} >= log.len {}", remove_count, self.state.log.len());
            }

        } else {
            error!("Failed to save snapshot to disk.");
        }
    }

    async fn handle_install_snapshot(
        &mut self,
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: Vec<u8>,
        _done: bool, // Assuming full snapshot for now
        reply_to: oneshot::Sender<InstallSnapshotReply>,
        election_timer: &mut Interval,
    ) {
        if term < self.state.current_term {
            let _ = reply_to.send(InstallSnapshotReply { term: self.state.current_term, success: false });
            return;
        }

        // Fix 7: Persist a higher term before processing or rejecting the
        // snapshot body so restart cannot resurrect an obsolete Raft term.
        if term > self.state.current_term {
            self.state.become_follower(term);
            if let Err(error) = self.persist_state().await {
                error!("Failed to persist higher InstallSnapshot term: {}", error);
                let _ = reply_to.send(InstallSnapshotReply {
                    term: self.state.current_term,
                    success: false,
                });
                return;
            }
        } else if self.state.role != Role::Follower {
            self.state.become_follower(term);
        }
        
        self.state.current_leader = Some(leader_id);
        election_timer.reset();

        // Fix 8: Reject malformed or metadata-inconsistent snapshots before
        // replacing either the state machine or its durable snapshot.
        let snapshot = match serde_json::from_slice::<Snapshot>(&data) {
            Ok(snapshot)
                if snapshot.last_included_index == last_included_index
                    && snapshot.last_included_term == last_included_term =>
            {
                snapshot
            }
            Ok(_) => {
                warn!("InstallSnapshot metadata does not match its serialized body");
                let _ = reply_to.send(InstallSnapshotReply {
                    term: self.state.current_term,
                    success: false,
                });
                return;
            }
            Err(error) => {
                warn!("Failed to deserialize snapshot data: {}", error);
                let _ = reply_to.send(InstallSnapshotReply {
                    term: self.state.current_term,
                    success: false,
                });
                return;
            }
        };

        info!("Installing snapshot up to index {}", last_included_index);

        // Fix 9: Persist the received state-machine snapshot itself. HardState
        // alone contains only the compacted log metadata and cannot recover KV data.
        if let Err(error) = Self::save_snapshot_to_disk(self.state.my_id, data).await {
            error!("Failed to persist installed snapshot: {}", error);
            let _ = reply_to.send(InstallSnapshotReply {
                term: self.state.current_term,
                success: false,
            });
            return;
        }

        self.state_machine.data = snapshot.data;
        self.state.last_included_index = last_included_index;
        self.state.last_included_term = last_included_term;
        self.state.commit_index = last_included_index;
        self.state.last_applied = last_included_index;
        self.state.log = vec![LogEntry {
            term: last_included_term,
            index: last_included_index,
            command: Command::Ping,
        }];

        // Fix 10: Acknowledge InstallSnapshot only after matching HardState is
        // durable. On failure, stop immediately so startup reconciliation adopts
        // the already durable snapshot instead of continuing with split state.
        self.persist_state().await.unwrap_or_else(|error| {
            panic!("Failed to persist installed snapshot HardState: {}", error)
        });

        let _ = reply_to.send(InstallSnapshotReply {
            term: self.state.current_term,
            success: true,
        });
    }
    async fn handle_request_vote(
        &mut self,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
        reply_to: oneshot::Sender<RequestVoteReply>,
        election_timer: &mut Interval,
    ) {
        if term > self.state.current_term {
            info!("Received RequestVote with higher term ({}), updating term.", term);
            self.state.become_follower(term);
        }

        let mut vote_granted = false;

        if term >= self.state.current_term {
            let can_vote = self.state.voted_for.is_none() || self.state.voted_for == Some(candidate_id);
            let is_log_ok = self.state.is_log_up_to_date(last_log_index, last_log_term);

            if can_vote && is_log_ok {
                vote_granted = true;
                self.state.voted_for = Some(candidate_id);
                election_timer.reset();
                info!("Vote GRANTED for candidate {} at term {}", candidate_id, term);
            } else {
                debug!("Vote DENIED for candidate {}. Reason: voted_for={:?}, log_ok={}", candidate_id, self.state.voted_for, is_log_ok);
            }
        } else {
            debug!("Vote DENIED for candidate {}. Reason: Term too old ({} < {})", candidate_id, term, self.state.current_term);
        }

        // Fix 11: Never acknowledge a vote that was not durably recorded;
        // otherwise a pod restart could vote twice in the same term.
        if let Err(error) = self.persist_state().await {
            error!("Failed to persist RequestVote state: {}", error);
            vote_granted = false;
        }
        let _ = reply_to.send(RequestVoteReply { term: self.state.current_term, vote_granted });
    }

    async fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        reply_to: oneshot::Sender<AppendEntriesReply>,
        election_timer: &mut Interval
    ) {
        if term < self.state.current_term {
            debug!("Rejecting AppendEntries from {} (Term {} < {})", leader_id, term, self.state.current_term);
            let _ = reply_to.send(AppendEntriesReply { term: self.state.current_term, success: false });
            return;
        }

        if term > self.state.current_term || self.state.role != Role::Follower {
            info!("Recognized valid Leader {} at term {}. Becoming Follower.", leader_id, term);
            self.state.become_follower(term);
        }

        self.state.current_leader = Some(leader_id);
        election_timer.reset();

        // Fix 12: Keep the pre-RPC log so a failed durable write can reject the
        // append without leaving an acknowledged entry only in volatile memory.
        let previous_log = self.state.log.clone();
        let mut success = self.state.append_entries(prev_log_index, prev_log_term, entries);
        if let Err(error) = self.persist_state().await {
            error!("Failed to persist AppendEntries state: {}", error);
            self.state.log = previous_log;
            success = false;
        }

        if success {
            if leader_commit > self.state.commit_index {
                let last_new_entry_index = self.state.last_log_index();
                self.state.commit_index = min(leader_commit, last_new_entry_index);
                self.apply_committed_entries().await;
                debug!("Commit index updated to {}", self.state.commit_index);
            }
        }
        let _ = reply_to.send(AppendEntriesReply { term: self.state.current_term, success });
    }

    async fn apply_committed_entries(&mut self) {
        while self.state.last_applied < self.state.commit_index {
            self.state.last_applied += 1;
            let idx = self.state.last_applied;
            
            // Adjust index for log access
            let log_len = self.state.log.len();
            // Virtual index -> Physical index
            if idx < self.state.last_included_index {
                // Already snapshot/compacted? Should not happen if last_applied is correct
                continue;
            }
            let physical_idx = (idx - self.state.last_included_index) as usize;

            if physical_idx < log_len {
                // Fix 13: Apply committed membership commands to every Raft
                // node's live membership, not only to the accepting leader.
                let command = self.state.log[physical_idx].command.clone();
                let membership_changed = match &command {
                    Command::AddNode { id, address } => {
                        self.state.peers.insert(*id, address.clone()).as_ref()
                            != Some(address)
                    }
                    Command::RemoveNode { id } => self.state.peers.remove(id).is_some(),
                    _ => false,
                };

                let result = self.state_machine.apply(&command);
                info!("Applied log index {}: {:?} -> {}", idx, command, result);

                // Fix 13: Persist follower membership at the same committed log
                // index so a replacement pod cannot forget a completed AddNode.
                if membership_changed {
                    self.persist_state().await.unwrap_or_else(|error| {
                        panic!("Failed to persist committed membership: {}", error)
                    });
                }

                if let Some(sender) = self.pending_requests.remove(&idx) {
                    let _ = sender.send(Ok(result));
                }
            } else {
                warn!("Log missing at index {}", idx);
            }
        }

        // Trigger snapshot if log is too big
        if self.state.log.len() as u64 > RAFT_LOG_SIZE_LIMIT {
            let _ = self.msg_sender.send(ActorMsg::TriggerSnapshot).await;
        }
    }

    /* Internal Logic */
    // Fix 14: Reconnect configured DNS peers before an election so startup
    // ordering or an earlier disconnect cannot prevent a surviving quorum voting.
    async fn reconnect_missing_peers(&mut self) {
        let configured_peers = self.state.peers.clone();
        for (peer_id, peer_addr) in configured_peers {
            if peer_id == self.state.my_id || self.peers.contains_key(&peer_id) {
                continue;
            }

            let connection = tokio::time::timeout(
                Duration::from_millis(1000),
                tarpc::serde_transport::tcp::connect(peer_addr.as_str(), Json::default),
            )
            .await;

            if let Ok(Ok(transport)) = connection {
                let peer_client =
                    RaftServiceClient::new(client::Config::default(), transport).spawn();
                self.peers.insert(peer_id, peer_client);
                info!("Connected to peer {} before election", peer_id);
            }
        }
    }

    async fn start_election(&mut self) {
        self.state.become_candidate(); 
        // Fix 15: Persist the incremented term and self-vote before requesting
        // external votes so a restart cannot reuse the term or vote twice.
        if let Err(error) = self.persist_state().await {
            error!("Cannot start election because self-vote was not persisted: {}", error);
            self.state.become_follower(self.state.current_term);
            return;
        }

        // Fix 14: Refresh missing outbound clients while the candidate still
        // has time to satisfy the sub-ten-second leader-transition requirement.
        self.reconnect_missing_peers().await;

        let term = self.state.current_term;
        let my_id = self.state.my_id;
        let last_log_index = self.state.last_log_index();
        let last_log_term = self.state.last_log_term();
        info!("Election started for Term {}", term);

        let (tx, mut rx): (mpsc::Sender<(u64, RequestVoteReply)>, mpsc::Receiver<(u64, RequestVoteReply)>) = mpsc::channel(self.peers.len().max(1)); 

        for (peer_id, client) in &self.peers {
            // Fix 16: The candidate already counted its durable self-vote and
            // must never send RequestVote to its own RPC endpoint.
            if *peer_id == my_id {
                continue;
            }

            let client = client.clone();
            let tx_inner = tx.clone();
            let peer_id = *peer_id;
            
            tokio::spawn(async move {
                let mut context = tarpc::context::current();
                context.deadline = std::time::Instant::now() + Duration::from_millis(1000);
                let reply = client.request_vote(context, term, my_id, last_log_index, last_log_term).await;
                
                if let Ok(response) = reply {
                    let _ = tx_inner.send((peer_id, response)).await;
                } else {
                    warn!("Peer {} failed to vote", peer_id);
                }
            });
        }
        drop(tx); 

        let mut votes_received = 1;
        // Fix 17: Count the local member exactly once so quorum is two of three
        // and three of five whether the peer map includes self or not.
        let local_member_missing =
            if self.state.peers.contains_key(&self.state.my_id) { 0 } else { 1 };
        let cluster_size = self.state.peers.len() + local_member_missing;
        let majority = cluster_size / 2 + 1;

        // Fix 17: Permit the already-durable self-vote to elect a one-node test
        // cluster without waiting on an empty response channel.
        if votes_received >= majority {
            self.state.become_leader();
            self.send_heartbeats().await;
            return;
        }

        while let Some((peer_id, reply)) = rx.recv().await {
            if reply.term > term {
                warn!("Peer {} has higher term ({}). Stepping down.", peer_id, reply.term);
                self.state.become_follower(reply.term);
                // Fix 18: Persist the higher election term before returning to
                // follower operation so pod restart cannot revive an obsolete term.
                self.persist_state().await.unwrap_or_else(|error| {
                    panic!("Failed to persist higher election term: {}", error)
                });
                return;
            }

            if reply.vote_granted { 
                votes_received += 1;
                info!("Vote received from {}. Total: {}", peer_id, votes_received);
            };

            if votes_received >= majority {
                info!("Won election with {} votes! Becoming LEADER for Term {}", votes_received, term);
                self.state.become_leader();
                self.send_heartbeats().await;
                return;
            }
        }
        info!("Election finished without majority.");
    }

    async fn send_heartbeats(&mut self) {
        debug!("Sending heartbeats...");
        let current_peers = self.state.peers.clone();

        for (peer_id, peer_addr_str) in current_peers {
            if peer_id == self.state.my_id {
                continue;
            }
            let existing_client = self.peers.get(&peer_id).cloned();
            let term = self.state.current_term;
            let my_id = self.state.my_id;
            let leader_commit = self.state.commit_index;
            let next_index = *self.state.next_index.get(&peer_id).unwrap_or(&1);
            
            // Check if we need to send a snapshot
            if next_index <= self.state.last_included_index {
                // Send Snapshot
                let last_included_index = self.state.last_included_index;
                let last_included_term = self.state.last_included_term;
                let sender = self.msg_sender.clone();
                let snapshot_path = format!("snapshot_{}.json", my_id);

                tokio::spawn(async move {
	                    let client = match existing_client {
	                        Some(c) => c,
	                        None => {
	                            // Fix 21: Pass the Headless Service hostname
	                            // directly to tarpc instead of rejecting DNS as SocketAddr.
	                            match tokio::time::timeout(
	                                Duration::from_millis(HEARTBEAT_INTERVAL),
	                                tarpc::serde_transport::tcp::connect(
	                                    peer_addr_str.as_str(),
	                                    Json::default,
	                                ),
	                            )
	                            .await
	                            {
	                                Ok(Ok(transport)) => {
	                                    let new_client = RaftServiceClient::new(client::Config::default(), transport).spawn();
	                                    let _ = sender.send(ActorMsg::UpdatePeerClient { node_id: peer_id, client: new_client.clone() }).await;
	                                    new_client
	                                }
	                                _ => return,
	                            }
	                        }
	                    };

                    // Read snapshot from disk
                    let data = match tokio::fs::read(&snapshot_path).await {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Failed to read snapshot file {}: {}", snapshot_path, e);
                            return;
                        }
                    };

                    let mut context = tarpc::context::current();
                    context.deadline = std::time::Instant::now() + Duration::from_millis(2000); // Longer timeout for snapshot

                    match client.install_snapshot(context, term, my_id, last_included_index, last_included_term, data, true).await {
                        Ok(reply) => {
                             // Treat success like AppendEntries success: update indices
                             // We re-use AppendEntriesResult for simplicity to update state
                             let _ = sender.send(ActorMsg::AppendEntriesResult {
                                peer_id,
                                term: reply.term,
                                success: reply.success,
                                last_log_index: last_included_index, 
                            }).await;
                        }
                        Err(e) => {
                             warn!("InstallSnapshot RPC failed for peer {}: {}", peer_id, e);
                             let _ = sender.send(ActorMsg::PeerDisconnected { peer_id }).await;
                        }
                    }
                });

            } else {
                // Send AppendEntries (Existing Logic)
                let prev_log_index = next_index - 1;
                // Use virtual indexing helper
                let prev_log_term = self.state.get_log_term(prev_log_index);
                
                // Get entries: adjust for virtual indexing
                let entries = if next_index <= self.state.last_log_index() {
                    // Calculate start index in the physical log vector
                    let start_physical_idx = (next_index - self.state.last_included_index) as usize;
                    if start_physical_idx < self.state.log.len() {
                        self.state.log[start_physical_idx..].to_vec()
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                };

                let sender = self.msg_sender.clone();
                let last_idx_sent = prev_log_index + entries.len() as u64;

                tokio::spawn(async move {
	                    let client = match existing_client {
	                        Some(c) => c,
	                        None => {
	                            info!("[Heartbeat] Peer {} disconnected, attempting to reconnect to {}...", peer_id, peer_addr_str);
	                            // Fix 21: Reconnect AppendEntries through the
	                            // configured DNS string so pod replacement remains reachable.
	                            match tokio::time::timeout(
	                                Duration::from_millis(HEARTBEAT_INTERVAL),
	                                tarpc::serde_transport::tcp::connect(
	                                    peer_addr_str.as_str(),
	                                    Json::default,
	                                ),
	                            )
	                            .await
	                            {
	                                Ok(Ok(transport)) => {
	                                    let new_client = RaftServiceClient::new(client::Config::default(), transport).spawn();
	                                    info!("[Heartbeat] Successfully reconnected to peer {}", peer_id);
	                                    let _ = sender.send(ActorMsg::UpdatePeerClient { node_id: peer_id, client: new_client.clone() }).await;
	                                    new_client
	                                }
	                                _ => {
	                                    debug!("[Heartbeat] Failed to reconnect to peer {}", peer_id);
	                                    return;
	                                }
	                            }
	                        }
	                    };
                    
                    let mut context = tarpc::context::current();
                    context.deadline = std::time::Instant::now() + Duration::from_millis(1000);

                    match client.append_entries(context, term, my_id, prev_log_index, prev_log_term, entries, leader_commit).await {
                        Ok(resp) => {
                             let _ = sender.send(ActorMsg::AppendEntriesResult {
                                peer_id,
                                term: resp.term,
                                success: resp.success,
                                last_log_index: last_idx_sent,
                            }).await;
                        }
                        Err(_) => {
                            warn!("[Heartbeat] RPC failed for peer {}. Marking disconnected.", peer_id);
                            let _ = sender.send(ActorMsg::PeerDisconnected { peer_id }).await;
                        }
                    }
                });
            }
        }
    }

    async fn handle_client_request(&mut self, cmd: Command, reply_to: oneshot::Sender<Result<String, NodeError>>) {
        if self.state.role != Role::Leader {
            let leader_id = self.state.current_leader;
            let leader_addr = leader_id.and_then(|id| self.state.peers.get(&id).cloned());
            let _ = reply_to.send(Err(NodeError::NotLeader { leader_addr }));
            return;
        }

        let new_index = self.state.last_log_index() + 1;
        let term = self.state.current_term;
        let entry = LogEntry { term, index: new_index, command: cmd };
        self.state.log.push(entry);
        self.pending_requests.insert(new_index, reply_to);

        info!("Leader appended command to log index {}", new_index);
        // Fix 22: Do not replicate or acknowledge a leader entry that failed
        // durable storage; roll back its volatile log and pending response.
        if let Err(error) = self.persist_state().await {
            self.state.log.pop();
            if let Some(sender) = self.pending_requests.remove(&new_index) {
                let _ = sender.send(Err(error));
            }
            return;
        }
        self.send_heartbeats().await;

        // Try to advance commit index immediately (important for single-node clusters)
        if let Some(new_commit) = self.state.advance_commit_index() {
            self.apply_committed_entries().await;
            info!("Commit index advanced to {}", new_commit);
        }
    }

    async fn handle_apply_membership(
        &mut self,
        node_id: u64,
        node_addr: String,
        reply_to: oneshot::Sender<Result<ApplyMembershipResponse, NodeError>>,
    ) {
        if self.state.role != Role::Leader {
            let leader_id = self.state.current_leader;
            let leader_addr = leader_id.and_then(|id| self.state.peers.get(&id).cloned());
            let _ = reply_to.send(Err(NodeError::NotLeader { leader_addr }));
            return;
        }

        // When sending back the peer list, create a temporary map that includes the leader itself.
        let mut peers_with_leader = self.state.peers.clone();
        peers_with_leader.insert(self.state.my_id, self.state.my_addr.clone());

        if self.state.peers.contains_key(&node_id) || self.state.my_id == node_id {
            info!("Node {} is already a member of the cluster.", node_id);
            let response = ApplyMembershipResponse {
                peers: peers_with_leader,
            };
            let _ = reply_to.send(Ok(response));
            return;
        }
    
        info!("Adding new node {} at {} to cluster", node_id, node_addr);
        self.state.peers.insert(node_id, node_addr.clone());
        self.state.next_index.insert(node_id, self.state.last_log_index() + 1);
        self.state.match_index.insert(node_id, 0);
    
        let new_index = self.state.last_log_index() + 1;
        let entry = LogEntry {
            term: self.state.current_term,
            index: new_index,
            command: Command::AddNode { id: node_id, address: node_addr.clone() },
        };
        self.state.log.push(entry);
    
        // Fix 23: Roll back every volatile AddNode mutation when membership and
        // its log entry cannot be persisted as one recoverable HardState.
        if let Err(error) = self.persist_state().await {
            self.state.peers.remove(&node_id);
            self.state.next_index.remove(&node_id);
            self.state.match_index.remove(&node_id);
            self.state.log.pop();
            let _ = reply_to.send(Err(error));
            return;
        }
        
        // Fix 24: Connect to a joining pod through its ConfigMap-provided DNS
        // string instead of requiring a numeric SocketAddr.
        match tarpc::serde_transport::tcp::connect(node_addr.as_str(), Json::default).await {
            Ok(transport) => {
                let client = RaftServiceClient::new(client::Config::default(), transport).spawn();
                self.peers.insert(node_id, client);
                info!("Connected to new peer {}", node_id);
            }
            Err(e) => {
                warn!("Failed to connect to new peer {}: {}", node_id, e);
                let msg_sender = self.msg_sender.clone();
                // Fix 24: Preserve the owned DNS name for background retries
                // after the new StatefulSet pod becomes reachable.
                let retry_addr = node_addr.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        if let Ok(transport) = tarpc::serde_transport::tcp::connect(
                            retry_addr.as_str(),
                            Json::default,
                        )
                        .await
                        {
                            let client = RaftServiceClient::new(client::Config::default(), transport).spawn();
                            let _ = msg_sender.send(ActorMsg::UpdatePeerClient { node_id, client }).await;
                            break;
                        }
                    }
                });
            }
        }
    
        self.send_heartbeats().await;
        
        // Re-create the complete list to include the new node and the leader.
        let mut final_peers = self.state.peers.clone();
        final_peers.insert(self.state.my_id, self.state.my_addr.clone());
        
        let response = ApplyMembershipResponse {
            peers: final_peers,
        };
        let _ = reply_to.send(Ok(response));
    }

    async fn handle_remove_membership(
        &mut self,
        node_id: u64,
        reply_to: oneshot::Sender<Result<(), NodeError>>,
    ) {
        if self.state.role != Role::Leader {
            let leader_id = self.state.current_leader;
            let leader_addr = leader_id.and_then(|id| self.state.peers.get(&id).cloned());
            let _ = reply_to.send(Err(NodeError::NotLeader { leader_addr }));
            return;
        }

        if !self.state.peers.contains_key(&node_id) {
            let _ = reply_to.send(Err(NodeError::Internal("Node not found".into())));
            return;
        }

        info!("Removing node {} from cluster", node_id);
        // Fix 25: Retain removed membership and client values until the removal
        // log entry is durable, so failure can restore the complete prior state.
        let removed_addr = self.state.peers.remove(&node_id);
        let removed_next_index = self.state.next_index.remove(&node_id);
        let removed_match_index = self.state.match_index.remove(&node_id);
        let removed_client = self.peers.remove(&node_id);

        let new_index = self.state.last_log_index() + 1;
        let entry = LogEntry {
            term: self.state.current_term,
            index: new_index,
            command: Command::RemoveNode { id: node_id },
        };
        self.state.log.push(entry);

        // Fix 25: Acknowledge RemoveNode only after its membership mutation and
        // log entry are durable; otherwise restore every removed value.
        if let Err(error) = self.persist_state().await {
            if let Some(address) = removed_addr {
                self.state.peers.insert(node_id, address);
            }
            if let Some(index) = removed_next_index {
                self.state.next_index.insert(node_id, index);
            }
            if let Some(index) = removed_match_index {
                self.state.match_index.insert(node_id, index);
            }
            if let Some(client) = removed_client {
                self.peers.insert(node_id, client);
            }
            self.state.log.pop();
            let _ = reply_to.send(Err(error));
            return;
        }
        self.send_heartbeats().await;
        let _ = reply_to.send(Ok(()));
    }

    pub async fn bootstrap(&mut self, contact_node_address: String) -> anyhow::Result<()> {
        // Fix 26: Accept the injected contact address as DNS-capable text from
        // the start of bootstrap instead of parsing it into SocketAddr.
        const MAX_RETRIES: u32 = 3;

        let my_id = self.state.my_id;
        let my_addr_str = self.state.my_addr.clone();

        let rpc_call = |client: RaftServiceClient| {
            let my_addr_str = my_addr_str.clone();
            async move {
                client.apply_membership(context::current(), my_id, my_addr_str).await
            }
        };

        // Fix 26: Keep the bootstrap contact as a DNS-capable string so a new
        // StatefulSet pod can join through the Headless Service.
        match execute_with_redirect(contact_node_address, MAX_RETRIES, rpc_call).await {
            Ok(join_response) => {
                info!("Successfully joined the cluster. Updating peer list.");
                // Fix 27: Preserve the previous membership until the leader's
                // returned cluster configuration is durable on this new pod.
                let previous_peers = self.state.peers.clone();
                self.state.peers = join_response.peers;
                if let Err(error) = self.persist_state().await {
                    self.state.peers = previous_peers;
                    return Err(anyhow::anyhow!(
                        "Failed to persist bootstrap membership: {}",
                        error
                    ));
                }
                info!("Updated peer list from leader: {:?}", self.state.peers);
                Ok(())
            }
            Err(e) => {
                error!("Failed to bootstrap after multiple retries: {}", e);
                Err(e)
            }
        }
    }
}