use crate::rpc::RaftServiceClient;
use crate::error::NodeError;
// Fix 1: Remove the numeric-only SocketAddr dependency now that this shared
// helper accepts both CLI socket values and Kubernetes DNS strings.
use tarpc::{client, tokio_serde::formats::Json};

/// Executes a generic RPC call with a built-in redirection loop.
///
/// This helper function will attempt an RPC call on a given address. If the server
/// responds with a `NotLeader` error, it will automatically parse the new leader's
/// address and retry, up to a specified number of times.
// Fix 1: Accept any displayable address and immediately retain it as a string,
// allowing both CLI SocketAddr values and Kubernetes DNS names without caller edits.
pub async fn execute_with_redirect<F, T, Fut>(
    initial_addr: impl ToString,
    max_retries: u32,
    mut rpc_call: F,
) -> anyhow::Result<T>
where
    // The closure takes a client and returns a future.
    F: FnMut(RaftServiceClient) -> Fut,
    // The future resolves to the result of a tarpc RPC call.
    Fut: std::future::Future<Output = Result<Result<T, NodeError>, client::RpcError>>,
    T: Send + 'static,
{
    // Fix 1: Preserve the initial RAFT_ADDR as DNS-capable text instead of
    // forcing resolution into a numeric SocketAddr before connecting.
    let mut addr = initial_addr.to_string();
    let mut retries = max_retries;

    loop {
        if retries == 0 {
            anyhow::bail!("Redirect limit reached.");
        }
        retries -= 1;

        println!("Connecting to server on {}...", addr);
        // Fix 1: Let tarpc resolve the current DNS name on each retry so pod
        // replacement and Headless Service discovery remain effective.
        let transport =
            tarpc::serde_transport::tcp::connect(addr.as_str(), Json::default).await?;
        let client = RaftServiceClient::new(client::Config::default(), transport).spawn();

        match rpc_call(client).await {
            Ok(Ok(response)) => {
                // The RPC was successful and the server returned Ok(response).
                return Ok(response);
            }
            Ok(Err(NodeError::NotLeader { leader_addr })) => {
                // The server is a follower and told us who the leader is.
                if let Some(leader_str) = leader_addr {
                    println!("Not the leader. Redirecting to leader at {}...", leader_str);
                    // Fix 1: Follow the leader's advertised DNS name directly;
                    // parsing it as SocketAddr would reject valid StatefulSet hosts.
                    addr = leader_str;
                    continue;
                } else {
                    anyhow::bail!("Not the leader, and no leader address was provided.");
                }
            }
            Ok(Err(e)) => {
                // The RPC was successful, but the server returned a non-redirect error.
                anyhow::bail!("Application error from node: {}", e);
            }
            Err(e) => {
                // The RPC itself failed (network error, etc.).
                anyhow::bail!("RPC transport error: {}", e);
            }
        }
    }
}