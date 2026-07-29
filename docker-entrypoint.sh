#!/bin/bash
set -e

# Fix 6: Dispatch the combined image's stateless API Gateway mode before
# requiring Raft-only ConfigMap values, without changing Gateway source or HTML.
MODE="${1:-server}"
if [ "$MODE" = "api_gateway" ]; then
    exec api_gateway
fi
if [ "$MODE" != "server" ]; then
    echo "Unsupported container mode: $MODE"
    exit 1
fi

# Fix 1: Require every peer-related runtime value from ConfigMap-backed
# environment variables so changing cluster configuration never rebuilds the image.
: "${NODE_MAP:?NODE_MAP environment variable must be injected through ConfigMap}"
: "${PEERS:?PEERS environment variable must be injected through ConfigMap}"
: "${BOOTSTRAP_ADDR:?BOOTSTRAP_ADDR environment variable must be injected through ConfigMap}"

# Fix 2: Resolve this pod's Raft ID and advertised DNS address from NODE_MAP.
# HOSTNAME only selects the record; the ConfigMap remains the source of the ID.
NODE_ID=""
ADVERTISED_ADDR=""
NORMALIZED_NODE_MAP="${NODE_MAP//[[:space:]]/}"
IFS=',' read -ra NODE_ENTRIES <<< "$NORMALIZED_NODE_MAP"
for NODE_ENTRY in "${NODE_ENTRIES[@]}"; do
    CANDIDATE_ID="${NODE_ENTRY%%=*}"
    CANDIDATE_ADDR="${NODE_ENTRY#*=}"
    CANDIDATE_HOST="${CANDIDATE_ADDR%%.*}"
    if [ "$CANDIDATE_HOST" = "$HOSTNAME" ]; then
        NODE_ID="$CANDIDATE_ID"
        ADVERTISED_ADDR="$CANDIDATE_ADDR"
        break
    fi
done

# Fix 2: Fail before starting Raft when this StatefulSet pod has no configured
# identity, because inventing an ordinal-based ID would violate the ConfigMap spec.
if [ -z "$NODE_ID" ] || [ -z "$ADVERTISED_ADDR" ]; then
    echo "No NODE_MAP entry matches StatefulSet hostname $HOSTNAME"
    exit 1
fi

# Fix 3: Derive the listening port from the injected advertised address instead
# of maintaining a second hardcoded port that could diverge from peer discovery.
PORT="${ADVERTISED_ADDR##*:}"
if ! [[ "$PORT" =~ ^[0-9]+$ ]]; then
    echo "Invalid advertised address in NODE_MAP: $ADVERTISED_ADDR"
    exit 1
fi

# Fix 4: Normalize the injected initial membership 
# whitespace cannot become part of peer IDs or DNS addresses.
PEERS="${PEERS//[[:space:]]/}"
BOOTSTRAP_ADDR="${BOOTSTRAP_ADDR//[[:space:]]/}"

echo "Starting Raft Node $NODE_ID on port $PORT"
echo "Peers: $PEERS"

# Fix 5: Bind on all pod interfaces and pass only ConfigMap-derived identity,
# membership, advertised address, and bootstrap contact information.
exec server \
    --ip 0.0.0.0 \
    --id "$NODE_ID" \
    --port "$PORT" \
    --peers "$PEERS" \
    --advertise-addr "$ADVERTISED_ADDR" \
    --contact-node-address "$BOOTSTRAP_ADDR"