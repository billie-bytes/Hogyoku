# Untuk menghindari akal-akalan Aizen dibuat dockernya agar jauh lebih performant
# Dibuat dua fase, dengan fase pertama ini hanya untuk mengcompile binary
FROM rust:latest AS builder
WORKDIR /app
COPY . .

# Buat app prod pake release mode biar debug symbols hilank (security) and binary teroptimisasi
RUN cargo build --release

# Ini fase kedua, dengan fase ini memikirkan tentang runtime environtmentnya 
FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y bash && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/server /usr/local/bin/server
COPY --from=builder /app/target/release/api_gateway /usr/local/bin/api_gateway

# And ini terakhir docker-entrypointnya dipake (sebelumnya ga dipake)
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Fix 1: Activate the runtime entrypoint so every Raft container resolves its
# ConfigMap-injected identity and addresses before starting the server.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["server"]

# How good is this compared to the previous docker?
#
# Sebelumnya tiap docker container punya ubuntu image (huge) and also 
# tiap container punya komponen builder, backend dan frontend. Kalo
# sekarang sudah dipisah builder, dengan backend dan frontend. Backend
# dan frontendnya masih digabung demi kubernetes caching, dan juga karena
# binary size dari keduanya ga sebesar itu.
