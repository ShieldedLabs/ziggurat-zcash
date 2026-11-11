# Fetches the release binary. zebra-chain builds too slowly on a small VPS.
FROM debian:trixie-slim
ARG RELEASE_URL
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -fsSL -o /usr/local/bin/mempool-listener "${RELEASE_URL}" \
    && chmod +x /usr/local/bin/mempool-listener \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
