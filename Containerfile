FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates coreutils \
    && rm -rf /var/lib/apt/lists/*
COPY tperf /usr/local/bin/tperf
ENTRYPOINT ["/usr/local/bin/tperf"]
