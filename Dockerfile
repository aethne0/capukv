FROM ubuntu

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates iproute2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY target/debug/capukv /usr/local/bin/capukv

ENTRYPOINT ["capukv"]
