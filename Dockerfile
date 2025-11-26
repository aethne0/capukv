# can reduce this later
FROM ubuntu:latest

COPY target/release/capukv /usr/local/bin/capukv
RUN chmod +x /usr/local/bin/capukv
RUN mkdir -p /data/capukv

ENTRYPOINT ["/usr/local/bin/capukv"]
