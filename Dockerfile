#FROM debian:bookworm-slim
FROM alpine:latest

#RUN apk add curl ca-certificates

COPY target/release/capukv /usr/local/bin/capukv
RUN chmod +x /usr/local/bin/capukv
RUN mkdir -p /data/capukv

CMD ["capukv"]
