FROM debian:bookworm-slim
LABEL maintainer="tencent-cloud"

RUN apt update && apt install -y procps htop vim curl wget dnsutils net-tools iputils-ping netcat-openbsd sysstat openssh-client && apt clean

EXPOSE 4433

WORKDIR /root

ADD target/release/quic_server /root/quic_server
ADD resources /root/resources

ENTRYPOINT ["/root/quic_server"]
