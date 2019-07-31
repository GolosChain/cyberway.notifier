ARG buildtype=stable
FROM cyberway/builder:$buildtype as builder

ARG branch=master
ARG compiletype=RelWithDebInfo

RUN cd /opt && wget https://github.com/nats-io/cnats/archive/v1.8.0.tar.gz && tar -xzf v1.8.0.tar.gz \
    && cd nats.c-1.8.0 && mkdir build && cd ./build && cmake -DCMAKE_BUILD_TYPE=$compiletype .. && make && make install

RUN echo "/opt/cnats-1.8.0/pbuf/lib/linux/" > /etc/ld.so.conf.d/protobuf-c.conf && ldconfig

ADD https://api.github.com/repos/cyberway/cyberway.notifier/git/refs/heads/$branch /etc/version.json

RUN cd /opt && git clone -b $branch https://github.com/cyberway/cyberway.notifier.git --recursive

RUN cd /opt/cyberway.notifier && mkdir build && cd build \
    && echo "$branch:$(git rev-parse HEAD)" > /etc/cyberway-notifier-version \
    && cmake \
        -DCMAKE_BUILD_TYPE=$compiletype \
        -DCMAKE_C_COMPILER=clang \
        -DCMAKE_CXX_COMPILER=clang++ \
        .. \
    && make

FROM ubuntu:18.04

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get -y install openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/cyberway.notifier/build/cyber-notifier /opt/cyberway/bin/cyber-notifier
COPY --from=builder /etc/version.json /opt/cyberway/cyber-notifier.version
COPY --from=builder /usr/local/lib/libnats.so.1.8.0 /usr/local/lib/
COPY --from=builder /opt/nats.c-1.8.0/pbuf/lib/linux/libprotobuf-c.so /usr/local/lib/

RUN ldconfig

VOLUME /queue
WORKDIR /opt

#ENTRYPOINT tail -f -n+0 /queue/msg.txt | /opt/cyberway-notifier/cyber-notifier
