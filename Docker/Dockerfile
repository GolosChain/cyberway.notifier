FROM cyberway/builder:master

ARG branch=master

RUN cd /opt && wget https://github.com/nats-io/cnats/archive/v1.8.0.tar.gz && tar -xzf v1.8.0.tar.gz \
    && cd cnats-1.8.0 && mkdir build && cd ./build && cmake .. && make && make install

RUN echo "/opt/cnats-1.8.0/pbuf/lib/linux/" > /etc/ld.so.conf.d/protobuf-c.conf && ldconfig

ADD https://api.github.com/repos/GolosChain/cyberway.notifier/git/refs/heads/$branch /etc/version.json

RUN cd /opt && git clone -b $branch https://github.com/GolosChain/cyberway.notifier.git --recursive

RUN cd /opt/cyberway.notifier && mkdir build && cd build \
    && echo "$branch:$(git rev-parse HEAD)" > /etc/cyberway-notifier-version \
    && cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_COMPILER=clang \
        -DCMAKE_CXX_COMPILER=clang++ \
        .. \
    && make

VOLUME /queue
WORKDIR /opt/cyberway.notifier/

#ENTRYPOINT tail -f -n+0 /queue/msg.txt | /opt/cyberway-notifier/cyber-notifier
