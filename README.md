# cyberway.notifier
Service to broadcast cyberway events to receivers

## Requirements
* https://github.com/nats-io/cnats/releases/tag/v1.8.0

### prototype usage

0. Build
1. Start NATS Streaming with "cyberway" cluster_id: `nats-streaming-server -cid=cyberway &`
2. Start notifier, connected to input file: `tail -f -n+0 events.txt | path-to-binary/cyber-notifier &`
3. Connect clients to NATS Streaming, subscribe to topics, enable . Now we send AcceptTrx, ApplyTrx, CommitBlock, AcceptBlock
4. Start nodeos with `event-engine-plugin` enabled and event dumpfile set:
```
nodeos -e -p eosio \
--plugin eosio::event_engine_plugin \
--event-engine-dumpfile ./events.txt \
…other options…
```
