Simulate packet loss https://github.com/quinn-rs/quinn/issues/675:

```tc qdisc add dev lo root netem delay 5ms 10ms 25% distribution normal loss 5% duplicate 5% reorder 40% 50%```