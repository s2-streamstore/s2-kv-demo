# s2-kv-demo

Example REST KV-store built on [S2](https://s2.dev).

Each node keeps an in-memory copy of the store, built by replaying a log kept in an S2 stream. Writes are appended to the log. Reads are served from the node's local copy, either right away (`Eventual`) or after checking that the copy has caught up with the log (`Strong`). Any number of nodes can share a single stream.

This accompanies a post on the s2.dev blog -- see [here](https://s2.dev/blog/kv-store) for more context.

## Setup

### Install the S2 CLI

We'll use the [S2 CLI](https://github.com/s2-streamstore/s2) to create a basin and stream, and to inspect the log. It also includes `s2 lite`, which runs an S2-compatible server on your machine.

```bash
brew install s2-streamstore/s2/s2
```

See the [installation instructions](https://github.com/s2-streamstore/s2#installation) for other options (`cargo install --locked s2-cli`, an install script, etc.).

### Pick a backend

You can run the demo entirely on your machine with `s2 lite` (no account needed), or against [s2.dev](https://s2.dev).

<details open>
<summary><strong>Option A: run locally with <code>s2 lite</code></strong></summary>

Start a local S2 server in its own terminal:

```bash
s2 lite --port 8080
```

By default it keeps everything in memory, so data is lost when it stops. To keep data across restarts (for example to try recovery), give it a directory: `s2 lite --port 8080 --local-root ./s2-data`.

In every other terminal you use for the demo, point both the CLI and the KV nodes at it:

```bash
export S2_ACCOUNT_ENDPOINT="http://localhost:8080"
export S2_BASIN_ENDPOINT="http://localhost:8080"
export S2_ACCESS_TOKEN="ignored"
```

`s2 lite` doesn't check the token, but the KV nodes require `S2_ACCESS_TOKEN` to be set.

</details>

<details>
<summary><strong>Option B: use s2.dev</strong></summary>

- Get an [S2 account](https://s2.dev/), if you don't have one already.
- Set up the CLI by following the [quickstart](https://s2.dev/docs/quickstart).
- The KV nodes read your auth token from the `S2_ACCESS_TOKEN` env var, so export it in every terminal you use for the demo. Use an existing token, or generate a new one from the [dashboard](https://s2.dev/dashboard).

  ```bash
  export S2_ACCESS_TOKEN="<authentication token>"
  ```

</details>

### Create a basin and stream

Export some relevant variables in a few terminals. Three should be sufficient (two for running KV-store nodes, one for sending `curl` commands to those nodes).

```bash
export MY_BASIN="my-kv-store-1"
export MY_STREAM="l1"
export KV_NODE_1="localhost:4001"
export KV_NODE_2="localhost:4002"
export RUST_LOG=info,s2kv=trace,tower=debug
```

Create a basin (or use an existing one), and the stream that will hold the log:

```bash
s2 create-basin "${MY_BASIN}"

s2 create-stream "s2://${MY_BASIN}/${MY_STREAM}" \
  --storage-class standard \
  --retention-policy 7d
```

> [!NOTE]
> Data will start to get trimmed in 7 days, which can break recovery.
> Extend the `--retention-policy` if needed... or implement snapshotting :D

### Start a node

In one of your terminals, start a node:

```bash
cargo run --release -- \
  --host "${KV_NODE_1}" \
  "${MY_BASIN}" \
  "${MY_STREAM}"
```

On startup, a node replays the log from the beginning to rebuild its state.

If S2 becomes unreachable, the node keeps running. `Eventual` reads keep working, and the tailing reader reconnects and resumes on its own. Once the SDK's retries are exhausted, `Strong` reads fail with a `503`, and so do writes that definitely never reached the log (safe to retry). A write that may or may not have reached the log (for example, if the connection dropped after it was sent) fails with a `500` saying its outcome is unknown. Requests S2 would reject, such as a value over the 1 MiB record limit, fail with a `400`.

## Using the store

### Data types

All keys are strings. Values can be any of the following:

```rust
enum Value {
    Bool(bool),
    Float(OrderedFloat<f64>),
    Int(i64),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
    Set(BTreeSet<Value>),
    Str(String),
    UInt(u64),
}
```

### Put

Try writing some values.

```bash
curl \
  -H 'Content-Type: application/json' \
  -X PUT \
  -d '{"key": "hello world", "value": {"Str": "world"}}' \
  "${KV_NODE_1}/api"

curl \
  -H 'Content-Type: application/json' \
  -X PUT \
  -d '{"key": "s2", "value": {"Set": [{"Str": "is really cool"}, {"Str": "is really cool"}, {"UInt": 1337}]}}' \
  "${KV_NODE_1}/api"

curl \
  -H 'Content-Type: application/json' \
  -X PUT \
  -d '{"key": "map-sample", "value": {"Map": {"k1": {"Str": "hello"}}}}' \
  "${KV_NODE_1}/api"
```

A put returns as soon as S2 has durably appended it to the log, without waiting for the node to apply it to its local state. The response includes the log position the write ends at, e.g.:

```json
{"Ok":{"end":1}}
```

### Get

Reads require a read consistency to be specified in the URL: either `/api/Strong` or `/api/Eventual`.

Eventually consistent reads will return a value immediately from the current materialized state.

Strongly consistent reads will perform a `check_tail` operation to find the current tail of the log, and wait to verify that the materialized state has caught up to that value. A strong read issues `check_tail` immediately unless one is already in flight, in which case it waits for the next one along with everyone else who arrived in the meantime (the "bus stand" optimization described in [this paper](https://maheshba.bitbucket.io/papers/osr2024.pdf)). So an idle node's strong reads cost one `check_tail` round trip, and a busy node's strong reads share them.

```bash
# SC read
curl -X GET -G --data-urlencode \
  "key=hello world" \
  "${KV_NODE_1}/api/Strong"

# EC read
curl -X GET -G --data-urlencode \
  "key=hello world" \
  "${KV_NODE_1}/api/Eventual"
```

The response includes the log position the node's state reflects, along with the value:

```json
{"Ok":[{"end":3},{"Str":"world"}]}
```

A missing key returns a `404` with `{"Missing":{"end":3}}`.

### Delete

Delete a key:

```bash
curl -X DELETE -G --data-urlencode \
  "key=hello world" \
  "${KV_NODE_1}/api"
```

### Exploring consistency

It can be difficult to find instances where eventual consistency reads actually differ from strong reads. For sake of experimentation, nodes can be started with an optional delay parameter, which applies a "throttle" to the rate at which log entries are tailed from S2.

Try starting a second node, in a new terminal, which sets a throttle value.

```bash
cargo run --release -- \
  --host "${KV_NODE_2}" \
  "${MY_BASIN}" \
  "${MY_STREAM}" \
  --throttle-tailing-reader "2s"
```

This node will take a little while to start serving up-to-date values, since it replays the existing log at the throttled rate too.

Then, make a series of concurrent (note the use of the `-P` flag in `xargs`) `put` requests on the same key:

```bash
seq 20 \
  | xargs -I {} -P 20 \
    curl \
      --silent -H 'Content-Type: application/json' \
      -X PUT \
      -d '{"key": "hello", "value": {"UInt": '{}'}}' \
      "${KV_NODE_1}/api"
```

(Note that, since these 20 puts are happening concurrently, there is no guarantee about the order in which they will be applied to the KV-store, and inspecting the log will show an arbitrary ordering of these puts.)

The puts should all finish quickly.

Assuming `KV_NODE_2` was started with a tailing reader throttle, any `Eventual` read against it, for ~40 seconds after invocation of the above, should show a fluctuating value for the key `hello`.

```bash
# EC, so should return right away but with any value 1..=20
curl -X GET -G --data-urlencode \
  "key=hello" \
  "${KV_NODE_2}/api/Eventual"
```

Similarly, for the ~40 seconds while `KV_NODE_2` is catching up and applying the log to its internal state, any `Strong` read against it should hang until the log has caught up with the tail at the time the `get` is processed.

```bash
# SC, so will block until it can return the value where applied_state == the tail of the log when the request was received
curl -X GET -G --data-urlencode \
  "key=hello" \
  "${KV_NODE_2}/api/Strong"
```

### Inspecting the log

The log entries are JSON, to make inspection easier.

```bash
s2 read "s2://${MY_BASIN}/${MY_STREAM}" --seq-num 0 --count 50
```

... should return something similar to:

```json
{"Put":{"key":"hello world","value":{"Str":"world"}}}
{"Put":{"key":"s2","value":{"Set":[{"Str":"is really cool"},{"UInt":1337}]}}}
{"Put":{"key":"map-sample","value":{"Map":{"k1":{"Str":"hello"}}}}}
{"Delete":{"key":"hello world"}}
{"Put":{"key":"hello","value":{"UInt":2}}}
{"Put":{"key":"hello","value":{"UInt":1}}}
```

Leave off `--count` to keep following the stream as new entries are appended.
