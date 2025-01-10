# s2-kv-demo 

Example REST KV-store built on S2.

This accompanies a post on the s2.dev blog -- see [here](https://s2.dev/blog/kv-store) for more context.

## Demo 

### Pre-requisites

- Get an [S2 account](https://s2.dev/), if you don't have one already.

- We'll be using the [S2 CLI](https://github.com/s2-streamstore/s2-cli) as well, so make sure that is also [setup](https://s2.dev/docs/quickstart#get-started-with-the-cli).

- Finally, be sure to make your auth token accessible; the code in this repo will expect to read it from the `S2_AUTH_TOKEN` env var:

  ```bash
  export S2_AUTH_TOKEN="<authentication token>"
  ```

  Use an existing token, or generate a new one from [s2.dev](https://s2.dev/dashboard).

- You can use an existing basin, or create a new dedicated one for this example.

  ```bash
  s2 create-basin my-kv-store-1
  ```

### Getting started

Export some relevant variables in a few terminals. Three should be sufficient (two for runing KV-store nodes, one for sending `curl` commands to those nodes).

```bash
export MY_BASIN="my-kv-store-1"
export MY_STREAM="l1"
export KV_NODE_1="localhost:4001"
export KV_NODE_2="localhost:4002"
export RUST_LOG=info,s2kv=trace,tower=debug
```

Create the stream, if it doesn't already exist.

```bash
s2 create-stream "s2://${MY_BASIN}/${MY_STREAM}" \
  --storage-class standard \
  --retention-policy 7d
```

> [!NOTE]
> Data will start to get trimmed in 7 days, which can break recovery.
> Extend the `--retention-policy` if needed... or implement snapshotting :D

In one of your terminals, start a node. 

```bash
cargo run --release -- \
  --host "${KV_NODE_1}" \
  "${MY_BASIN}" \
  "${MY_STREAM}"
```

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

### Get 

Reads require a read consistency to be specified in the URL: either `/api/Strong/` or `/api/Eventual`. 

Eventually consistent reads will return a value immediately from the current materialized state.

Strongly consistent reads will perform a `check_tail` operation to find the current tail of the log, and wait to verify that the materialized state has caught up to that value.

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

### Delete 

Delete a key:

```bash
curl -X DELETE -G --data-urlencode \
  "key=hello" \
  "${KV_NODE_1}/api"
```

### Exploring consistency 

It can be difficult to find instances where eventual consistency reads actually differ from strong reads. For sake of experimentation, nodes can be started with optional delay parameters, which apply a "throttle" to the rate at which log entries are tailed from S2, or the rate at which append acknowledgments are received.

Try starting a second node, in a new terminal, which sets throttle values.
```bash
cargo run --release -- \
  --host "${KV_NODE_2}" \
  "${MY_BASIN}" \
  "${MY_STREAM}" \
  --throttle-tailing-reader "2s"
```

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
s2 read "s2://${MY_BASIN}/${MY_STREAM}"
```

... should return something similar to:

```json
{"Delete":{"key":"hello"}}
{"Put":{"key":"hello","value":{"UInt":2}}}
{"Put":{"key":"hello","value":{"UInt":1}}}
{"Put":{"key":"hello","value":{"UInt":3}}}
{"Put":{"key":"hello","value":{"UInt":4}}}
```
