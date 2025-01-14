use async_stream::stream;
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::routing::{delete, get, put};
use axum::{Json, Router};
use bytes::Bytes;
use clap::Parser;
use derivative::Derivative;
use eyre::eyre;
use futures::TryFutureExt;
use humantime::parse_duration;
use ordered_float::OrderedFloat;
use s2::client::{ClientConfig, ClientError, S2Endpoints, StreamClient};
use s2::types::{
    AppendInput, AppendRecord, AppendRecordBatch, BasinName, ConvertError, ReadOutput,
    ReadSessionRequest, SequencedRecord,
};
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::ops::{RangeFrom, RangeTo};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep_until, Instant};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, trace};

type SequenceNumber = u64;
type SequencedValue = (RangeTo<SequenceNumber>, Option<Value>);
type WriteSender = oneshot::Sender<Result<RangeTo<SequenceNumber>, KVError>>;
type ReadSender = oneshot::Sender<Result<SequencedValue, KVError>>;

const BUS_RIDER_MAX_WAIT: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
enum KVError {
    #[error("bus stand task failed")]
    BusStandTaskFailure,
    #[error("serde: {0}")]
    JsonError(String),
    #[error("orchestrator task failed")]
    OrchestratorTaskFailure,
    #[error("record convert: {0}")]
    RecordConvertError(#[from] ConvertError),
    #[error("s2 client: {0}")]
    S2ClientError(#[from] ClientError),
    #[error("{0}")]
    Weird(&'static str),
}

#[derive(Debug, Deserialize)]
enum ReadConsistency {
    /// Read may be serviced only from local materialized state, even if state has not
    /// caught up to the tail of the log at the time of the request.
    Eventual,
    /// Read must reflect all state up to the tail of the log at the time of the request.
    Strong,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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

/// Local materialized database state for a node.
struct LocalState {
    /// State reflects this portion of the log.
    applied_state: RangeTo<SequenceNumber>,
    /// Internal representation of the database, constructed from the log.
    storage: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum LogEntry {
    Put { key: String, value: Value },
    Delete { key: String },
}

impl From<LogEntry> for Bytes {
    fn from(value: LogEntry) -> Self {
        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &value).expect("json conversion to bytes");
        bytes::Bytes::from(bytes)
    }
}

struct BusRider {
    max_wait: Duration,
    response_tx: oneshot::Sender<Result<RangeTo<SequenceNumber>, KVError>>,
}

struct KVStore {
    orchestrator_cmd_tx: mpsc::UnboundedSender<OrchestratorCommand>,
    bus_tx: mpsc::UnboundedSender<BusRider>,
    _orchestrator_task: tokio::task::JoinHandle<Result<(), KVError>>,
    _bus_stand_task: tokio::task::JoinHandle<Result<(), KVError>>,
}

#[derive(Debug)]
enum ResponseContext {
    Read {
        key: String,
        response_tx: ReadSender,
    },
    #[cfg(test)]
    None,
}

#[derive(Debug)]
enum OrchestratorCommand {
    WriteLog {
        log: LogEntry,
        response_tx: WriteSender,
    },
    ReadStrongConsistency {
        key: String,
        reflect_applied_state: RangeTo<SequenceNumber>,
        response_tx: ReadSender,
    },
    ReadEventualConsistency {
        key: String,
        response_tx: ReadSender,
    },
}

/// Responses that cannot be fulfilled until the KVStore's internal `applied_to` state
/// matches a specified value.
#[derive(Default)]
struct PendingResponses(BinaryHeap<PendingResponse>);

impl PendingResponses {
    fn drain_applied_responses(
        &mut self,
        applied_state: RangeTo<SequenceNumber>,
    ) -> impl Iterator<Item = PendingResponse> + '_ {
        std::iter::from_fn(move || match self.0.peek() {
            Some(PendingResponse { end_seq_num, .. }) if end_seq_num.0 <= applied_state.end => {
                Some(self.0.pop().expect("queue entry"))
            }
            _ => None,
        })
    }

    fn submit(&mut self, pending: PendingResponse) {
        self.0.push(pending);
    }
}

#[derive(Derivative, Debug)]
#[derivative(Eq, Ord, PartialEq, PartialOrd)]
struct PendingResponse {
    end_seq_num: Reverse<SequenceNumber>,
    #[derivative(Ord = "ignore", PartialEq = "ignore", PartialOrd = "ignore")]
    response_context: ResponseContext,
}

impl PendingResponse {
    fn new(wait_for_exclusive: RangeTo<SequenceNumber>, response_context: ResponseContext) -> Self {
        Self {
            end_seq_num: Reverse(wait_for_exclusive.end),
            response_context,
        }
    }
}

impl KVStore {
    /// Implements the "bus stand" optimization from <https://maheshba.bitbucket.io/papers/osr2024.pdf>,
    /// allowing multiple readers to be serviced by the same underlying `check_tail` invocation.
    async fn bus_stand(
        client: StreamClient,
        cancellation_token: CancellationToken,
        mut inbox: mpsc::UnboundedReceiver<BusRider>,
    ) -> Result<(), KVError> {
        let _guard = cancellation_token.drop_guard();

        let sometime_later = Duration::from_secs(3600 * 24);
        let mut next_departure = Instant::now() + sometime_later;
        let mut queue = VecDeque::new();

        loop {
            tokio::select! {
                Some(BusRider { max_wait, response_tx }) = inbox.recv() => {
                    trace!("new bus rider");
                    let desired_departure = Instant::now() + max_wait;
                    if desired_departure < next_departure {
                        next_departure = desired_departure;
                    }
                    queue.push_back(response_tx);
                },
                _ = sleep_until(next_departure), if !queue.is_empty() => {
                    trace!(num_passengers=queue.len(), "next departure is leaving!");
                    let tail = client.check_tail().await?;
                    trace!(?tail);
                    for rider in queue.drain(..) {
                        _ = rider
                            .send(Ok(..tail))
                            .inspect_err(|_| debug!("SC caller disconnected before check_tail returned"));
                    }
                    next_departure = Instant::now() + sometime_later;
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn orchestrate(
        client: StreamClient,
        cancellation_token: CancellationToken,
        mut local_state: LocalState,
        mut command_rx: mpsc::UnboundedReceiver<OrchestratorCommand>,
        throttle: Throttle,
    ) -> Result<(), KVError> {
        let _guard = cancellation_token.drop_guard();

        // Writes to S2 that are "inflight", and have not yet received acknowledgment.
        let mut write_queue: VecDeque<WriteSender> = VecDeque::new();
        // Blocked responses for internal callers.
        let mut pending_responses = PendingResponses::default();

        // Start an append session.
        let (append_tx, append_rx) = mpsc::unbounded_channel();
        let append_acknowledgments = client
            .append_session(UnboundedReceiverStream::new(append_rx))
            .await?
            .throttle(throttle.throttle_append_acknowledgments);

        // Start a tailing read session.
        let tailing_reader = futures::StreamExt::flat_map(
            client
                .read_session(ReadSessionRequest {
                    start_seq_num: local_state.applied_state.end,
                    limit: None,
                })
                .await?,
            |read_output| {
                stream! {
                    let ReadOutput::Batch(batch) = read_output.map_err(KVError::from)? else {
                        Err(KVError::Weird("received non-batch while tailing"))?
                    };
                    for record in batch.records {
                        yield Ok::<SequencedRecord, KVError>(record)
                    }
                }
            },
        )
        .throttle(throttle.throttle_tailing_reader);

        tokio::pin!(append_acknowledgments);
        tokio::pin!(tailing_reader);

        loop {
            tokio::select! {
                Some(cmd) = command_rx.recv() => {
                    trace!(?cmd, "command received");
                    match cmd {
                        OrchestratorCommand::WriteLog { log, response_tx } => {
                            write_queue.push_back(response_tx);
                            append_tx.send(AppendInput {
                                records: AppendRecordBatch::try_from_iter(
                                    [AppendRecord::new(bytes::Bytes::from(log))?]
                                ).map_err(|_| KVError::Weird("unable to construct batch"))?,
                                ..Default::default()
                            }).map_err(|_| KVError::Weird("s2 append_session rx dropped"))?;
                        }
                        OrchestratorCommand::ReadStrongConsistency {
                            key,
                            reflect_applied_state,
                            response_tx
                        } => {
                            if reflect_applied_state.end <= local_state.applied_state.end {
                                // Applied state is caught up with the tail.
                                _ = response_tx
                                    .send(Ok((local_state.applied_state, local_state.storage.get(&key).cloned())))
                                    .inspect_err(|_| debug!("read rx dropped"));
                            } else {
                                // Not yet caught up. Defer until we do.
                                pending_responses.submit(PendingResponse::new(
                                    reflect_applied_state, ResponseContext::Read{ key, response_tx }
                                ))
                            }
                        },
                        OrchestratorCommand::ReadEventualConsistency {
                            key,
                            response_tx
                        } => {
                            _ = response_tx
                                .send(Ok((local_state.applied_state, local_state.storage.get(&key).cloned())))
                                .inspect_err(|_| debug!("read rx dropped"));
                        },
                    }
                }

                Some(ack) = append_acknowledgments.next() => {
                    trace!(?ack);
                    let ack = ack?;
                    let response_tx = write_queue.pop_front().expect("queue entry");
                    // Since our `put` and `delete` KV-store actions do not return prior values,
                    // we can acknowledge them as soon as their corresponding log append is ack-ed
                    // by S2, even if they are not yet applied to the local internalized state.
                    _ = response_tx
                        .send(Ok(..ack.end_seq_num))
                        .inspect_err(|_| debug!("write ack rx dropped"));
                }

                Some(record) = tailing_reader.next() => {
                    trace!(?record, "received record from tailing reader");
                    let record = record?;
                    let log_entry = serde_json::from_slice(record.body.as_ref())
                        .map_err(|e| KVError::JsonError(e.to_string()))?;
                    match log_entry {
                        LogEntry::Put { key, value } => local_state.storage.insert(key, value),
                        LogEntry::Delete { key } => local_state.storage.remove(&key),
                    };

                    local_state.applied_state = ..record.seq_num + 1;

                    for PendingResponse {
                        end_seq_num: _,
                        response_context
                    } in pending_responses.drain_applied_responses(local_state.applied_state) {
                        match response_context {
                            ResponseContext::Read{ key, response_tx } => {
                                _ = response_tx.send(Ok((local_state.applied_state, local_state.storage.get(&key).cloned())))
                                    .inspect_err(|_| debug!("read rx dropped"));
                            }
                            #[cfg(test)]
                            ResponseContext::None => unreachable!(),
                        }
                    }
                }

                else => { break; }
            }
        }

        Ok(())
    }

    async fn new(
        cancellation_token: CancellationToken,
        client: StreamClient,
        recover_from: RangeFrom<SequenceNumber>,
        throttle: Throttle,
    ) -> Result<KVStore, KVError> {
        let (orchestrator_cmd_tx, orchestrator_cmd_rx) = mpsc::unbounded_channel();
        let (bus_tx, bus_rx) = mpsc::unbounded_channel();

        let local_state = LocalState {
            applied_state: ..recover_from.start,
            storage: Default::default(),
        };

        Ok(KVStore {
            orchestrator_cmd_tx,
            bus_tx,
            _orchestrator_task: tokio::spawn(
                KVStore::orchestrate(
                    client.clone(),
                    cancellation_token.clone(),
                    local_state,
                    orchestrator_cmd_rx,
                    throttle,
                )
                .inspect_err(|e| error!(?e, "orchestrator task cancelled")),
            ),
            _bus_stand_task: tokio::spawn(
                KVStore::bus_stand(client, cancellation_token, bus_rx)
                    .inspect_err(|e| error!(?e, "bus stand task cancelled")),
            ),
        })
    }

    /// Get the current stream's tail.
    ///
    /// Wait up to `max_wait` before the actual `check_tail` invocation occurs, allowing
    /// other callers to also be served by the same underlying S2 tail op.
    async fn bus_stand_check_tail(
        &self,
        max_wait: Duration,
    ) -> Result<RangeTo<SequenceNumber>, KVError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.bus_tx
            .send(BusRider {
                max_wait,
                response_tx,
            })
            .map_err(|_| KVError::BusStandTaskFailure)?;

        response_rx
            .await
            .map_err(|_| KVError::BusStandTaskFailure)?
    }

    /// Get the value of a key.
    async fn get(
        &self,
        read_consistency: ReadConsistency,
        key: String,
    ) -> Result<(RangeTo<SequenceNumber>, Option<Value>), KVError> {
        let (response_tx, response_rx) = oneshot::channel();

        let cmd = match read_consistency {
            ReadConsistency::Eventual => {
                OrchestratorCommand::ReadEventualConsistency { key, response_tx }
            }
            ReadConsistency::Strong => OrchestratorCommand::ReadStrongConsistency {
                key,
                reflect_applied_state: self.bus_stand_check_tail(BUS_RIDER_MAX_WAIT).await?,
                response_tx,
            },
        };

        self.orchestrator_cmd_tx
            .send(cmd)
            .map_err(|_| KVError::OrchestratorTaskFailure)?;

        response_rx
            .await
            .map_err(|_| KVError::OrchestratorTaskFailure)?
    }

    /// Put a new key/value to the store.
    ///
    /// This will wait until the associated write is made durable on the log, and until
    /// the log has been applied to the materialized state on this node, before returning
    /// an `Ok` response.
    async fn put(&self, key: String, value: Value) -> Result<RangeTo<SequenceNumber>, KVError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.orchestrator_cmd_tx
            .send(OrchestratorCommand::WriteLog {
                log: LogEntry::Put { key, value },
                response_tx,
            })
            .map_err(|_| KVError::OrchestratorTaskFailure)?;

        response_rx
            .await
            .map_err(|_| KVError::OrchestratorTaskFailure)?
    }

    /// Delete a key and its value from the store.
    ///
    /// This will wait until the associated write is made durable on the log, and until
    /// the log has been applied to the materialized state on this node, before returning
    /// an `Ok` response.
    async fn delete(&self, key: String) -> Result<RangeTo<SequenceNumber>, KVError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.orchestrator_cmd_tx
            .send(OrchestratorCommand::WriteLog {
                log: LogEntry::Delete { key },
                response_tx,
            })
            .map_err(|_| KVError::OrchestratorTaskFailure)?;

        response_rx
            .await
            .map_err(|_| KVError::OrchestratorTaskFailure)?
    }
}

fn router(db_container: Arc<KVStore>) -> Router<()> {
    async fn get_value(
        Path(consistency): Path<ReadConsistency>,
        Query(key): Query<KeyRequest>,
        db_container: Arc<KVStore>,
    ) -> (StatusCode, Json<ValueResponse>) {
        match db_container.get(consistency, key.key).await {
            Ok((applied_state, Some(v))) => {
                (StatusCode::OK, Json(ValueResponse::Ok(applied_state, v)))
            }
            Ok((applied_state, None)) => (
                StatusCode::NOT_FOUND,
                Json(ValueResponse::Missing(applied_state)),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ValueResponse::Error(e.to_string())),
            ),
        }
    }

    async fn put_value(
        Json(put): Json<KeyValueRequest>,
        db_container: Arc<KVStore>,
    ) -> (StatusCode, Json<SequenceResponse>) {
        match db_container.put(put.key, put.value).await {
            Ok(applied_state) => (StatusCode::OK, Json(SequenceResponse::Ok(applied_state))),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(SequenceResponse::Error(e.to_string())),
            ),
        }
    }

    async fn delete_value(
        Query(key): Query<KeyRequest>,
        db_container: Arc<KVStore>,
    ) -> (StatusCode, Json<SequenceResponse>) {
        match db_container.delete(key.key).await {
            Ok(applied_state) => (StatusCode::OK, Json(SequenceResponse::Ok(applied_state))),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(SequenceResponse::Error(e.to_string())),
            ),
        }
    }

    Router::new()
        .route(
            "/api/:consistency",
            get({
                let db = db_container.clone();
                move |path, key| async move { get_value(path, key, db).await }
            }),
        )
        .route(
            "/api",
            put({
                let db = db_container.clone();
                move |req| async move { put_value(req, db).await }
            }),
        )
        .route(
            "/api",
            delete({
                let db = db_container.clone();
                move |key| async move { delete_value(key, db).await }
            }),
        )
        .layer(TraceLayer::new_for_http())
}

#[derive(Debug, Deserialize)]
struct KeyRequest {
    key: String,
}

#[derive(Debug, Deserialize)]
struct KeyValueRequest {
    key: String,
    value: Value,
}

#[derive(Serialize)]
enum SequenceResponse {
    Ok(RangeTo<SequenceNumber>),
    Error(String),
}

#[derive(Serialize)]
enum ValueResponse {
    Ok(RangeTo<SequenceNumber>, Value),
    Missing(RangeTo<SequenceNumber>),
    Error(String),
}

#[derive(Clone, Parser, Debug)]
struct Throttle {
    #[clap(long, default_value = "0ms", value_parser = parse_duration)]
    throttle_append_acknowledgments: Duration,
    #[clap(long, default_value = "0ms", value_parser = parse_duration)]
    throttle_tailing_reader: Duration,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    basin: String,
    stream: String,
    #[arg(long)]
    host: String,
    #[arg(long, default_value_t = 0)]
    recover_from: SequenceNumber,
    #[clap(flatten)]
    throttle: Throttle,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let stream_client = StreamClient::new(
        ClientConfig::new(std::env::var("S2_AUTH_TOKEN")?)
            .with_endpoints(S2Endpoints::from_env().map_err(|msg| eyre!(msg))?),
        args.basin.parse::<BasinName>()?,
        args.stream,
    );
    let cancellation_token = CancellationToken::new();

    let db = KVStore::new(
        cancellation_token.clone(),
        stream_client,
        args.recover_from..,
        args.throttle,
    )
    .await?;

    tokio::select! {
       _ = axum::serve(tokio::net::TcpListener::bind(args.host).await?, router(Arc::new(db))) => {
            Err(eyre!("axum server failed"))
        },
        _ = cancellation_token.cancelled() => {
            Err(eyre!("db token cancelled"))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{PendingResponse, PendingResponses, ResponseContext};

    #[test]
    fn test_pending_responses() {
        let mut pending_responses = PendingResponses::default();

        pending_responses.submit(PendingResponse::new(..5, ResponseContext::None));
        pending_responses.submit(PendingResponse::new(..0, ResponseContext::None));
        pending_responses.submit(PendingResponse::new(..2, ResponseContext::None));
        pending_responses.submit(PendingResponse::new(..10, ResponseContext::None));
        pending_responses.submit(PendingResponse::new(..100, ResponseContext::None));

        let responses: Vec<_> = pending_responses.drain_applied_responses(..0).collect();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses.first().unwrap().end_seq_num.0, 0);
        println!("{:?}", responses);

        let responses: Vec<_> = pending_responses.drain_applied_responses(..0).collect();
        assert_eq!(responses.len(), 0);

        let responses: Vec<_> = pending_responses.drain_applied_responses(..9).collect();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses.first().unwrap().end_seq_num.0, 2);

        let responses: Vec<_> = pending_responses.drain_applied_responses(..200).collect();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses.first().unwrap().end_seq_num.0, 10);

        let responses: Vec<_> = pending_responses.drain_applied_responses(..1000).collect();
        assert_eq!(responses.len(), 0);
    }
}
