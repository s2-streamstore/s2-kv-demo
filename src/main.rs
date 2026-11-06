use async_stream::stream;
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::routing::{delete, get, put};
use axum::{Json, Router};
use bytes::Bytes;
use clap::Parser;
use eyre::eyre;
use futures::TryFutureExt;
use humantime::parse_duration;
use ordered_float::OrderedFloat;
use s2_sdk::append_session::{AppendSession, AppendSessionConfig};
use s2_sdk::error::{AppendSessionError, ReadError, ReadSessionError};
use s2_sdk::types::{
    AppendInput, AppendRecord, AppendRecordBatch, AppendRetryPolicy, BasinName, ReadBatch,
    ReadFrom, ReadInput, ReadSessionConfig, ReadSessionRetryPolicy, ReadStart, RetryConfig,
    S2Config, S2Endpoints, SequencedRecord, StreamName, ValidationError,
};
use s2_sdk::{S2, S2Stream};
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::ops::RangeTo;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, trace};

type SequenceNumber = u64;
type SequencedValue = (RangeTo<SequenceNumber>, Option<Value>);
type WriteSender = oneshot::Sender<Result<RangeTo<SequenceNumber>, KVError>>;
type ReadSender = oneshot::Sender<Result<SequencedValue, KVError>>;

#[derive(Debug, thiserror::Error)]
enum KVError {
    #[error("bus stand task failed")]
    BusStandTaskFailure,
    #[error("serde: {0}")]
    JsonError(String),
    #[error("orchestrator task failed")]
    OrchestratorTaskFailure,
    #[error("validation: {0}")]
    ValidationError(#[from] ValidationError),
    #[error("s2 read: {0}")]
    S2Read(#[from] ReadError),
    #[error("s2 read session: {0}")]
    S2ReadSession(#[from] ReadSessionError),
    #[error("s2 append session: {0}")]
    S2AppendSession(#[from] AppendSessionError),
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

/// Ordered (and compared) by `end_seq_num` alone, so `PendingResponses` acts as a
/// min-heap on the sequence number each response is waiting for.
#[derive(Debug)]
struct PendingResponse {
    end_seq_num: Reverse<SequenceNumber>,
    response_context: ResponseContext,
}

impl PartialEq for PendingResponse {
    fn eq(&self, other: &Self) -> bool {
        self.end_seq_num == other.end_seq_num
    }
}

impl Eq for PendingResponse {}

impl PartialOrd for PendingResponse {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingResponse {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.end_seq_num.cmp(&other.end_seq_num)
    }
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
        stream: S2Stream,
        cancellation_token: CancellationToken,
        mut inbox: mpsc::UnboundedReceiver<BusRider>,
    ) -> Result<(), KVError> {
        let _guard = cancellation_token.drop_guard();

        // A bus leaves as soon as there is a rider and no bus is already out. Riders who
        // show up while a `check_tail` is in flight board the next one, so under load
        // each round trip serves everyone who arrived during the previous one.
        while let Some(BusRider { response_tx }) = inbox.recv().await {
            let mut riders = vec![response_tx];
            while let Ok(BusRider { response_tx }) = inbox.try_recv() {
                riders.push(response_tx);
            }
            trace!(num_passengers = riders.len(), "next departure is leaving!");
            let tail = stream.check_tail().await.map(|tail| ..tail.seq_num);
            trace!(?tail);
            for rider in riders {
                _ = rider
                    .send(tail.clone().map_err(KVError::from))
                    .inspect_err(|_| debug!("SC caller disconnected before check_tail returned"));
            }
        }

        Ok(())
    }

    async fn orchestrate(
        stream: S2Stream,
        cancellation_token: CancellationToken,
        mut local_state: LocalState,
        mut command_rx: mpsc::UnboundedReceiver<OrchestratorCommand>,
        throttle: Throttle,
    ) -> Result<(), KVError> {
        use futures::stream::FuturesOrdered;

        let _guard = cancellation_token.drop_guard();

        // Blocked responses for internal callers.
        let mut pending_responses = PendingResponses::default();

        // Append session, opened on first write and replaced after it fails. The SDK retries
        // transient errors itself, but once its retry budget is exhausted the session is dead.
        let mut append_session: Option<AppendSession> = None;
        // Track pending append tickets paired with response senders (ordered for FIFO).
        let mut pending_appends: FuturesOrdered<_> = FuturesOrdered::new();

        // Start a tailing read session. It reconnects and resumes on its own for as long as
        // S2 is unreachable; only non-retryable errors end it (and with it, the node).
        let read_input = ReadInput::new().with_start(
            ReadStart::new().with_from(ReadFrom::SeqNum(local_state.applied_state.end)),
        );
        let read_config =
            ReadSessionConfig::new().with_retry_policy(ReadSessionRetryPolicy::Indefinite);
        let tailing_reader = futures::StreamExt::flat_map(
            stream.read_session(read_input, read_config).await?,
            |read_batch: Result<ReadBatch, ReadSessionError>| {
                stream! {
                    let batch = read_batch.map_err(KVError::from)?;
                    for record in batch.records {
                        yield Ok::<SequencedRecord, KVError>(record)
                    }
                }
            },
        )
        .throttle(throttle.throttle_tailing_reader);

        tokio::pin!(tailing_reader);

        loop {
            tokio::select! {
                Some(cmd) = command_rx.recv() => {
                    trace!(?cmd, "command received");
                    match cmd {
                        OrchestratorCommand::WriteLog { log, response_tx } => {
                            let records = AppendRecordBatch::try_from_iter(
                                [AppendRecord::new(bytes::Bytes::from(log))?]
                            ).map_err(|_| KVError::Weird("unable to construct batch"))?;
                            let input = AppendInput::new(records);
                            let submitted = append_session
                                .get_or_insert_with(|| stream.append_session(AppendSessionConfig::default()))
                                .submit(input)
                                .await;
                            match submitted {
                                Ok(ticket) => {
                                    // Wrap ticket with response_tx so we can respond when it completes.
                                    pending_appends.push_back(async move {
                                        let result = ticket.await;
                                        (result, response_tx)
                                    });
                                }
                                Err(e) => {
                                    append_session = None;
                                    _ = response_tx
                                        .send(Err(e.into()))
                                        .inspect_err(|_| debug!("write ack rx dropped"));
                                }
                            }
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

                Some((ack_result, response_tx)) = pending_appends.next() => {
                    trace!(?ack_result);
                    // Since our `put` and `delete` KV-store actions do not return prior values,
                    // we can acknowledge them as soon as their corresponding log append is ack-ed
                    // by S2, even if they are not yet applied to the local internalized state.
                    //
                    // A failed append fails only that write (the caller gets a 500 and, thanks to
                    // `AppendRetryPolicy::NoSideEffects`, the record is not in the log). The
                    // session is dropped so the next write opens a fresh one.
                    let response = match ack_result {
                        Ok(ack) => Ok(..ack.end.seq_num),
                        Err(e) => {
                            append_session = None;
                            Err(e.into())
                        }
                    };
                    _ = response_tx
                        .send(response)
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
        stream: S2Stream,
        throttle: Throttle,
    ) -> Result<KVStore, KVError> {
        let (orchestrator_cmd_tx, orchestrator_cmd_rx) = mpsc::unbounded_channel();
        let (bus_tx, bus_rx) = mpsc::unbounded_channel();

        let local_state = LocalState {
            applied_state: ..0,
            storage: Default::default(),
        };

        Ok(KVStore {
            orchestrator_cmd_tx,
            bus_tx,
            _orchestrator_task: tokio::spawn(
                KVStore::orchestrate(
                    stream.clone(),
                    cancellation_token.clone(),
                    local_state,
                    orchestrator_cmd_rx,
                    throttle,
                )
                .inspect_err(|e| error!(?e, "orchestrator task cancelled")),
            ),
            _bus_stand_task: tokio::spawn(
                KVStore::bus_stand(stream, cancellation_token, bus_rx)
                    .inspect_err(|e| error!(?e, "bus stand task cancelled")),
            ),
        })
    }

    /// Get the current stream's tail.
    ///
    /// Callers that arrive while a `check_tail` is already in flight are served by the
    /// next one, so concurrent strong reads share S2 tail ops instead of each issuing
    /// their own.
    async fn bus_stand_check_tail(&self) -> Result<RangeTo<SequenceNumber>, KVError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.bus_tx
            .send(BusRider { response_tx })
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
                reflect_applied_state: self.bus_stand_check_tail().await?,
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
    /// Returns `Ok` once the associated write is durable on the log (i.e. S2 has
    /// acknowledged the append). It does not wait for the write to be applied to this
    /// node's materialized state, so an immediate `Eventual` read may not reflect it;
    /// a `Strong` read will.
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
    /// Returns `Ok` once the associated write is durable on the log (i.e. S2 has
    /// acknowledged the append). It does not wait for the write to be applied to this
    /// node's materialized state, so an immediate `Eventual` read may not reflect it;
    /// a `Strong` read will.
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
            "/api/{consistency}",
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
    throttle_tailing_reader: Duration,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    basin: String,
    stream: String,
    #[arg(long)]
    host: String,
    #[clap(flatten)]
    throttle: Throttle,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    // Only retry appends the server provably never saw, so a write that fails with a
    // 500 is guaranteed not to be in the log (the default `All` could duplicate it).
    let mut config = S2Config::new(std::env::var("S2_ACCESS_TOKEN")?).with_retry(
        RetryConfig::default().with_append_retry_policy(AppendRetryPolicy::NoSideEffects),
    );

    if let Ok(s) = S2Endpoints::from_env() {
        config = config.with_endpoints(s)
    };

    let s2 = S2::new(config)?;
    let stream = s2
        .basin(args.basin.parse::<BasinName>()?)
        .stream(args.stream.parse::<StreamName>()?);
    let cancellation_token = CancellationToken::new();

    let db = KVStore::new(cancellation_token.clone(), stream, args.throttle).await?;

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
