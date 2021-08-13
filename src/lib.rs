use futures::{SinkExt, Stream, StreamExt};
use std::marker::PhantomData;
use std::sync::Mutex;
use std::{convert::TryInto, sync::Arc};
use tokio::task::JoinHandle;

use graphql_client::{GraphQLQuery, QueryBody, Response};
use thiserror::Error;
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::{tungstenite, MaybeTlsStream, WebSocketStream};

use crate::protocol::{ClientMessage, ClientPayload, ServerMessage};

mod protocol;

#[derive(Clone)]
pub struct GraphQLWebSocket {
    shared: Arc<Shared>,
}

struct Shared {
    state: Mutex<State>,
    handle: JoinHandle<Result<(), Error>>,

    server_tx: broadcast::Sender<ServerMessage>,
    client_tx: mpsc::UnboundedSender<ClientMessage>,
}

struct State {
    id: u32,
}

impl GraphQLWebSocket {
    /// Initialize a GraphQLWS connection over the provided WebSocket.
    /// Optionally provide the provided payload as part of the ConnectionInit
    /// message.
    pub fn new(
        socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
        payload: Option<serde_json::Value>,
    ) -> Self {
        let server_tx = broadcast::channel::<ServerMessage>(16).0;
        let (mut write, mut read) = socket.split();
        let (client_tx, mut client_rx) = mpsc::unbounded_channel::<ClientMessage>();

        let handle = tokio::spawn({
            let server_tx = server_tx.clone();
            async move {
                write
                    .send(ClientMessage::ConnectionInit { payload }.into())
                    .await?;
                loop {
                    tokio::select! {
                        Some(message) = read.next() => {
                            let msg = message?;
                            let _ = server_tx.send(
                                msg.clone()
                                    .try_into()
                                    .map_err(Error::MessageFormat)?,
                            );
                        },
                        Some(message) = client_rx.recv() => {
                            write.send(message.into()).await?;
                        },
                        else => {break}
                    };
                }
                Ok::<(), Error>(())
            }
        });

        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State { id: 0 }),
                handle,
                server_tx,
                client_tx,
            }),
        }
    }

    /// Execute the GraphQL query or mutation against the remote engine.
    pub async fn query<Query: GraphQLQuery + Send + Sync + 'static>(
        &self,
        variables: Query::Variables,
    ) -> Result<Response<Query::ResponseData>, Error> {
        let op = self.subscribe::<Query>(variables);
        let mut stream = op.execute();
        let response = stream.next().await.ok_or(Error::NoResponse)?;
        response
    }

    /// Execute the GraphQL query or mutation against the remote engine,
    /// returning only response data and panicking if an error occurs.
    pub async fn query_unchecked<Query: GraphQLQuery + Send + Sync + 'static>(
        &self,
        variables: Query::Variables,
    ) -> Query::ResponseData {
        let result = self.query::<Query>(variables).await;
        let response = result.unwrap();
        assert_eq!(response.errors, None);
        response.data.unwrap()
    }

    /// Return a handle to a GraphQL subscription.
    pub fn subscribe<Query: GraphQLQuery + Send + Sync + 'static>(
        &self,
        variables: Query::Variables,
    ) -> GraphQLOperation<Query> {
        let mut state = self.shared.state.lock().unwrap();
        let op = GraphQLOperation::<Query>::new(
            state.id.to_string(),
            Query::build_query(variables),
            self.shared.server_tx.subscribe(),
            self.shared.client_tx.clone(),
        );
        state.id += 1;
        op
    }

    pub fn handle(&self) -> &JoinHandle<Result<(), Error>> {
        &self.shared.handle
    }
}

/// A handle to a long-lived GraphQL operation. Drop the handle to end the
/// operation.
pub struct GraphQLOperation<Query: GraphQLQuery + Send + Sync + 'static> {
    id: String,
    payload: ClientPayload,
    server_rx: broadcast::Receiver<ServerMessage>,
    client_tx: mpsc::UnboundedSender<ClientMessage>,
    _query: PhantomData<Query>,
}
impl<Query: GraphQLQuery + Send + Sync + 'static> GraphQLOperation<Query> {
    fn new(
        id: String,
        query_body: QueryBody<Query::Variables>,
        server_rx: broadcast::Receiver<ServerMessage>,
        client_tx: mpsc::UnboundedSender<ClientMessage>,
    ) -> Self {
        Self {
            id,
            payload: ClientPayload {
                query: query_body.query.to_owned(),
                operation_name: Some(query_body.operation_name.to_owned()),
                variables: Some(serde_json::to_value(query_body.variables).unwrap()),
            },
            server_rx,
            client_tx,
            _query: PhantomData,
        }
    }

    /// Execute the subscription against the remote GraphQL engine. Returns a
    /// Stream of responses.
    pub fn execute(mut self) -> impl Stream<Item = Result<Response<Query::ResponseData>, Error>> {
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let query_msg = ClientMessage::Start {
                id: self.id.to_string(),
                payload: self.payload.clone(),
            };
            if self.client_tx.send(query_msg).is_err() {
                return;
            }
            while let Ok(msg) = self.server_rx.recv().await {
                match msg {
                    ServerMessage::Data { id, payload } if id == self.id => {
                        if tx.send(Ok(payload)).is_err() {
                            return;
                        }
                    }
                    ServerMessage::Complete { id } if id == self.id => {
                        return;
                    }
                    ServerMessage::ConnectionError { payload } => {
                        let _ = tx.send(Err(Error::GraphQl(payload)));
                        return;
                    }
                    ServerMessage::Error { id, payload } if id == self.id => {
                        if tx.send(Err(Error::GraphQl(payload))).is_err() {
                            return;
                        }
                    }
                    ServerMessage::ConnectionAck => {}
                    ServerMessage::ConnectionKeepAlive => {
                        // we should probably send something back...
                    }
                    _ => {}
                }
            }
        });
        UnboundedReceiverStream::new(rx).map(|result| {
            result.map(|payload| {
                serde_json::from_value::<Response<Query::ResponseData>>(payload).unwrap()
            })
        })
    }
}

impl<Query: GraphQLQuery + Send + Sync + 'static> Drop for GraphQLOperation<Query> {
    fn drop(&mut self) {
        let _ = self.client_tx.send(ClientMessage::Stop {
            id: self.id.clone(),
        });
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("websocket error")]
    WebSocket(#[from] tungstenite::Error),
    #[error("graphql protocol error")]
    GraphQl(serde_json::Value),
    #[error("invalid message format")]
    MessageFormat(protocol::MessageError),
    #[error("expected response, but none received")]
    NoResponse,
}
