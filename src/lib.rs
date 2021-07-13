use futures::{future, SinkExt, Stream, StreamExt};
use std::marker::PhantomData;
use std::sync::Mutex;
use std::{convert::TryInto, sync::Arc};

use graphql_client::{GraphQLQuery, QueryBody, Response};
use thiserror::Error;
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{tungstenite, MaybeTlsStream, WebSocketStream};

use crate::protocol::{ClientMessage, ClientPayload, ServerMessage};

mod protocol;

#[derive(Clone)]
pub struct GraphQLWebSocket {
    shared: Arc<Shared>,
}

struct Shared {
    state: Mutex<State>,

    client_tx: broadcast::Sender<ClientMessage>,
    server_tx: broadcast::Sender<ServerMessage>,
}

struct State {
    id: u32,
}

impl GraphQLWebSocket {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State { id: 0 }),
                client_tx: broadcast::channel(16).0,
                server_tx: broadcast::channel(16).0,
            }),
        }
    }

    /// Initialize a GraphQLWS connection over the provided WebSocket.
    /// Optionally provide the provided payload as part of the ConnectionInit
    /// message.
    pub fn connect(
        &self,
        socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
        payload: Option<serde_json::Value>,
    ) -> JoinHandle<Result<(), Error>> {
        let (mut write, mut read) = socket.split();
        let server_tx = self.shared.server_tx.clone();
        let recv_fut: future::BoxFuture<Result<(), Error>> = Box::pin(async move {
            while let Some(message) = read.next().await {
                let msg = message?;
                let _ = server_tx.send(
                    msg.clone()
                        .try_into()
                        .map_err(|_| Error::MessageFormat(msg))?,
                );
            }
            Ok::<(), Error>(())
        });
        let mut client_rx = self.shared.client_tx.subscribe();
        let send_fut: future::BoxFuture<Result<(), Error>> = Box::pin(async move {
            write
                .send(ClientMessage::ConnectionInit { payload }.into())
                .await?;
            while let Ok(message) = client_rx.recv().await {
                write.send(message.into()).await?;
            }
            Ok::<(), Error>(())
        });
        tokio::spawn(async move {
            future::try_join_all(vec![recv_fut, send_fut])
                .await
                .map(|_| ())
        })
    }

    /// Execute the GraphQL query or mutation against the remote engine.
    pub async fn query<Query: GraphQLQuery>(
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
    pub async fn query_unchecked<Query: GraphQLQuery>(
        &self,
        variables: Query::Variables,
    ) -> Query::ResponseData {
        let result = self.query::<Query>(variables).await;
        let response = result.unwrap();
        assert_eq!(response.errors, None);
        response.data.unwrap()
    }

    /// Return a handle to a GraphQL subscription.
    pub fn subscribe<Query: GraphQLQuery>(
        &self,
        variables: Query::Variables,
    ) -> GraphQLOperation<Query> {
        let mut state = self.shared.state.lock().unwrap();
        let op = GraphQLOperation::<Query>::new(
            state.id.to_string(),
            Query::build_query(variables),
            self.shared.server_tx.clone(),
            self.shared.client_tx.clone(),
        );
        state.id += 1;
        op
    }
}

/// A handle to a long-lived GraphQL operation. Drop the handle to end the
/// operation.
pub struct GraphQLOperation<Query: GraphQLQuery> {
    id: String,
    payload: ClientPayload,
    server_tx: broadcast::Sender<ServerMessage>,
    client_tx: broadcast::Sender<ClientMessage>,
    _query: PhantomData<Query>,
}
impl<Query: GraphQLQuery> GraphQLOperation<Query> {
    fn new(
        id: String,
        query_body: QueryBody<Query::Variables>,
        server_tx: broadcast::Sender<ServerMessage>,
        client_tx: broadcast::Sender<ClientMessage>,
    ) -> Self {
        Self {
            id,
            payload: ClientPayload {
                query: query_body.query.to_owned(),
                operation_name: Some(query_body.operation_name.to_owned()),
                variables: Some(serde_json::to_value(query_body.variables).unwrap()),
            },
            server_tx,
            client_tx,
            _query: PhantomData,
        }
    }

    /// Execute the subscription against the remote GraphQL engine. Returns a
    /// Stream of responses.
    pub fn execute(self) -> impl Stream<Item = Result<Response<Query::ResponseData>, Error>> {
        let (tx, rx) = mpsc::channel(16);

        let client_tx = self.client_tx.clone();
        let mut server_rx = self.server_tx.subscribe();
        let op_id = self.id.clone();
        let query_msg = ClientMessage::Start {
            id: op_id.to_string(),
            payload: self.payload.clone(),
        };
        tokio::spawn(async move {
            if let Err(_) = client_tx.send(query_msg) {
                return;
            }
            while let Ok(msg) = server_rx.recv().await {
                match msg {
                    ServerMessage::Data { id, payload } if id == op_id => {
                        if let Err(_) = tx.send(Ok(payload)).await {
                            return;
                        }
                    }
                    ServerMessage::Complete { id } if id == op_id => {
                        return;
                    }
                    ServerMessage::ConnectionError { payload } => {
                        let _ = tx.send(Err(Error::GraphQl(payload))).await;
                        return;
                    }
                    ServerMessage::Error { id, payload } if id == op_id => {
                        if let Err(_) = tx.send(Err(Error::GraphQl(payload))).await {
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
        ReceiverStream::new(rx).map(|result| {
            result.map(|payload| {
                serde_json::from_value::<Response<Query::ResponseData>>(payload).unwrap()
            })
        })
    }
}

impl<Query: GraphQLQuery> Drop for GraphQLOperation<Query> {
    fn drop(&mut self) {
        self.client_tx
            .send(ClientMessage::Stop {
                id: self.id.clone(),
            })
            .unwrap();
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("websocket error")]
    WebSocket(#[from] tungstenite::Error),
    #[error("graphql protocol error")]
    GraphQl(serde_json::Value),
    #[error("invalid message format")]
    MessageFormat(tungstenite::Message),
    #[error("expected response, but none received")]
    NoResponse,
}
