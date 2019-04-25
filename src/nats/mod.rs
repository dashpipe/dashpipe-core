mod nats_output;
mod nats_input;

use std::sync::{Mutex};
use nitox::{commands::*, NatsClient, NatsClientOptions, NatsError};
use futures::{prelude::*, future};

pub use nats_output::NatsOutput;
pub use nats_input::NatsInput;

lazy_static!{
    static ref RUNTIME: Mutex<tokio::runtime::Runtime> = {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        Mutex::new(runtime)
    };
}


fn connect_to_nats(cluster_uri: &str) -> impl Future<Item = NatsClient, Error = NatsError> {
    // Defaults as recommended per-spec, but you can customize them
    let connect_cmd = ConnectCommand::builder().build().unwrap();
    let options = NatsClientOptions::builder()
        .connect_command(connect_cmd)
        .cluster_uri(cluster_uri)
        .build()
        .unwrap();

    NatsClient::from_options(options)
        .and_then(|client| {
            // Makes the client send the CONNECT command to the server, but it's usable as-is if needed
            client.connect()
        })
        .and_then(|client| {
            // Client has sent its CONNECT command and is ready for usage
            future::ok(client)
        })
}