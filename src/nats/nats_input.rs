use crate::InputChannel;

use super::RUNTIME;

use futures::{
    prelude::*,
    Stream,
};
use nitox::{commands::*, NatsClient};
use std::io;

pub struct NatsInput {
    subject: String,
    queue_group:Option<String>,
    nats_client: NatsClient,
}

impl NatsInput {
    pub fn new(cluster_uri: &str, a_subject: String, queue_group: Option<String>) -> NatsInput {
        let nats_client_builder = super::connect_to_nats(cluster_uri);
        let nats_client: NatsClient = match RUNTIME.lock().unwrap().block_on(nats_client_builder) {
            Ok(c) => c,
            Err(e) => {
                println!("Unable to connect to NATS: {}", e);
                panic!(e)
            }
        };
        NatsInput {
            nats_client: nats_client,
            queue_group: queue_group,
            subject: a_subject,
        }
    }
}

impl InputChannel for NatsInput {
    fn start(&self) -> Box<Stream<Item = String, Error = io::Error>> {
        let sub_cmd: SubCommand = SubCommand::builder()
            .subject(self.subject.to_owned())
            .queue_group(match &self.queue_group{
                Some(s) => Some(s.clone()),
                None => None,
            })
            .build()
            .unwrap();
        let receiver = self.nats_client.subscribe(sub_cmd).wait().unwrap();

        let ret = receiver.map_err(|_| { io::Error::from(io::ErrorKind::Other)})
        .map(
            |msg: Message| { 
                let slice = msg.payload.slice_from(0);
                String::from_utf8(slice.to_vec())
            })
        .filter_map(|res| {
            match res{
                Ok(s) => Some(s),
                Err(_) => None,
            }
        });

        Box::new(ret)
    }
}