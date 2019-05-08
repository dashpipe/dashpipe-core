use crate::InputChannel;
use crate::error::DashPipeError;

use super::RUNTIME;
use log::{error, info};
use futures::{
    prelude::*,
    Stream,
};
use nitox::{commands::*, NatsClient};
use std::io;
use prometheus::{Counter};

pub struct NatsInput {
    subject: String,
    queue_group:Option<String>,
    nats_client: NatsClient,
}

impl NatsInput {
    pub fn new(cluster_uri: &str, a_subject: String, queue_group: Option<String>) -> Result<NatsInput, DashPipeError> {
        let nats_client_builder = super::connect_to_nats(cluster_uri); 
        match RUNTIME.lock().unwrap().block_on(nats_client_builder) {
            Ok(client) => {
                let ret = NatsInput {
                    nats_client: client,
                    queue_group: queue_group,
                    subject: a_subject,
                };
                info!("Consuming client successfully created");
                Ok(ret)
            },
            Err(e) => {
                let disp = format!("Unable to create nats input: {}", e);
                error!("Unable to connect to NATS: {}", e);
                Err(DashPipeError::InitializeError(disp))
            }
        }
    }
}

impl InputChannel for NatsInput {
    fn start(&self) -> Result<Box<Stream<Item=String, Error = io::Error>>, DashPipeError> {
         let group_tag = match self.queue_group.clone(){
            Some(s) => s.to_owned(),
            None => "None".to_owned(),
        };
        
        let msg_recv_counter = register_int_counter!(opts!(
        "dashpipe_received_messages",
        "Total number of messages received",
        labels! {"channel" => "nats", "subject" => &self.subject, "queue_group" => &group_tag, }))
        .unwrap();

        let receive_err_counter = register_int_counter!(opts!(
        "dashpipe_received_messages_error",
        "Total number of errors while receiving messages",
        labels! {"channel" => "nats", "subject" => &self.subject, "queue_group" => &group_tag, }))
        .unwrap();

        let parse_err_counter = register_int_counter!(opts!(
        "dashpipe_received_messages_parse_error",
        "Total number of errors while parsing received messages",
        labels! {"channel" => "nats", "subject" => &self.subject, "queue_group" => &group_tag, }))
        .unwrap();

        let sub_cmd: SubCommand = SubCommand::builder()
            .subject(self.subject.to_owned())
            .queue_group(match &self.queue_group{
                Some(s) => Some(s.clone()),
                None => None,
            })
            .build()
            .unwrap();
            
        let receiver = match self.nats_client.subscribe(sub_cmd).wait(){
            Ok(r) => r,
            Err(e) => {
                let disp = format!("Unable to initialize NATS subscription {}", e);
                error!("Unable to initialize NATS subscription {}", e);
                return Err(DashPipeError::InitializeError(disp))
                },
        };

        let ret = receiver.map_err(move |_| {
            receive_err_counter.inc();
            io::Error::from(io::ErrorKind::Other)})
        .map(
            move |msg: Message| { 
                msg_recv_counter.inc();
                let slice = msg.payload.slice_from(0);
                String::from_utf8(slice.to_vec())
            })
        .filter_map(move |res| {
            match res{
                Ok(s) => Some(s),
                Err(_) => {
                    parse_err_counter.inc();
                    error!("Unable to parse incoming message as a string, skipping");
                    None
                    }
            }
        });

        Ok(Box::new(ret))
    }
}