use std::io;
use std::io::{Error, ErrorKind};
use serde::{Deserialize};
use std::thread;
use std::collections::HashMap;
use futures::future::Future;
use futures::Stream;
use futures::sync::mpsc::channel;
use futures::sink::Sink;
use rdkafka::error::KafkaError;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::Message;
use log::{warn, info};

use crate::InputChannel;
use crate::error::DashPipeError;

#[derive(Debug, Deserialize)]
pub struct KafkaInput{
    topics: String,
    config: HashMap<String, String>,
}

impl KafkaInput{
    pub fn new(kafkaconfig: HashMap<String, String>, stopics: String) -> KafkaInput{
        KafkaInput{
            config:kafkaconfig,
            topics:stopics
        }
    }
}

impl InputChannel for KafkaInput{
    fn start(&self) -> Result<Box<Stream<Item=String, Error = io::Error>>, DashPipeError>{
        let sconsumer = match build_streaming_consumer(&self.config){
            Ok(c) => c,
            Err(e) => return Err(DashPipeError::InitializeError("Unable to initialize Kakfa".to_string())),
        };

        match sconsumer.subscribe(&[&self.topics]){
            Ok(_) => {},
            Err(e) => return Err(DashPipeError::InitializeError("Unable to initialize Kakfa".to_string())),
        }

        Ok(Box::new(receive_messages_fn(sconsumer)))
    }
}

fn receive_messages_fn(sconsumer: StreamConsumer) -> impl Stream<Item=String, Error=io::Error> {
    let (mut sender, receiver) = channel(1000);
    thread::spawn(move || {
        let message_stream = sconsumer.start();
        for message in message_stream.wait() {
            match message{
                Err(_) => warn!("Error while reading from stream."),
                Ok(Err(e)) => warn!("Kafka error: {}", e),
                Ok(Ok(m)) => {
                    info!("Got a beautiful message!!");
                    let payload = match m.payload_view::<str>() {
                        None => Err(Error::new(ErrorKind::InvalidData, "error")),
                        Some(Ok(s)) => Ok(s.to_string()),
                        Some(Err(_)) => Err(Error::new(ErrorKind::InvalidData, "error")),
                    };
                    match sender.send(payload).wait(){
                        Ok(s) => sender = s,
                        Err(_) => break,
                    }; 
                }
            }
        }
    });
    receiver.then(|result|{result.unwrap()})
}


fn build_streaming_consumer(kafkaconfig: &HashMap<String, String>) -> Result<StreamConsumer, KafkaError> {
    let mut client_config = ClientConfig::new();
    for (key, value) in kafkaconfig{
        client_config.set(&key, &value);
    }
   client_config.create()
}