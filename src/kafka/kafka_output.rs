use crate::OutputChannel;
use crate::error::DashPipeError;

use log::{error, info};
use std::collections::HashMap;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;

pub struct KafkaOutput{
    topic: String,
    producer: FutureProducer,
}

impl KafkaOutput{
    pub fn new(kafka_config: HashMap<String, String>, a_topic: String) -> Result<KafkaOutput, DashPipeError>{
        match build_producer(&kafka_config){
            Ok(producer) => Ok(KafkaOutput{
                                topic: a_topic,
                                producer : producer,
                              }),
            Err(e) => Err(e),
        }
    }
}

impl OutputChannel for KafkaOutput{
    fn send(&self, msg: String){
        //TODO how can we customize the key??
        let record = FutureRecord::to(&self.topic)
                    .payload(&msg)
                    .key(&msg);
        //TODO add a handle here and_then or a map to keep metrics on success and
        //failure
        self.producer.send(record, 100);
    }
}

fn build_producer(kafkaconfig: &HashMap<String, String>) -> Result<FutureProducer, DashPipeError>{
    let mut client_config = ClientConfig::new();
    for (key, value) in kafkaconfig{
        client_config.set(&key, &value);
    }

    match client_config.create(){
        Ok(c) => {info!("Properly created producer"); Ok(c)},
        Err(e) => {
            let disp = format!("Unable to initialize kakfa producer {}", e);
            error!("Unnable to intiialize kafka, {}", e);
            Err(DashPipeError::InitializeError(disp))
        },
    }
}