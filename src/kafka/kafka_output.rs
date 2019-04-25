use crate::OutputChannel;

use std::collections::HashMap;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;

pub struct KafkaOutput{
    topic: String,
    producer: FutureProducer,
}

impl KafkaOutput{
    pub fn new(kafka_config: HashMap<String, String>, a_topic: String) -> KafkaOutput{
        let a_producer = build_producer(&kafka_config);
        KafkaOutput{
            topic: a_topic,
            producer : a_producer,
        }
    }
}

impl OutputChannel for KafkaOutput{
    fn send(&self, msg: String){
        let record = FutureRecord::to(&self.topic)
                    .payload(&msg)
                    .key(&msg);
        //TODO add a handle here and_then or a map to keep metrics on success and
        //failure
        self.producer.send(record, 100);
        println!("sent data");
    }
}

fn build_producer(kafkaconfig: &HashMap<String, String>) -> FutureProducer{
    let mut client_config = ClientConfig::new();
    for (key, value) in kafkaconfig{
        client_config.set(&key, &value);
    }

    let producer: FutureProducer = match client_config.create(){
        Ok(c) => {println!("Properly created producer"); c},
        Err(e) => panic!("Unnable to intiialize kafka, {}", e),
    };
    producer
}