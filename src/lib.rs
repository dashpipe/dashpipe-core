extern crate rdkafka;
extern crate log;
extern crate nitox;
#[macro_use]
extern crate lazy_static;

use futures::Stream;
use std::io;
 
mod standard;
mod kafka;
mod nats;

pub use standard::{StdinInput, StdoutOutput};
pub use kafka::{KafkaInput, KafkaOutput};
pub use nats::{NatsOutput, NatsInput};

pub trait InputChannel{
    fn start(&self) -> Box<Stream<Item=String, Error = io::Error>>;
}

pub trait OutputChannel{
    fn send(&self, msg: String);
}