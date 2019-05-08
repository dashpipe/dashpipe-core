#[macro_use]
extern crate prometheus;
extern crate rdkafka;
extern crate log;
extern crate nitox;
#[macro_use]
extern crate lazy_static;
extern crate failure;
#[macro_use]
extern crate failure_derive;

use futures::Stream;
use std::io;

mod error; 
mod standard;
mod kafka;
mod nats;

pub use self::error::*;
pub use standard::{StdinInput, StdoutOutput};
pub use kafka::{KafkaInput, KafkaOutput};
pub use nats::{NatsOutput, NatsInput};

pub trait InputChannel{
    fn start(&self) -> Result<Box<Stream<Item=String, Error = io::Error>>, DashPipeError>;
}

pub trait OutputChannel{
    fn send(&self, msg: String);
}