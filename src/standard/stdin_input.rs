use std::io;
use std::io::prelude::*;
use std::thread;
use futures::{Stream, Sink, Future};
use futures::sync::mpsc::channel;

use crate::InputChannel;
use crate::error::DashPipeError;

pub struct StdinInput{

}

impl StdinInput{
    pub fn new() -> StdinInput{
        StdinInput{}
    }
}

impl InputChannel for StdinInput{
    fn start(&self) -> Result<Box<Stream<Item=String, Error = io::Error>>, DashPipeError>{
        Ok(Box::new(stdin()))
    }
}

fn stdin() -> impl Stream<Item=String, Error=io::Error>{
    let (mut tx, rx) = channel(100);
    thread::spawn(move || {
        let input = io::stdin();
        for line in input.lock().lines() {
            match tx.send(line).wait() {
                Ok(s) => tx = s,
                Err(_) => break,
            }
        }
    });
    rx.then(|e| e.unwrap())
}