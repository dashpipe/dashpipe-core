use crate::OutputChannel;

use std::thread;
use std::thread::{JoinHandle};
use crossbeam::crossbeam_channel::{select, unbounded, Sender, Receiver};


pub struct StdoutOutput{
    sender : Sender<String>,
    joiner : JoinHandle<()>
}

impl StdoutOutput{
    pub fn new()-> StdoutOutput{
        let (sender, receiver) = unbounded::<String>();
        let joiner = thread::spawn(move || do_output(&receiver));
        let instance = StdoutOutput{
            sender, joiner
        };
        instance
    }
}

impl OutputChannel for StdoutOutput{
    fn send(&self, msg: String){
        self.sender.send(msg).unwrap();
    }
}

fn do_output(receiver: &Receiver<String>) {
    loop{
        select! {
            recv(receiver)-> result =>{
                let msg: String = result.unwrap();
                println!("{}", msg);
            }
        }
    }
}
