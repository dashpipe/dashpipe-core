use crate::OutputChannel;

use super::{RUNTIME};

use nitox::{commands::*, NatsClient};
use futures::{prelude::*, future};

pub struct NatsOutput {
    subject: String,
    nats_client: NatsClient,
}

impl NatsOutput {
    pub fn new(cluster_uri: &str, a_subject: String) -> NatsOutput {
        let nats_client_builder = super::connect_to_nats(cluster_uri);
        //we need to run the connection inside the tokio runtime
        let nats_client: NatsClient = match RUNTIME.lock().unwrap().block_on(nats_client_builder){
            Ok(c) => c,
            Err(e) => {
                        println!("Unable to connect to NATS: {}", e);
                        panic!(e)
            },
        };
        NatsOutput{
            subject: a_subject.to_owned(),
            nats_client: nats_client,
        }
    }
}

impl OutputChannel for NatsOutput{
    fn send(&self, msg: String){
        let pub_cmd = PubCommand::builder()
                        .subject(self.subject.to_string()).payload(msg).build().unwrap();
        let send = self.nats_client.publish(pub_cmd)
                                    .then(|f| {
                                        match f {
                                            Ok(_) => {//TODO metrics here
                                                     },
                                            Err(e) =>{
                                                //TODO metrcis here
                                            },
                                        }
                                        future::ok(())
                                    });
        RUNTIME.lock().unwrap().spawn(send);
    }
}
