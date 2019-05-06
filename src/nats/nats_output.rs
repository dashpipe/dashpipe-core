use crate::OutputChannel;
use crate::error::*;

use super::{RUNTIME};

use log::{error};
use nitox::{commands::*, NatsClient};
use futures::{prelude::*, future};

pub struct NatsOutput {
    subject: String,
    nats_client: NatsClient,
}

impl NatsOutput {
    pub fn new(cluster_uri: &str, a_subject: String) -> Result<NatsOutput, DashPipeError> {
        let nats_client_builder = super::connect_to_nats(cluster_uri);

        let ret: Result<NatsOutput, DashPipeError> = match RUNTIME.lock().unwrap().block_on(nats_client_builder){
            Ok(nats_client) => {
                let output = NatsOutput{
                                subject: a_subject.to_owned(),
                                nats_client: nats_client,
                            };
                Ok(output)
            },
            Err(nats_error) => {
                let disp = format!("Unable to initialize NATs {}", nats_error);
                error!("Unable to initialize NATs {}", nats_error);
                Err(DashPipeError::InitializeError(disp))
            }
        };
        ret
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
