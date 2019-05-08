use crate::OutputChannel;
use crate::error::*;

use super::{RUNTIME};

use log::{error};
use nitox::{commands::*, NatsClient};
use futures::{prelude::*, future};
use prometheus::{IntCounter, Histogram};

pub struct NatsOutput {
    subject: String,
    nats_client: NatsClient,
    msg_send_histogram: Histogram,
    send_err_counter: IntCounter,
}

impl NatsOutput {
    pub fn new(cluster_uri: &str, a_subject: String) -> Result<NatsOutput, DashPipeError> {
        let msg_send_histogram = register_histogram!(histogram_opts!(
        "dashpipe_sent_messages",
        "Histogram for messages sent",
        vec![1.0, 2.0],
        labels! {"channel".to_string() => "nats".to_string(), "subject".to_string() => a_subject.clone(), }))
        .unwrap();

        let send_err_counter = register_int_counter!(opts!(
        "dashpipe_sent_messages_error",
        "Total number of errors while sending messages",
        labels! {"channel" => "nats", "subject" => &a_subject, }))
        .unwrap();

        let nats_client_builder = super::connect_to_nats(cluster_uri);

        let ret: Result<NatsOutput, DashPipeError> = match RUNTIME.lock().unwrap().block_on(nats_client_builder){
            Ok(nats_client) => {
                let output = NatsOutput{
                                subject: a_subject.to_owned(),
                                nats_client: nats_client,
                                msg_send_histogram: msg_send_histogram,
                                send_err_counter: send_err_counter,
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
        
        //try to avoid the clone
        let ctr = self.send_err_counter.clone();
        let timer = self.msg_send_histogram.start_timer();
        let send = self.nats_client.publish(pub_cmd)
                                    .then(move |f| {
                                        timer.observe_duration();
                                        match f {
                                            Ok(_) => {},
                                            Err(e) =>{
                                               error!("Unable to send message {}", e); 
                                               ctr.inc();
                                            },
                                        }
                                        future::ok(())
                                    });
        RUNTIME.lock().unwrap().spawn(send);
    }
}
