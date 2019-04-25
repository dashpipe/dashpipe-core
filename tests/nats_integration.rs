extern crate dashpipe;
extern crate testcontainers;

use futures::{
    prelude::*,
    Stream,
    sync::{oneshot},
};
use dashpipe::{OutputChannel, InputChannel, NatsInput, NatsOutput};
use testcontainers::*;
use testcontainers::images::generic::{GenericImage, WaitFor};
use nitox::commands::{ConnectCommand, SubCommand, PubCommand};
use nitox::{NatsClient, NatsError, NatsClientOptions};

#[test]
fn test_send_message(){
    let docker = clients::Cli::default();
    let nats_image = GenericImage::new("nats:1.4.1-linux")
                    .with_wait_for(WaitFor::message_on_stderr("Listening for client connections"));
    let node = docker.run(nats_image);
    let cluster_uri = format!("localhost:{}", node.get_host_port(4222).unwrap());
    let output = NatsOutput::new(&cluster_uri, "testsubject".to_string());

    let connect_cmd = ConnectCommand::builder().build().unwrap();
    let options = NatsClientOptions::builder()
        .connect_command(connect_cmd)
        .cluster_uri(cluster_uri.clone())
        .build()
        .unwrap();

    let fut = NatsClient::from_options(options)
        .and_then(|client| client.connect())
        .and_then(|client| {
            client
                .subscribe(SubCommand::builder().subject("testsubject").build().unwrap())
                .map_err(|_| NatsError::InnerBrokenChain)
                .and_then(move |stream| {
                    output.send("hello world".to_string());

                    stream
                        .take(1)
                        .into_future()
                        .map(|(maybe_message, _)| maybe_message.unwrap())
                        .map_err(|_| NatsError::InnerBrokenChain)
                })
        });

    let (tx, rx) = oneshot::channel();
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(fut.then(|r| tx.send(r).map_err(|e| panic!("Cannot send Result {:?}", e))));
    let connection_result = rx.wait().expect("Cannot wait for a result");
    let _ = runtime.shutdown_now().wait();
    assert!(connection_result.is_ok());
    let msg = connection_result.unwrap();
    assert_eq!(msg.payload, "hello world");
    docker.rm(node.id());
}

#[test]
fn test_receive_messages(){
    let docker = clients::Cli::default();
    let nats_image = GenericImage::new("nats:1.4.1-linux")
                    .with_wait_for(WaitFor::message_on_stderr("Listening for client connections"));
    let node = docker.run(nats_image);
    let cluster_uri = format!("localhost:{}", node.get_host_port(4222).unwrap());

    let input = NatsInput::new(&cluster_uri, "receivedata".to_string(), None);

    let connect_cmd = ConnectCommand::builder().build().unwrap();
    let options = NatsClientOptions::builder()
        .connect_command(connect_cmd)
        .cluster_uri(cluster_uri.clone())
        .build()
        .unwrap();

    let fut = NatsClient::from_options(options)
        .and_then(|client| client.connect())
        .map_err(|_| NatsError::InnerBrokenChain)
        .and_then(|client| {
            for x in 0..5 {
                let pub_cmd = PubCommand::builder()
                            .subject("receivedata").payload(format!("hello {}", x)).build().unwrap();
                client.publish(pub_cmd);
            }
            Ok(())
        });
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(fut.or_else(|_|{Ok(())}));

    let mut messages: Vec<String> = Vec::new();
    let _ = input.start()
       .take(5)
       .for_each(|m| { 
           let _ = messages.push(m); 
           Ok(())})
        .wait();
    
    assert_eq!(messages.len(), 5);
    let worked = match messages.pop(){
        Some(s) => s.starts_with("hello "),
        None => false,
    };
    assert!(worked);
    docker.rm(node.id());
}