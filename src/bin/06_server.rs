use std::convert::TryInto;

use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        ExchangeDeclareOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    Connection, ConnectionProperties,
};

fn fib(n: i32) -> i32 {
    match n {
        0 => 0,
        1 => 1,
        n => fib(n - 1) + fib(n - 2),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://127.0.0.1:5672/%2f";
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;

    println!("Connected");

    let channel = conn.create_channel().await?;

    let _ = channel.queue_declare(
        "rpc_queue",
        QueueDeclareOptions::default(),
        FieldTable::default(),
    );

    channel
        .exchange_declare(
            "topic_logs",
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let _ = channel.basic_qos(1, BasicQosOptions::default());

    let mut consumer = channel
        .basic_consume(
            "rpc_queue",
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!(" [x] Awaiting RPC requests");

    while let Some(delivery) = consumer.next().await {
        let (_channel, delivery) = delivery.expect("error in consumer");

        // if let Ok(message) = std::str::from_utf8(&delivery.data) {
        if let Ok(message) = delivery.data.as_slice().try_into() {
            println!(" [x] Received '{:?}'", message);
            let num: i32 = i32::from_le_bytes(message);
            let response = fib(num);

            let reply_to = delivery.properties.reply_to().as_ref().unwrap();

            let _ = channel.basic_publish(
                "",
                reply_to.as_str(),
                BasicPublishOptions::default(),
                response.to_le_bytes().to_vec(),
                Default::default(),
            );
            let _ = channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default());
        } else {
            println!("[!] Received a new message, but failed to decode it")
        }
    }

    let _ = conn.close(0, "");

    Ok(())
}
