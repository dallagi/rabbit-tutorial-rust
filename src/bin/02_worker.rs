use std::thread;
use std::time::Duration;

use futures_lite::stream::StreamExt;
use lapin::{Connection, options::*, types::FieldTable};
use lapin::ConnectionProperties;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://127.0.0.1:5672/%2f";
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;

    println!("Connected");

    let channel = conn.create_channel().await?;
    let _queue = channel
        .queue_declare(
            "task_queue",
            QueueDeclareOptions{ durable: true, ..QueueDeclareOptions::default() },
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            "task_queue",
            "my_consumer",
            BasicConsumeOptions { no_ack: true, ..BasicConsumeOptions::default() },
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        let (_channel, delivery) = delivery.expect("error in consumer");

        if let Ok(message) = std::str::from_utf8(&delivery.data) {
            println!(" [x] Received '{}'", message);
            do_work(message);
            let _ = channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default());
            println!(" [x] Done");
        } else {
            println!("[!] Received a new message, but failed to decode it")
        }
        // println!("Received {:?}", std::str::from_utf8(&delivery.data).unwrap_or("[ERROR]"));
        // delivery.ack(BasicAckOptions::default()).await.expect("ack");
    }

    Ok(())
}

fn do_work(message: &str) {
    for char in message.chars() {
        if char == '.' {
            thread::sleep(Duration::from_secs(1));
        }
    }
}
