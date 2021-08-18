use futures_lite::stream::StreamExt;
use lapin::ConnectionProperties;
use lapin::{options::*, types::FieldTable, Connection};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://127.0.0.1:5672/%2f";
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;

    println!("Connected");

    let channel = conn.create_channel().await?;
    let queue = channel
        .queue_declare(
            "",
            QueueDeclareOptions {
                exclusive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_bind(
            queue.name().as_str(),
            "logs",
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            queue.name().as_str(),
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        let (_channel, delivery) = delivery.expect("error in consumer");

        if let Ok(message) = std::str::from_utf8(&delivery.data) {
            println!(" [x] Received '{}'", message);
            let _ = channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default());
        } else {
            println!("[!] Received a new message, but failed to decode it")
        }
    }

    Ok(())
}
