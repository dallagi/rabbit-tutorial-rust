use futures_lite::stream::StreamExt;
use lapin::ConnectionProperties;
use lapin::{options::*, types::FieldTable, Connection};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let severities = args;

    let addr = "amqp://127.0.0.1:5672/%2f";
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;

    println!("Connected");

    let channel = conn.create_channel().await?;
    channel
        .exchange_declare(
            "direct_logs",
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
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

    for severity in severities {
        channel
            .queue_bind(
                queue.name().as_str(),
                "direct_logs",
                severity.as_str(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
    }

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
