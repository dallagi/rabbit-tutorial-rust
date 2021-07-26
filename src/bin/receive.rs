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
            "hello",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            "hello",
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    while let Some(delivery) = consumer.next().await {
        let (_channel, delivery) = delivery.expect("error in consumer");
        println!("Received {:?}", std::str::from_utf8(&delivery.data).unwrap_or("[ERROR]"));
        // delivery.ack(BasicAckOptions::default()).await.expect("ack");
    }

    Ok(())
}
