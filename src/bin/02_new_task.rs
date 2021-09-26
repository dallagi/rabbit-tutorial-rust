use std::env;

use lapin::{
    options::{BasicPublishOptions, BasicQosOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://127.0.0.1:5672/%2f";
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;

    println!("Connected");

    let channel = conn.create_channel().await?;
    let _ = channel.basic_qos(1, BasicQosOptions::default());
    let queue = channel
        .queue_declare(
            "task_queue",
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    println!("Declared queue {:?}", queue);

    let message = message_from_args();

    let _confirm = channel
        .basic_publish(
            "",
            "hello",
            BasicPublishOptions::default(),
            message.as_bytes().to_vec(),
            BasicProperties::default().with_delivery_mode(2),
        )
        .await?
        .await?;

    println!(" [x] Sent {:?}", message);

    let _ = conn.close(0, "");

    Ok(())
}

fn message_from_args() -> String {
    let args = env::args().collect::<Vec<String>>();
    args[1..].join(" ")
}
