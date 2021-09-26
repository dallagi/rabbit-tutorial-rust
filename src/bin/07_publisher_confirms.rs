use std::{error::Error, time::SystemTime};

use lapin::{BasicProperties, Connection, ConnectionProperties, options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions}, types::FieldTable};

const MESSAGE_COUNT: i32 = 50_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    publish_messages_individually().await?;

    Ok(())
}

async fn create_connection() -> Result<Connection, Box<dyn Error>> {
    let addr = "amqp://127.0.0.1:5672/%2f";
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;
    Ok(conn)
}

async fn publish_messages_individually() -> Result<(), Box<dyn Error>> {
    let conn = create_connection().await?;
    let channel = conn.create_channel().await?;
    let queue_name = "publish_messages_individually";

    let _ = channel.queue_declare(
        queue_name,
        QueueDeclareOptions {
            durable: false,
            exclusive: false,
            auto_delete: true,
            ..Default::default()
        },
        FieldTable::default(),
    );

    let _ = channel
        .confirm_select(ConfirmSelectOptions {
            ..Default::default()
        })
        .await;

    let now = SystemTime::now();

    for i in 0..MESSAGE_COUNT {
        let body = i.to_le_bytes().to_vec();
        let _ = channel.basic_publish(
            "",
            queue_name,
            BasicPublishOptions {
                ..Default::default()
            },
            body,
            BasicProperties::default()
            ,
        );
        let _ = channel.wait_for_confirms();
    }

    let elapsed = now.elapsed().unwrap();
    println!("Published {} messages individually in {} ms", MESSAGE_COUNT, elapsed.as_millis());

    Ok(())
}
