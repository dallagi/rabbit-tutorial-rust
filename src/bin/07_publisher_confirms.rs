use std::{error::Error, io::{self, Write}, time::SystemTime};

use lapin::{
    options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

const MESSAGE_COUNT: i32 = 50_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    publish_messages_individually().await?;
    publish_messages_in_batch().await?;

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
        .confirm_select(ConfirmSelectOptions::default())
        .await;

    let now = SystemTime::now();

    for i in 0..MESSAGE_COUNT {
        if i % 100 == 0 {
            print!("\r{}", i);
            io::stdout().flush().unwrap();
        }
        let body = i.to_le_bytes().to_vec();
        let _ = channel.basic_publish(
            "",
            queue_name,
            BasicPublishOptions::default(),
            body,
            BasicProperties::default(),
        );
        let _ = channel.wait_for_confirms().await?;
    }

    let elapsed = now.elapsed().unwrap();
    println!(
        "\rPublished {} messages individually in {} ms",
        MESSAGE_COUNT,
        elapsed.as_millis()
    );

    Ok(())
}

async fn publish_messages_in_batch() -> Result<(), Box<dyn Error>> {
    let conn = create_connection().await?;
    let channel = conn.create_channel().await?;
    let queue_name = "publish_messages_in_batch";

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
        .confirm_select(ConfirmSelectOptions::default())
        .await;

    let batch_size = 100;
    let mut outstanding_message_count = 0;

    let now = SystemTime::now();

    for i in 0..MESSAGE_COUNT {
        let body = i.to_le_bytes().to_vec();
        let _ = channel.basic_publish(
            "",
            queue_name,
            BasicPublishOptions::default(),
            body,
            BasicProperties::default(),
        );
        outstanding_message_count += 1;

        if outstanding_message_count == batch_size {
            let _ = channel.wait_for_confirms().await?;
            outstanding_message_count = 0;
        }
    }
    if outstanding_message_count > 0 {
        let _ = channel.wait_for_confirms().await?;
    }

    let elapsed = now.elapsed().unwrap();
    println!(
        "Published {} messages in batch in {} ms",
        MESSAGE_COUNT,
        elapsed.as_millis()
    );

    Ok(())
}
