use lapin::{BasicProperties, Connection, ConnectionProperties, options::{BasicPublishOptions, QueueDeclareOptions}, types::FieldTable};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://127.0.0.1:5672/%2f";
    let conn = Connection::connect(
        &addr,
        ConnectionProperties::default(),
    )
    .await?;

    println!("Connected");

    let channel = conn.create_channel().await?;
    let queue = channel.queue_declare("hello", QueueDeclareOptions::default(), FieldTable::default()).await?;

    println!("Declared queue {:?}", queue);

    let payload = b"Hello world!";
    let _confirm = channel
        .basic_publish(
            "",
            "hello",
            BasicPublishOptions::default(),
            payload.to_vec(),
            BasicProperties::default(),
        )
    .await?
    .await?;

    println!(" [x] Sent hello world!");

    let _ = conn.close(0, "");

    Ok(())
}
