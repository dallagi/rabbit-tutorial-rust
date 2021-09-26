use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let message = match args.len() {
        0 => "Some message".to_string(),
        _ => args.join(" ").as_str().to_string(),
    };

    let addr = "amqp://127.0.0.1:5672/%2f";
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;

    println!("Connected");

    let channel = conn.create_channel().await?;

    channel
        .exchange_declare(
            "logs",
            lapin::ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let _confirm = channel
        .basic_publish(
            "logs",
            "",
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
