use std::convert::TryInto;

use anyhow::bail;
use futures_lite::StreamExt;
use lapin::{
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, Queue,
};
use uuid::Uuid;

struct FibonacciRpcClient {
    callback_queue: Queue,
    channel: Channel,
    consumer: lapin::Consumer,
    correlation_id: String,
}

impl FibonacciRpcClient {
    async fn new() -> Result<Self, lapin::Error> {
        let addr = "amqp://127.0.0.1:5672/%2f";
        let connection = Connection::connect(&addr, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        let callback_queue = channel
            .queue_declare(
                "",
                QueueDeclareOptions {
                    exclusive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let consumer = channel
            .basic_consume(
                callback_queue.name().as_str(),
                "rpc_client",
                BasicConsumeOptions {
                    no_ack: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let correlation_id = Uuid::new_v4().to_string();
        println!("Correlation id: {:?}", correlation_id);

        Ok(Self {
            callback_queue,
            channel,
            consumer,
            correlation_id,
        })
    }

    async fn call(&mut self, num: i32) -> Result<i32, anyhow::Error> {
        self.channel
            .basic_publish(
                "",
                "rpc_queue",
                BasicPublishOptions::default(),
                num.to_le_bytes().to_vec(),
                BasicProperties::default()
                    .with_reply_to(self.callback_queue.name().clone())
                    .with_correlation_id(self.correlation_id.clone().into()),
            )
            .await?
            .await?;

        while let Some(delivery) = self.consumer.next().await {
            let (_, reply) = delivery?;
            if reply.properties.correlation_id().as_ref()
                == Some(&self.correlation_id.clone().into())
            {
                return Ok(i32::from_le_bytes(reply.data.as_slice().try_into()?)
                    .try_into()
                    .unwrap());
            }
        }

        bail!("No reply")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 1 {
        panic!("You should provide exactly one number as argument");
    }
    let number: i32 = args[0].parse()?;

    println!("Connected");

    let mut client = FibonacciRpcClient::new().await?;

    let res = client.call(number).await?;

    println!("{:?}", res);

    Ok(())
}
