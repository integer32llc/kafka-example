use futures::{StreamExt, stream::FuturesUnordered};
use rdkafka::{ClientConfig, Message, admin::{AdminClient, AdminOptions, NewTopic, TopicReplication}, client::DefaultClientContext, consumer::{Consumer, StreamConsumer}, producer::{FutureProducer, FutureRecord}, util::Timeout, Offset, TopicPartitionList};
use std::{array, convert::TryInto, time::{Duration, SystemTime, UNIX_EPOCH}, sync::{Arc, Mutex}};
use tokio::{sync::oneshot, task::JoinHandle, time};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;

const TOPIC: &str = "my-topic2222";

#[tokio::main]
async fn main() -> Result<()> {
    let x = || {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", "localhost:9092");
        cfg
    };

    let admin_cfg = x();

    let mut producer_cfg = x();
    producer_cfg.set("message.timeout.ms", "5000");

    let mut consumer_cfg = x();
    consumer_cfg.set("session.timeout.ms", "6000");
    consumer_cfg.set("enable.auto.commit", "false");
    consumer_cfg.set("group.id", "placeholder");

    let admin: AdminClient<DefaultClientContext> = admin_cfg.create()?;
    let producer: FutureProducer = producer_cfg.create()?;
    let consumer: StreamConsumer = consumer_cfg.create()?;

    let topic = NewTopic::new(TOPIC, 1, TopicReplication::Fixed(1));
    let opts = AdminOptions::default();
    admin.create_topics(&[topic], &opts).await?;

    let seek = Arc::new(Mutex::new(None));


    eprintln!("Created");

    let consumer_seek = seek.clone();
    let consumer_task: JoinHandle<Result<()>> = tokio::spawn(async move {
        eprintln!("Consumer task starting");

        // This is redundant but shows it working
        // consumer.seek(TOPIC, 0, Offset::Beginning, Timeout::After(Duration::from_millis(100)))?;

        loop {
            let seek_val = {
                consumer_seek.lock().unwrap().clone()
            };
            match seek_val {
                Some(seek) => {
                    eprintln!("Seeking from {}", seek);

                    let mut topics = TopicPartitionList::new();
                    topics.add_partition(TOPIC, 0);
                    topics.set_partition_offset(TOPIC, 0, Offset::Offset(seek));
                    consumer.assign(&topics)?;

                    loop {
                        let p = consumer.recv().await?;
                        eprintln!("Received a {:?}", p.payload().map(String::from_utf8_lossy));
                    }
                }
                None => {
                    eprintln!("No consumer seek val yet");
                }

            }
        }
    });

    let producer_seek = seek.clone();
    let producer_task = tokio::spawn(async move {
        eprintln!("Producer task starting");
        for i in 0u128.. {
            let mut last_offset = 0;
            for j in 0..10 {
                let s = format!("hello! {} {}", i, j);
                let record = FutureRecord::to(TOPIC).key(&s).payload(&s).timestamp(now());
                match producer.send_result(record) {
                    Ok(x) => match x.await? {
                        Ok((_partition, offset)) => {
                            last_offset = offset;
                            eprintln!("Setting last offset to {} for message {}", last_offset, s);
                         },
                        Err((e, _msg)) => return Err(e.into()),
                    },
                    Err((e, _msg)) => return Err(e.into()),
                };
                eprintln!("Sent {}", i);
            }
            let mut seek = producer_seek.lock().unwrap();
            *seek = Some(last_offset);
        }
        eprintln!("exiting producer");
        Ok::<_, Error>(())
    });

    let mut tasks: FuturesUnordered<_> = array::IntoIter::new([consumer_task, producer_task]).collect();

    while let Some(t) = tasks.next().await {
        t??;
    }

    Ok(())
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
