/// Kafka TestContainer Wrapper
///
/// Using images with envs:
/// 1. bitnami/zookeeper:3.7.0:
///    ZOOKEEPER_CLIENT_PORT: 2181
///    ZOOKEEPER_TICK_TIME: 2000
///    ALLOW_PLAINTEXT_LISTENER: yes
///    ALLOW_ANONYMOUS_LOGIN: yes
///
/// 2. bitnami/kafka:2.8.0
///    ALLOW_PLAINTEXT_LISTENER: yes
///    KAFKA_BROKER_ID: 1
///    KAFKA_CFG_ZOOKEEPER_CONNECT: {zk_connect}(take from zookeper container)
///    KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://{kafka_exp_ip}:9092 (calc next after zookeper ip)
///    KAFKA_CFG_LISTENERS: PLAINTEXT://{kafka_exp_ip}:9092 (calc next after zookeper ip)
///
/// after create check equal real kafka container ip and expectation ip
///
/// 3. provectuslabs/kafka-ui:latest (optional)
///    KAFKA_CLUSTERS_0_NAME: local
///    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: {kafka_addr} (take from kafka container)
///
///    mapped ports 8080:8080
///
/// For createing topics using rdkafka
///
/// # Example
///
/// ```rust
/// #[cfg(test)]
/// mod test {
///     use std::{time::Duration};
/// 
///     use rdkafka::Message;
/// 
///     use crate::kafka_test_wrapper::{self, CreateTopic};
/// 
///     #[tokio::test]
///     async fn test_kafka() {
///         let options = kafka_test_wrapper::KafkaTestContainersOptions{
///             is_kafka_ui_need: true, 
///             wait_timeout_before_check_container: Duration::from_secs(1),};
///         //let options = kafka_test_wrapper::KafkaTestContainersOptions::default();
///         
///         let kafka_wrapper = kafka_test_wrapper::KafkaTestContainers::init_containers(options).await;
///         kafka_wrapper.create_new_topics(&[CreateTopic::new("input_topic", 4), CreateTopic::new("output_topic", 4)]).await;
///         let msgs = vec![kafka_test_wrapper::MessageToSend{key: "1", msg: "message 1"}, kafka_test_wrapper::MessageToSend{key: "2", msg: "message 2"}];
///         kafka_wrapper.send_data_to_topic("input_topic", &msgs).await.iter().for_each(|r|{
///             if let Err(e) = r {
///                 panic!("{e}");
///             };
///         });
///         kafka_wrapper.get_msgs_handle_one_by_one("input_topic", 2, Duration::from_secs(10), |msg| async move {
///             let key = String::from_utf8_lossy(msg.key().unwrap());
///             let value = String::from_utf8_lossy(msg.payload().unwrap());
///             assert!(!key.is_empty());
///             assert!(!value.is_empty());
///         }).await;
///     }
/// }
/// ```
mod kafka_test_wrapper{
    use std::{net::{IpAddr, Ipv4Addr}, panic, time::Duration};

    use rdkafka::{admin::{AdminClient, AdminOptions, NewTopic, TopicReplication}, client::DefaultClientContext, consumer::{CommitMode, Consumer, StreamConsumer}, message::OwnedMessage, producer::{FutureProducer, FutureRecord}, ClientConfig};
    use testcontainers::{runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt};
    use tokio::time::{self, sleep, Instant};
    use uuid::Uuid;

    /// Struct with data for topic create.
    pub struct CreateTopic<'a> {
        /// Name of topic
        pub topic_name : &'a str,
        /// Partition count
        pub partition_count: i32,
    }

    impl<'a> CreateTopic<'a> {
        pub fn new(topic_name: &'a str, partition_count: i32) -> Self {
            Self { topic_name, partition_count }
        }
    }

    /// Struct with data for send to topic.
    pub struct MessageToSend<'a> {
        /// message key
        pub key: &'a str,
        /// message payload
        pub message: &'a str,
    }

    /// Options for init kafka containers
    pub struct KafkaTestContainersOptions{
        /// Wait time before check is container active. In `default()` is `1` sec.
        pub wait_before_check: Duration,
        /// Is need to up kafka-ui container. In `default()` is `false`.
        pub is_kafka_ui_need: bool,
    }

    impl Default for KafkaTestContainersOptions{
        fn default() -> Self {
            Self { wait_before_check: Duration::from_secs(1), is_kafka_ui_need: false }
        }
    }

    pub struct KafkaTestContainers {
        /// Need save, overwise rust will drop after create.
        _zookeeper_container: ContainerAsync<GenericImage>,
        /// Need save, overwise rust will drop after create.
        _kafka_container: ContainerAsync<GenericImage>,
        /// Need save, overwise rust will drop after create.
        _kafka_ui_container: Option<ContainerAsync<GenericImage>>,
        /// kafka container port for using in tests
        pub kafka_addr: String
    }

    impl KafkaTestContainers{
        /// Initialize zookeper, kafka, kafka-ui(optional) container in network with random uuid.
        pub async fn init_containers(options: KafkaTestContainersOptions) -> KafkaTestContainers {
            let network = Uuid::new_v4().to_string();
            let zookeeper_container = GenericImage::new("bitnami/zookeeper", "3.7.0")
                .with_network(&network)
                .with_env_var("ZOOKEEPER_CLIENT_PORT", "2181")
                .with_env_var("ZOOKEEPER_TICK_TIME", "2000")
                .with_env_var("ALLOW_PLAINTEXT_LISTENER", "yes")
                .with_env_var("ALLOW_ANONYMOUS_LOGIN", "yes")
                .start()
                .await
                .unwrap();
            sleep(options.wait_before_check).await;
            let is_running = zookeeper_container.is_running().await.unwrap();
            assert!(is_running);

            let zk_ip = zookeeper_container.get_bridge_ip_address().await.unwrap();
            let zk_connect = format!("{zk_ip}:2181");
            let zk_ip_bytes = Self::ip_to_bytes(zk_ip);
            let kafka_exp_ip = IpAddr::V4(Ipv4Addr::new(zk_ip_bytes[0], zk_ip_bytes[1], zk_ip_bytes[2], zk_ip_bytes[3]+1));

            let kafka_container = GenericImage::new("bitnami/kafka", "2.8.0")
                .with_network(&network)
                .with_env_var("ALLOW_PLAINTEXT_LISTENER", "yes")
                .with_env_var("KAFKA_BROKER_ID", "1")
                .with_env_var("KAFKA_CFG_ZOOKEEPER_CONNECT", zk_connect)
                .with_env_var("KAFKA_CFG_ADVERTISED_LISTENERS", format!("PLAINTEXT://{kafka_exp_ip}:9092"))
                .with_env_var("KAFKA_CFG_LISTENERS", format!("PLAINTEXT://{kafka_exp_ip}:9092"))
                .start()
                .await
                .unwrap();
            sleep(options.wait_before_check).await;
            let is_running = kafka_container.is_running().await.unwrap();
            assert!(is_running);

            let kafka_ip = kafka_container.get_bridge_ip_address().await.unwrap();
            assert_eq!(kafka_exp_ip, kafka_ip);
            let kafka_addr = format!("{kafka_ip}:9092");

            let mut kafka_ui_container = None;
            if options.is_kafka_ui_need {
                let kui = GenericImage::new("provectuslabs/kafka-ui","latest")
                    .with_mapped_port(8080, 8080.into())
                    .with_network(&network)
                    .with_env_var("KAFKA_CLUSTERS_0_NAME", "local")
                    .with_env_var("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS", &kafka_addr)
                    .start().await.unwrap();
                sleep(options.wait_before_check).await;
                let is_running = kui.is_running().await.unwrap();
                assert!(is_running);
                kafka_ui_container = Some(kui);
            }

            KafkaTestContainers{ _zookeeper_container: zookeeper_container, _kafka_container: kafka_container, kafka_addr: kafka_addr, _kafka_ui_container: kafka_ui_container }
        }

        /// Create new topic with rdkafka
        pub async fn create_new_topics(&self, create_topics: &[CreateTopic<'_>]){
            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", &self.kafka_addr);
            let admin: AdminClient<DefaultClientContext> = config.create().unwrap();
            let new_topics = create_topics.iter().map(|t| NewTopic::new(t.topic_name, t.partition_count, TopicReplication::Fixed(1))).collect::<Vec<_>>();
            admin.create_topics(&new_topics, &AdminOptions::new()).await.unwrap();
        }

        /// Send async messages to kafka topic and `panic` on `KafkaError` message
        pub async fn send_data_to_topic(&self, topic: &str, msgs: &[MessageToSend<'_>]) {
            let producer: &FutureProducer = &ClientConfig::new()
                .set("bootstrap.servers", &self.kafka_addr).create().unwrap();
            for f in msgs.iter().map(|msg| async move {
                producer.send(FutureRecord::to(topic)
                    .payload(msg.message)
                    .key(msg.key),Duration::from_secs(0)).await}){
                match f.await {
                    Ok(_) => {},
                    Err((e,_)) => panic!("{e}"),
                }
            }
        }

        /// Get message from earliest in topic and Handle message by message.
        /// If count lower than `expected_count` or message is not in the topic at `required_time`
        /// then fn panics.
        pub async fn handle_msgs_from_topic<H, Fut>(&self, topic: &str, expected_count: i32, required_time: Duration, handler: H)
    where
            H: Fn(OwnedMessage) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = ()> + Send + 'static
        {
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.kafka_addr)
                .set("group.id", format!("test_reader_{}",Uuid::new_v4()))
                .set("enable.partition.eof", "false")
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .create().unwrap();
            consumer.subscribe(&[topic]).unwrap();
            let start = Instant::now();
            for _ in 0..expected_count{
                let wait_duration = required_time.checked_sub(start.elapsed()).unwrap();
                if wait_duration.is_zero() {
                    panic!("total timeout out");
                }

                match time::timeout(wait_duration, consumer.recv()).await {
                    Ok(rm) =>{
                        match rm {
                            Ok(m) => {
                                handler(m.detach()).await;
                                consumer.commit_message(&m, CommitMode::Async).unwrap();
                            },
                            Err(e) => panic!("{e}"),
                        }
                    },
                    Err(e) => panic!("{e}"),
                }
            }
        }

        fn ip_to_bytes(ip: IpAddr) -> [u8;4] {
            match ip {
                IpAddr::V4(v4) => v4.octets(),        // [u8;4] -> Vec<u8>
                _ => panic!("incorrect ip")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{time::Duration};

    use rdkafka::Message;

    use crate::kafka_test_wrapper::{self, CreateTopic};

    #[tokio::test]
    async fn test_kafka_wrapper_standart_using() {
        let kafka_wrapper = kafka_test_wrapper::KafkaTestContainers::init_containers(kafka_test_wrapper::KafkaTestContainersOptions::default()).await;
        kafka_wrapper.create_new_topics(&[CreateTopic::new("test_topic", 4), ]).await;
        let msgs = vec![kafka_test_wrapper::MessageToSend{key: "1", message: "message 1"}, kafka_test_wrapper::MessageToSend{key: "2", message: "message 2"}];
        kafka_wrapper.send_data_to_topic("test_topic", &msgs).await;
        kafka_wrapper.handle_msgs_from_topic("test_topic", 2, Duration::from_secs(10), |msg| async move {
            let key = String::from_utf8_lossy(msg.key().unwrap());
            let value = String::from_utf8_lossy(msg.payload().unwrap());
            assert!(!key.is_empty());
            assert!(!value.is_empty());
        }).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_kafka_wrapper_should_panic_incorrect_topic() {
        let kafka_wrapper = kafka_test_wrapper::KafkaTestContainers::init_containers(kafka_test_wrapper::KafkaTestContainersOptions::default()).await;
        kafka_wrapper.create_new_topics(&[CreateTopic::new("test_topic", 4),]).await;
        let msgs = vec![kafka_test_wrapper::MessageToSend{key: "1", message: "message 1"}, kafka_test_wrapper::MessageToSend{key: "2", message: "message 2"}];
        kafka_wrapper.send_data_to_topic("test_topic_2", &msgs).await;
        kafka_wrapper.handle_msgs_from_topic("test_topic", 2, Duration::from_secs(5), |msg| async move {
            let key = String::from_utf8_lossy(msg.key().unwrap());
            let value = String::from_utf8_lossy(msg.payload().unwrap());
            assert!(!key.is_empty());
            assert!(!value.is_empty());
        }).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_kafka_wrapper_should_panic_not_message_timeout() {
        let kafka_wrapper = kafka_test_wrapper::KafkaTestContainers::init_containers(kafka_test_wrapper::KafkaTestContainersOptions::default()).await;
        kafka_wrapper.create_new_topics(&[CreateTopic::new("test_topic", 4), ]).await;
        kafka_wrapper.handle_msgs_from_topic("test_topic", 2, Duration::from_secs(5), |msg| async move {
            let key = String::from_utf8_lossy(msg.key().unwrap());
            let value = String::from_utf8_lossy(msg.payload().unwrap());
            assert!(!key.is_empty());
            assert!(!value.is_empty());
        }).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_kafka_wrapper_should_panic_not_all_expect_message_return() {
        let kafka_wrapper = kafka_test_wrapper::KafkaTestContainers::init_containers(kafka_test_wrapper::KafkaTestContainersOptions::default()).await;
        kafka_wrapper.create_new_topics(&[CreateTopic::new("test_topic", 4)]).await;
        let msgs = vec![kafka_test_wrapper::MessageToSend{key: "1", message: "message 1"}, kafka_test_wrapper::MessageToSend{key: "2", message: "message 2"}];
        kafka_wrapper.send_data_to_topic("test_topic", &msgs).await;
        kafka_wrapper.handle_msgs_from_topic("test_topic", 3, Duration::from_secs(5), |msg| async move {
            let key = String::from_utf8_lossy(msg.key().unwrap());
            let value = String::from_utf8_lossy(msg.payload().unwrap());
            assert!(!key.is_empty());
            assert!(!value.is_empty());
        }).await;
    }
}
