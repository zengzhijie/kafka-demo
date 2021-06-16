package com.example.kafkademo;

import com.example.kafkademo.constants.Constants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SpringBootTest
class KafkaDemoApplicationTests {


    @Test
    void contextLoads() throws InterruptedException {
        // 生产者示例
        providerDemo();
        // 消费者示例
        consumerDemo();

    }



    @Test
    public void createTopic() {
        //创建topic
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.SERVER_URL);
        AdminClient adminClient = AdminClient.create(props);
        ArrayList<NewTopic> topics = new ArrayList<NewTopic>();
        NewTopic newTopic = new NewTopic(Constants.TOPIC_NAME, 1, (short) 1);
        topics.add(newTopic);
        CreateTopicsResult result = adminClient.createTopics(topics);
        try {
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void providerDemo() {
        Properties properties = new Properties();
        /**
         * kafka的服务地址
         */
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.SERVER_URL);
        /**
         * 在考虑完成请求之前，生产者要求leader收到的确认数量。这可以控制发送记录的持久性。允许以下设置：
         * acks = 0，生产者将不会等待来自服务器的任何确认。该记录将立即添加到套接字缓冲区并视为已发送。在这种情况下，无法保证服务器已收到记录，并且retries配置将不会生效（因为客户端通常不会知道任何故障）。
         * acks = 1，这意味着leader会将记录写入其本地日志，但无需等待所有follower的完全确认即可做出回应。在这种情况下，如果leader在确认记录后立即失败但在关注者复制之前，则记录将丢失。
         * acks = all，这意味着leader将等待完整的同步副本集以确认记录。这保证了只要至少一个同步副本仍然存活，记录就不会丢失。这是最强有力的保证。这相当于acks = -1设置
         */
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        /**
         * 当从broker接收到的是临时可恢复的异常时，生产者会向broker重发消息，但是不能无限制重发，如果重发次数达到限制值，生产者将不会重试并返回错误。
         * 通过retries属性设置。默认情况下生产者会在重试后等待100ms，可以通过 retries.backoff.ms属性进行修改
         */
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        /**
         * 当有多条消息要被发送到同一分区时，生产者会把他们放到同一批里。kafka通过批次的概念来 提高吞吐量，但是也会在增加延迟。
         * 以下配置，当缓存数量达到16kb，就会触发网络请求，发送消息
         */
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        /**
         * 每条消息在缓存中的最长时间（单位ms），如果超过这个时间就会忽略batch.size的限制，由客户端立即将消息发送出去
         */
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        /**
         * Kafka的客户端发送数据到服务器，不是来一条就发一条，而是经过缓冲的，也就是说，通过KafkaProducer发送出去的消息都是先进入到客户端本地的内存缓冲里，然后把很多消息收集成一个一个的Batch，再发送到Broker上去的，这样性能才可能高。
         * buffer.memory的本质就是用来约束KafkaProducer能够使用的内存缓冲的大小的，默认值32MB。
         * 如果buffer.memory设置的太小，可能导致的问题是：消息快速的写入内存缓冲里，但Sender线程来不及把Request发送到Kafka服务器，会造成内存缓冲很快就被写满。而一旦被写满，就会阻塞用户线程，不让继续往Kafka写消息了。
         * 所以“buffer.memory”参数需要结合实际业务情况压测，需要测算在生产环境中用户线程会以每秒多少消息的频率来写入内存缓冲。经过压测，调试出来一个合理值。
         */
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        /**
         * key的序列化方式
         */
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        /**
         * value序列化方式
         */
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;

        producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            String msg = "------Message " + i;
            producer.send(new ProducerRecord<String, String>(Constants.TOPIC_NAME, msg));
            System.out.println("Sent:" + msg);
        }

        // String msg = "------Message hello world!";

        //  producer.send(new ProducerRecord<String, String>(TOPIC_NAME, msg));
        // System.out.println("Sent:" + msg);
        producer.close();


    }


    @Test
    public  void consumerDemo() throws InterruptedException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 每个消费者分配独立的组号
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        // 如果value合法，则自动提交偏移量
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 设置自动更新被消费消息的偏移量的时间间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // 设置服务返回的最大数据量，这不是绝对最大值，如果提取的第一个非空分区中的第一条消息大于此值，则仍将返回该消息以确保使用者使用。此处设置5MB
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "5242880");
        // 设置服务返回的每个分区的最大数据量，此大小必须至少与服务器允许的最大消息大小（fetch.max.bytes）一样大，否则，生产者有可能发送大于消费者可以获取的消息。此处设置5MB
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "5242880");
        /**
         * earliest，当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
         * latest，当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
         * none，topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // mytest 为topic
        consumer.subscribe(Arrays.asList(Constants.TOPIC_NAME), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                // 将偏移设置到最开始
                consumer.seekToBeginning(collection);
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("------------------offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println();
            }
            /**
             * 手动提交偏移量
             * 保证同一个consumer group中，下一次读取（不论进行了rebalance）时，既不会重复消费消息，也不会遗漏消息。
             * 防止consumer莫名挂掉后，下次进行数据fetch时，不能从上次读到的数据开始读而导致Consumer消费的数据丢失
             */
            consumer.commitSync();
            Thread.sleep(2000);
        }


    }


}
