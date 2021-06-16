package com.example.kafkademo.config;

import com.example.kafkademo.constants.Constants;
import com.example.kafkademo.consumer.ConsumerListen;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: kafka-demo
 * @description:
 * @author: jack
 * @create: 2021-06-16 16:31
 */
@Configurable
@EnableKafka
public class KafkaConfig {

//    /**
//     * 1.配置topic
//     * @return
//     */
//    public KafkaAdmin admin() {
//        Map<String, Object> configs = new HashMap<String, Object>();
//        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.SERVER_URL);
//        return new KafkaAdmin(configs);
//    }
//
//    @Bean
//    public NewTopic topic1() {
//        return new NewTopic("foo", 10, (short) 2);
//    }
//
//
//    /**
//     *  配置生产者Factort及Template
//     * @return
//     */
//    @Bean
//    public ProducerFactory<Integer, String> producerFactory() {
//        return new DefaultKafkaProducerFactory<Integer,String>(producerConfigs());
//    }
//
//
//    @Bean
//    public Map<String, Object> producerConfigs() {
//        Map<String, Object> props = new HashMap<String,Object>();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.SERVER_URL);
//        props.put(ProducerConfig.ACKS_CONFIG, "all");
//        props.put(ProducerConfig.RETRIES_CONFIG, 0);
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        return props;
//    }
//    @Bean
//    public KafkaTemplate<Integer, String> kafkaTemplate() {
//        return new KafkaTemplate<Integer, String>(producerFactory());
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<Integer,String> kafkaListenerContainerFactory(){
//        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<Integer, String>();
//        factory.setConsumerFactory(consumerFactory());
//        return factory;
//    }
//
//    @Bean
//    public ConsumerFactory<Integer,String> consumerFactory(){
//        return new DefaultKafkaConsumerFactory<Integer, String>(consumerConfigs());
//    }
//
//
//    @Bean
//    public Map<String,Object> consumerConfigs(){
//        HashMap<String, Object> props = new HashMap<String, Object>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.SERVER_URL);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        return props;
//    }
//
//
//    /**
//     * 同时也需要将这个类作为一个Bean配置到KafkaConfig中
//     * 默认spring-kafka会为每一个监听方法创建一个线程来向kafka服务器拉取消息
//     * @return
//     */
//    @Bean
//    public ConsumerListen consumerListen(){
//        return new ConsumerListen();
//    }
}
