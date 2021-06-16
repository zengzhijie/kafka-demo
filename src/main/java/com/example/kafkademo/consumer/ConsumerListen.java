package com.example.kafkademo.consumer;

import com.example.kafkademo.constants.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: kafka-demo
 * @description:
 * @author: jack
 * @create: 2021-06-16 17:03
 */
@Component
@KafkaListener(topics = Constants.TOPIC_NAME)
public class ConsumerListen {


    @Autowired
    private KafkaTemplate kafkaTemplate;


    @KafkaHandler
    public void consumer(String records) {
        System.out.println("kafka监听收到消息："+records);
    }


}
