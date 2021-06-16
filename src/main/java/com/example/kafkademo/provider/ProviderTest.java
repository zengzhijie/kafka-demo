package com.example.kafkademo.provider;

import com.example.kafkademo.config.KafkaConfig;
import com.example.kafkademo.constants.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
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
@RestController
@Component
public class ProviderTest {


    @Autowired
    private KafkaTemplate kafkaTemplate;

    @GetMapping("/sendKafka")
    public void provider() {
//        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(KafkaConfig.class);
//        KafkaTemplate<Integer, String> kafkaTemplate = (KafkaTemplate<Integer, String>) ctx.getBean("kafkaTemplate");
        String data = "this is a test message";
        ListenableFuture<SendResult<Integer, String>> send = kafkaTemplate.send(Constants.TOPIC_NAME, data);
        System.out.println("kafka发送消息成功："+data);
        send.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {

            }

            @Override
            public void onSuccess(SendResult<Integer, String> integerStringSendResult) {

            }
        });
    }
}
