package com.javakafka;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
    public static void main(String[] args) {
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getEnv())) { // 클로저 클래스이기에 try
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(200)); // 끌고오기
            for (ConsumerRecord<String, String> record : records) {
                String topic = record.topic();
                if ("jav-kafka-test".equals(topic)) { // 올바른 토픽을 끌고왔는지 체크
                    System.out.println(record.value());
                }else {
                    throw new IllegalStateException("Topic Interrupt : " + "jav-kafka-test");
                }
            }

        }catch (Exception e) {
            System.out.println("consumer was Interrupted");
            e.printStackTrace();
        }
    }

    private static Properties getEnv() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "jav-kafka-test");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }
}
