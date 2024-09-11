package com.javakafka;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerFactory {
    private final KafkaProducer<String, String> producer; // 클로저 클래스로 원래 try 문이나 코드 마지막에 close 호출로 구현하는게 맞음

    public KafkaProducerFactory() throws UnknownHostException {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost().getHostName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 0, 1, all | 왼쪽에서 오른쪽 순으로 속도가 빠르고 반대 순으로는 메세지를 확인하는 깊이임

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<>(props);
    }

    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        Future<RecordMetadata> future = producer.send(record); // 유사 Promise?
        RecordMetadata rm = future.get();

        System.out.println(rm);

        producer.flush();
    }
}
