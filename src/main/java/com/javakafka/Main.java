package com.javakafka;

public class Main {
    public static void main(String[] args) throws Exception {
        KafkaProducerFactory producerFactory = new KafkaProducerFactory();

        for (int i = 0; i < 6; i++) {
            producerFactory.send("jav-kafka-test", "keys", "hihi" + i); // key 값은 파티션 분리에 따라 특정 파티션에 주기 위해 있음(아마)
        }



    }
}