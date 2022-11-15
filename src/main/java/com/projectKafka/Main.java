package com.projectKafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.sleep;


public class Main {

    public static void main(String[] args) {

        Producer<String, Product> producer = createProducer();

        ProducerRecord<String, Product> record = new ProducerRecord<>("new_test_topic","3key666", new Product("piwo", 5.05));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("Sent message " + recordMetadata.toString());
                }
            });


        try {
            sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public static Producer<String, Product> createProducer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OwnPartitioner.class);


        return new KafkaProducer<>(properties);

    }
}
