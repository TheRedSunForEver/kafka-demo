package org.x.kafkastudy.simpledemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer extends Thread {
    private final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args) {
        SimpleConsumer consumer = new SimpleConsumer();
        consumer.start();
    }

    public Properties configure() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("group.id", "x1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void run() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configure())) {
            consumer.subscribe(Collections.singletonList("test_tp"));

            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
                for (ConsumerRecord<String, String> r : records) {
                    if (log.isInfoEnabled()) {
                        log.info("offset:{}, key:{}, value:{}", r.offset(), r.key(), r.value());
                    }
                }
            }
        }
    }
}
