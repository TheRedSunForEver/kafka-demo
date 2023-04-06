package org.x.kafkastudy.saslpliantextdemo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer extends Thread {
    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.start();
    }

    public Properties configure() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "1");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='admin-secret';");

        return props;
    }

    @Override
    public void run() {
        Producer<String, String> producer = new KafkaProducer<>(configure());
        for (int i = 0; i < 5; i++) {
            String key = "key_" + i;
            String message = "msg_" + i;
            producer.send(new ProducerRecord<>("test_tp", key, message), (metadata, e) -> {
                if (e != null) {
                    log.error("send message is excepted. ", e);
                } else {
                    log.info("The offset of the record is : {}", metadata.offset());
                }
            });
        }

        try {
            sleep(3000);
        } catch (InterruptedException e) {
            log.error("sleep is excepted. ", e);
            Thread.currentThread().interrupt();
        }

        producer.close();
    }
}
