package org.x.kafkastudy.simpledemo;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 1. 构造一个ProducerRecord对象。ProducerRecord的构造函数需要三个参数（topic名称, 消息的键, 消息的值)
 *    键和值的类型必须与键序列化器和值序列化器相对应。
 *
 * 2. 调用send()方法来发送ProducerRecord对象。消息会先被放进缓冲区，然后通过单独的线程发送给服务器。
 *    send()方法会返回一个包含RecordMetadata的Future对象。
 *
 */
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
