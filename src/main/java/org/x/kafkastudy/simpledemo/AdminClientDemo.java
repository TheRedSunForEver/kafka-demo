package org.x.kafkastudy.simpledemo;

import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClientDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "xx_tp";
        AdminClientDemo demo = new AdminClientDemo();
//        demo.listTopic();
//        demo.createTopic("xx_tp");
//        demo.listTopic();
        demo.describeDemo(topicName);
    }

    public void listTopic() {
        AdminClient adminClient = createAdminClient();
        ListTopicsResult topics = adminClient.listTopics();
        try {
            topics.names().get().forEach(System.out::println);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void createTopic(String topicName) throws ExecutionException, InterruptedException {
        AdminClient adminClient = createAdminClient();
        CreateTopicsResult newTopic = adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short)1)));
        if (newTopic.numPartitions(topicName).get() != 1) {
            System.out.println("failed.");
        }
    }

    private void describeDemo(String topicName) throws ExecutionException, InterruptedException {
        AdminClient adminClient = createAdminClient();
        DescribeTopicsResult demoTopic = adminClient.describeTopics(Collections.singletonList(topicName));
        TopicDescription topicDescription = demoTopic.topicNameValues().get(topicName).get();
        System.out.println(topicDescription);

    }

    public AdminClient createAdminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty("request.timeout.ms", "20000");
        return AdminClient.create(properties);
    }
}
