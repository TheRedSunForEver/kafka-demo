package org.x.kafkastudy.simpledemo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClientDemo {

    public static void main(String[] args) {
        new AdminClientDemo().listTopic();
    }

    public void listTopic() {
        AdminClient adminClient = createAdminClient();
        ListTopicsResult result = adminClient.listTopics();
        try {
            System.out.println(result.names().get());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public AdminClient createAdminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        return AdminClient.create(properties);
    }
}
