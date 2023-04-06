package org.x.kafkastudy.saslpliantextdemo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.config.SaslConfigs;

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
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='ke' password='ke';");
        return AdminClient.create(props);
    }
}
