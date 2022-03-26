//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerCreator {
    public ConsumerCreator() {
    }

    public static Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", IKafkaConstants.KAFKA_BROKERS);
        props.put("group.id", "consumerGroup10");
        props.put("key.deserializer", LongDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("max.poll.records", IKafkaConstants.MAX_POLL_RECORDS);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        Consumer<Long, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList("Topic1"));
        return consumer;
    }

    public static Consumer<Long, String> createConsumer(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", IKafkaConstants.KAFKA_BROKERS);
        props.put("group.id", "consumerGroup10");
        props.put("key.deserializer", LongDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("max.poll.records", IKafkaConstants.MAX_POLL_RECORDS);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        Consumer<Long, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
}