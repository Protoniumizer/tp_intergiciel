import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class Producer1 {

    private Producer1(){
        Properties kafkaProps=new Properties();
        kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        
        return KafkaProducer<String, String>(kafkaProps);
    }
}
