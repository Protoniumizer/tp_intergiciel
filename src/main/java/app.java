//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class app {
    public app() {
    }

    public static void main(String[] args) {
        runProducer();
        runConsumer();
    }

    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
        int noMessageToFetch = 0;

        do {
            while(true) {
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000L);
                if (consumerRecords.count() == 0) {
                    ++noMessageToFetch;
                    break;
                }

                consumerRecords.forEach((record) -> {
                    System.out.println("Record Key " + record.key());
                    System.out.println("Record value " + (String)record.value());
                    System.out.println("Record partition " + record.partition());
                    System.out.println("Record offset " + record.offset());
                });
                consumer.commitAsync();
            }
        } while(noMessageToFetch <= IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT);

        consumer.close();
    }

    static void runProducer() {
        Producer<Long, String> producer = ProducerCreator.createProducer();

        for(int index = 0; index < IKafkaConstants.MESSAGE_COUNT; ++index) {
            ProducerRecord record = new ProducerRecord("HT-topic", "Enregistrement N° " + index);

            try {
                RecordMetadata metadata = (RecordMetadata)producer.send(record).get();
                System.out.println("Enregistrement envoyer avec clè " + index + " vers la partition " + metadata.partition() + " Et l'offset " + metadata.offset());
            } catch (ExecutionException var4) {
                System.out.println("Erreur dans l'envoi de l'enregistrement");
                System.out.println(var4);
            } catch (InterruptedException var5) {
                System.out.println("Erreur dans l'envoi de l'enregistrement");
                System.out.println(var5);
            }
        }

    }
}