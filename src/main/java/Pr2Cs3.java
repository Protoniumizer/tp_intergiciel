import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Scanner;

public class Pr2Cs3 {

    public static void main(String[] args) {
        Producer producer = ProducerCreator.createProducer();
        Consumer consumer = ConsumerCreator.createConsumer("Topic3");
        Scanner scanner = new Scanner(System.in);


        System.out.println("Bienvenue dans le programme de requêtage de données Covid19.");
        System.out.println("Saisissez une commande à exécuter (faites \"Help\" pour connaître les commandes possibles) :");

        String saisie = scanner.next();
        boolean commandRecognized = false;
        while(true){
        while(!commandRecognized){

            switch (saisie){
                case "Get_global_values":
                    commandRecognized = true;
                    send(producer, "Topic2", saisie);
                    printOutput(consumer);
                    break;

                case "Get_country_values":
                    if(scanner.hasNext()){
                        commandRecognized = true;
                        send(producer, "Topic2", saisie+" "+scanner.next());
                        printOutput(consumer);
                    }
                    else{
                        System.out.println("Merci de saisir un code pays.");
                        commandRecognized = false;
                    }
                    break;

                case "Get_confirmed_avg":
                    commandRecognized = true;
                    send(producer, "Topic2", saisie);
                    printOutput(consumer);
                    break;

                case "Get_deaths_avg":
                    commandRecognized = true;
                    send(producer, "Topic2", saisie);
                    printOutput(consumer);
                    break;

                case "Get_countries_deaths_percent":
                    commandRecognized = true;
                    send(producer, "Topic2", saisie);
                    printOutput(consumer);
                    break;

                case "Export":
                    commandRecognized = true;
                    send(producer, "Topic2", saisie);
                    printOutput(consumer);
                    break;

                case "Quit":
                    commandRecognized = true;
                    System.out.println("Merci d'avoir utilisé notre service.");
                    System.out.println("Bonne journée !");
                    System.exit(0);
                    break;

                case "Help":
                    commandRecognized = false;
                    System.out.println("Get_global_values permet de retourner les valeurs globales du covid");
                    System.out.println("Get_country_values v_pays permet de retourner les valeurs du pays demandé ou v_pays est le code pays du pays demandé (ex : France = FR)");
                    System.out.println("Get_confirmed_avg permet de retourner une moyenne des cas confirmés sum(pays)/nb(pays))");
                    System.out.println("Get_deaths_avg permet de retourner une moyenne des Décès sum(pays)/nb(pays))");
                    System.out.println("Get_countries_deaths_percent permet de retourner le pourcentage de Décès par rapport aux cas confirmés)");
                    System.out.println("Export permet d’exporter les données de la base de données en XML dans un fichier");
                    break;

                default:
                    System.out.println("Commande inconnue.");

            }
            System.out.println("Saisissez une commande à exécuter (faites \"Help\" pour connaître les commandes possibles) :");
            saisie = scanner.next();
        }
            commandRecognized = false;
        }
    }



    static void send(Producer producer, String topic, String toSend){
        ProducerRecord record = new ProducerRecord(topic, toSend);
        try {
            RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
            System.out.println("Enregistrement envoyé : " + toSend + " vers la partition " + metadata.partition() + " Et l'offset " + metadata.offset());
        } catch (Exception e) {
            System.out.println("Erreur dans l'envoi de l'enregistrement");
            System.out.println(e);
        }
    }

    static void printOutput(Consumer consumer){
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000L);
            while (consumerRecords.count() == 0) {
                consumerRecords = consumer.poll(1000L);
                }
                consumerRecords.forEach((record) -> {
                    System.out.println("Retour de la requête :");
                    System.out.println(record.value().toString());
                });
                consumer.commitAsync();
    }
}
