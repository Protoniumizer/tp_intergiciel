import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class Pr1 {

    public static void main(String[] args) {
        while(true){
            send("Topic1", getApiDatas());
            try {
                Thread.sleep(1800000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getApiDatas() {
        try {
            URL url = new URL(IApiConstants.API_ADDRESS);//your url i.e fetch data from .
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP Error code : "
                        + conn.getResponseCode());
            }
            InputStreamReader in = new InputStreamReader(conn.getInputStream());
            BufferedReader br = new BufferedReader(in);

            String output = "";
            String json;
            while ((json = br.readLine()) != null) {
                output+=json;
            }
            return output;
        } catch (Exception e) {
            System.out.println("Exception in NetClientGet:- " + e);
            return null;
        }
    }


    static void send(String topic, String toSend){
        Producer producer = ProducerCreator.createProducer();
        ProducerRecord record = new ProducerRecord(topic, toSend);
        try {
            RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
            System.out.println("Enregistrement envoyé : " + toSend + " vers la partition " + metadata.partition() + " Et l'offset " + metadata.offset());
        } catch (Exception e) {
            System.out.println("Erreur dans l'envoi de l'enregistrement");
            System.out.println(e);
        }
    }
}
