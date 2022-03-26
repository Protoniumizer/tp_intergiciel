//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import kafka.utils.json.JsonObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.postgresql.util.PGobject;

public class app {
    private static String url = "jdbc:postgresql://localhost/postgres?currentSchema=covid19";
    private static String user = "Covid19";
    private static String password = "Covid19";
    public app() {
    }

    public static void main(String[] args) {
        Producer<Long, String> producer1 = ProducerCreator.createProducer();
        Producer<Long, String> producer2 = ProducerCreator.createProducer();
        Producer<Long, String> producer3 = ProducerCreator.createProducer();
        Consumer<Long, String> consumer1 = ConsumerCreator.createConsumer("Topic1");
        Consumer<Long, String> consumer2 = ConsumerCreator.createConsumer("Topic2");
        Consumer<Long, String> consumer3 = ConsumerCreator.createConsumer("Topic3");

        producerSend(producer1, "Topic1", getApiDatas());
        System.out.println("----------------------------");
        System.out.println("----------------------------");
        System.out.println("----------------------------");
        System.out.println("----------------------------");
        runConsumer(consumer1);
    }

    public static Connection connect() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(url, user, password);
    }

    public static long insertGlobal(String json) {
        String SQL = "insert into covid19.global (data) values(?);";

        long id = 0;

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL,
                     Statement.RETURN_GENERATED_KEYS)) {

            PGobject jsonObject = new PGobject();
            jsonObject.setType("jsonb");
            jsonObject.setValue(json);
            pstmt.setObject(1, jsonObject);

            int affectedRows = pstmt.executeUpdate();
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        id = rs.getLong(1);
                    }
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return id;
    }

    static void runConsumer(Consumer consumer) {
        int noMessageToFetch = 0;

        do {
            while (true) {
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000L);
                if (consumerRecords.count() == 0) {
                    ++noMessageToFetch;
                    break;
                }

                consumerRecords.forEach((record) -> {
                    System.out.println("Record value " + record.value().toString());
                    insertGlobal(record.value().toString());


                    /*
                    String json = record.value().toString();
                    System.out.println(JsonPath.read(json, "$.Countries[*]").toString());
                    List<String> countries = JsonPath.read(json, "$.Countries[*]");
                    for (String s : countries
                         ) {
                        System.out.println("-----------------");
                        System.out.println(s);

                    }
                    */

                });
                consumer.commitAsync();
            }
        } while (noMessageToFetch <= IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT);

        consumer.close();
    }

    static void producerSend(Producer p, String topic, String toSend){
        ProducerRecord record = new ProducerRecord(topic, toSend);
        try {
            RecordMetadata metadata = (RecordMetadata) p.send(record).get();
            System.out.println("Enregistrement envoyé : " + toSend + " vers la partition " + metadata.partition() + " Et l'offset " + metadata.offset());
        } catch (Exception e) {
            System.out.println("Erreur dans l'envoi de l'enregistrement");
            System.out.println(e);
        }
    }

    public static String getApiDatas() {
        System.out.println("Coucou petite perruche");
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

}