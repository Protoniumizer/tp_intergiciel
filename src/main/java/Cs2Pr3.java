import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;
import org.json.XML;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class Cs2Pr3 {

    public static void main(String[] args) {
        Producer producer = ProducerCreator.createProducer();
        Consumer consumer = ConsumerCreator.createConsumer("Topic2");
        int noMessageToFetch = 0;
        do {
            while (true) {
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000L);
                if (consumerRecords.count() == 0) {
                    ++noMessageToFetch;
                    break;
                }
                consumerRecords.forEach((record) -> {
                    send(producer, "Topic3", executeCommande(record.value().toString()));
                });
                consumer.commitAsync();
            }
        } while (noMessageToFetch <= IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT);
        consumer.close();
    }

    static String executeCommande(String command){
        System.out.println("Commande recue : " +command);
        String sqlData = "";
        String returnString = "";
        int avg = 0;
        List<Integer> intList;
        String[] splited = command.split("\\s+");
        switch (splited[0]) {

            case "Get_global_values":
                sqlData = getBddValues(command);
                break;

            case "Get_country_values":
                sqlData = getBddValues(command);
                intList = JsonPath.read(sqlData, "$.[*].TotalConfirmed");
                for (int i = 0; i<intList.size(); i++) {
                    if(JsonPath.read(sqlData, "$.["+i+"].CountryCode").equals(splited[1])){
                        returnString = JsonPath.read(sqlData, "$.["+i+"]").toString();
                    }
                }
                break;

            case "Get_confirmed_avg":
                sqlData = getBddValues(command);
                avg = 0;
                intList = JsonPath.read(sqlData, "$.[*].TotalConfirmed");
                for (int i:intList) {
                    avg+=i;
                }
                avg=avg/intList.size();
                returnString =  "Nombre moyen de cas confirmés : "+avg;
                break;

            case "Get_deaths_avg":
                sqlData = getBddValues(command);
                avg = 0;
                intList = JsonPath.read(sqlData, "$.[*].TotalDeaths");
                for (int i:intList) {
                    avg+=i;
                }
                avg=avg/intList.size();
                returnString =  "Nombre moyen de morts : "+avg;
                break;

            case "Get_countries_deaths_percent":
                sqlData = getBddValues(command);
                int nbD = 0;
                int nbC = 0;
                double percent = 0;
                intList = JsonPath.read(sqlData, "$.[*].TotalDeaths");
                for(int i=0; i< intList.size(); i++){
                    nbD = JsonPath.read(sqlData, "$.["+i+"].TotalDeaths");
                    nbC = JsonPath.read(sqlData, "$.["+i+"].TotalConfirmed");
                    percent = (double) Math.round((nbD*100)/(nbC+0.00001)* 100) / 100; //calcul du pourcentage qui est ensuite arrondi au centieme superieur
                    returnString += "Pourcentage pour le pays "+ JsonPath.read(sqlData, "$.["+i+"].Country") + " : "+percent+"%\n";
                }
                break;

            case "Export":
                sqlData = getBddValues(command);
                System.out.println(sqlData);
                BufferedWriter writer = null;
                try {
                    LocalTime time = LocalTime.now(); // Gets the current time
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HHmmss");
                    writer = new BufferedWriter(new FileWriter("Export"+time.format(formatter)+".xml"));

                    JSONObject obj = new JSONObject(sqlData);
                    String xml_data = XML.toString(obj);
                    writer.write(xml_data);
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                break;
        }
    return returnString;
    }

    public static String getBddValues(String command){
        String SQL="";
        String result="";
        String[] splited = command.split("\\s+");
        switch (splited[0]) {

            case "Get_global_values":
                SQL = "select (data->'Global') from covid19.global;";
                break;

            case "Get_country_values":
                SQL = "select (data->'Countries') from covid19.global;";
                break;

            case "Get_confirmed_avg":
                SQL = "select (data->'Countries') from covid19.global;";
                break;

            case "Get_deaths_avg":
                SQL = "select (data->'Countries') from covid19.global;";
                break;

            case "Get_countries_deaths_percent":
                SQL = "select (data->'Countries') from covid19.global;";
                break;

            case "Export":
                SQL = "select data from covid19.global";
                break;
        }


        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(SQL,
                     ResultSet.TYPE_SCROLL_SENSITIVE,
                     ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = ps.executeQuery();
            rs.last();
            result = rs.getString(1);
            rs.close();
        }catch(Exception e ) {
            System.out.println(e);
        }

        return result;
    }

    public static Connection connect() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(IPostGresConstants.URL, IPostGresConstants.USER, IPostGresConstants.PASSWORD);
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
