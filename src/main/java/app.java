import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.postgresql.util.PGobject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.Scanner;

public class app {
    public app() {
    }

    public static void main(String[] args) {

        CustomProducer p2 = new CustomProducer(ProducerCreator.createProducer(), 2);
        CustomProducer p3 = new CustomProducer(ProducerCreator.createProducer(), 3);

        CustomConsumer c1 = new CustomConsumer(ConsumerCreator.createConsumer("Topic1"), 1);
        CustomConsumer c2 = new CustomConsumer(ConsumerCreator.createConsumer("Topic2"), 2);
        CustomConsumer c3 = new CustomConsumer(ConsumerCreator.createConsumer("Topic3"), 3);

        c1.run();

        Scanner scanner = new Scanner(System.in);


        System.out.println("Bienvenue dans le programme de requêtage de données Covid19.");
        System.out.println("Saisissez une commande à exécuter (faites \"Help\" pour connaître les commandes possibles) :");

        String saisie = scanner.next();
        boolean commandRecognized = false;
        //while(true){
            while(!commandRecognized){
                switch (saisie){

                case "Get_global_values":
                    System.out.println("Get_global_values");
                    commandRecognized = true;
                    p2.send("Topic2", saisie);
                    System.out.println(getBddValues("Get_global_values"));
                    break;

                case "Get_country_values":
                    System.out.println("Get_country_values");
                    commandRecognized = true;
                    p2.send("Topic2", saisie);
                    break;

                case "Get_confirmed_avg":
                    System.out.println("Get_confirmed_avg");
                    commandRecognized = true;
                    p2.send("Topic2", saisie);
                    break;

                case "Get_deaths_avg":
                    System.out.println("Get_deaths_avg");
                    commandRecognized = true;
                    p2.send("Topic2", saisie);
                    break;

                case "Get_countries_deaths_percent":
                    System.out.println("Get_countries_deaths_percent");
                    commandRecognized = true;
                    p2.send("Topic2", saisie);
                    break;

                case "Export":
                    System.out.println("Export");
                    commandRecognized = true;
                    p2.send("Topic2", saisie);
                    break;

                case "Quit":
                    System.out.println("Quit");
                    commandRecognized = true;
                    System.out.println("Merci d'avoir utilisé notre service.");
                    System.out.println("Bonne journée !");
                    System.exit(0);
                    break;

                case "Help":
                    System.out.println("Help");
                    commandRecognized = false;
                    System.out.println("Get_global_values permet de ....");
                    System.out.println("Get_country_values permet de ....");
                    System.out.println("Saisissez une commande à exécuter :");
                    saisie = scanner.next();
                    break;

                default:
                    System.out.println("Commande inconnue.");
                    System.out.println("Saisissez une commande à exécuter (faites \"Help\" pour connaître les commandes possibles) :");
                    saisie = scanner.next();
            }
        }
    //}

        System.out.println("----------------------------");
        System.out.println("----------------------------");
        System.out.println("----------------------------");
        System.out.println("----------------------------");
        c1.run();

    }

    public static String getBddValues(String commande){
        String SQL="";
        String result="";
        switch (commande) {

            case "Get_global_values":
                SQL = "select (data->'Global') from covid19.global;";
                break;

            case "Get_country_values":
                SQL = "";
                break;

            case "Get_confirmed_avg":
                SQL = "";
                break;

            case "Get_deaths_avg":
                SQL = "";
                break;

            case "Get_countries_deaths_percent":
                SQL = "";
                break;

            case "Export":
                SQL = "";
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

}