import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.postgresql.util.PGobject;

import java.sql.*;

public class Cs1 {

    public static void main(String[] args) {
        Consumer consumer = ConsumerCreator.createConsumer("Topic1");
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
                });
                consumer.commitAsync();
            }
        } while (noMessageToFetch <= IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT);
        consumer.close();

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

    public static Connection connect() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(IPostGresConstants.URL, IPostGresConstants.USER, IPostGresConstants.PASSWORD);
    }
}
