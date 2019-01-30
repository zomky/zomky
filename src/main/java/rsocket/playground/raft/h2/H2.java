package rsocket.playground.raft.h2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rsocket.playground.raft.NodeData;

import java.sql.*;
import java.util.Optional;

public class H2 {

    // CREATE TABLE nodes(id INT PRIMARY KEY, current_term BIGINT, voted_for INT);
    // Insert into NODES (ID, CURRENT_TERM) Values (7000, 0);
    // Insert into NODES (ID, CURRENT_TERM) Values (7001, 0);
    // Insert into NODES (ID, CURRENT_TERM) Values (7002, 0);

    private static final Logger LOGGER = LoggerFactory.getLogger(H2.class);

    private static Connection connection() {
        String url = "jdbc:h2:tcp://localhost:9092/testdb";
        String user = "sa";
        String passwd = "";
        try {
            return DriverManager.getConnection(url, user, passwd);
        } catch (SQLException e) {
            throw new RuntimeException("h2 problem", e);
        }
    }

    public static Optional<NodeData> nodeDataFromDB(int nodeId) {
//        LOGGER.info("GET NODE from db , nodeId {}", nodeId);
        Statement stmt = null;
        String query = "select id, current_term, voted_for from NODES where id = " + nodeId;

        try {
            stmt = connection().createStatement();
            ResultSet rs = stmt.executeQuery(query);
            boolean next = rs.next();
            NodeData nodeData = null;
            if (next) {
                int id = rs.getInt("ID");
                Long currentTerm = rs.getLong("CURRENT_TERM");
                Integer voted = rs.getInt("VOTED_FOR");
                if (rs.wasNull()) {
                    voted = null;
                }
                nodeData = new NodeData().nodeId(id).currentTerm(currentTerm).votedFor(voted);
            }
            stmt.close();
//            LOGGER.info("GET NODE from db , nodeData {}", nodeData);
            return Optional.ofNullable(nodeData);
        } catch (SQLException e ) {
            throw new RuntimeException("h2 problem", e);
        }
    }

    public static void insertNode(int nodeId, long currentTerm) {
        String s = "Insert into NODES (ID, CURRENT_TERM) Values (?, ?)";
        try {
            PreparedStatement preparedStatement = connection().prepareStatement(s);
            preparedStatement.setInt(1, nodeId);
            preparedStatement.setLong(2, currentTerm);
            preparedStatement.execute();
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException("h2 problem", e);
        }
    }

    public static void updateTerm(int nodeId) {
        LOGGER.info("UPDATE TERM node {}", nodeId);
        String s = "UPDATE NODES set current_term = current_term + 1, voted_for = ? where id = ?";
        try {
            PreparedStatement preparedStatement = connection().prepareStatement(s);
            preparedStatement.setNull(1, java.sql.Types.INTEGER);
            preparedStatement.setInt(2, nodeId);
            preparedStatement.execute();
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException("h2 problem", e);
        }
    }

    public static void updateTerm(int nodeId, long currentTerm) {
        LOGGER.info("UPDATE TERM node {}, current term {}", nodeId, currentTerm);
        String s = "UPDATE NODES set current_term = ?, voted_for = ? where id = ?";
        try {
            PreparedStatement preparedStatement = connection().prepareStatement(s);
            preparedStatement.setLong(1, currentTerm);
            preparedStatement.setNull(2, java.sql.Types.INTEGER);
            preparedStatement.setInt(3, nodeId);
            preparedStatement.execute();
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException("h2 problem", e);
        }
    }

    public static void updateVotedFor(int nodeId, int votedFor) {
        LOGGER.info("UPDATE VOTED_FOR nodeId {}, voted {}", nodeId, votedFor);
        String s = "UPDATE NODES set voted_for = ? where id = ?";
        try {
            PreparedStatement preparedStatement = connection().prepareStatement(s);
            preparedStatement.setInt(1, votedFor);
            preparedStatement.setInt(2, nodeId);
            preparedStatement.execute();
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException("h2 problem", e);
        }
    }

}
