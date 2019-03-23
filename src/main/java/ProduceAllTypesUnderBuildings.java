import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;

public class ProduceAllTypesUnderBuildings {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private static Connection yagoConnection;

    public static Properties PROPERTIES;

    private HashSet<String> childrenOfBuilding = new HashSet<>();

    private ProduceAllTypesUnderBuildings(){
        if (PROPERTIES == null) {
            try {
                PROPERTIES = new Properties();
                //Load properties file
                PROPERTIES.load(new InputStreamReader(new FileInputStream("./src/main/resources/config.properties"), "UTF8"));
            } catch (IOException exception) {
                return;
            }
        }

        try {
            yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                    PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));
        } catch (SQLException ex) {
            logger.error("Error failed!");
        }

    }

    private void processResults(ResultSet rs) throws SQLException{
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            PreparedStatement stmt;
            ResultSet new_rs;
            String query_yagotype = "select * from yagotaxonomy where object = ?;";;
            stmt = yagoConnection.prepareStatement(query_yagotype);

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                childrenOfBuilding.add(subject);

                if (!PROPERTIES.getProperty("debugLocally").equals("true")) {
                    stmt.setString(1, subject);
                    new_rs = stmt.executeQuery();
                    processResults(new_rs);
                }
            }
        }
        logger.info("Finished finding types under building!");
    }

    private static boolean isValidObject(String typeInfo) {
        return (typeInfo != null &&
                !typeInfo.contains("wikicat_Abbreviations")
                && !typeInfo.contains("wordnet_first_name")
                && !typeInfo.contains("wordnet_surname")
        );
    }

    private void startWorking(){
        PreparedStatement stmt;
        ResultSet rs;
        String query_yagotype;

        try {
            query_yagotype = "select * from yagotaxonomy where object = ? ;";

            stmt = yagoConnection.prepareStatement(query_yagotype);
            stmt.setString(1, "<wordnet_building_102913152>");
            rs = stmt.executeQuery();
            //mark non-leaf nodes
            processResults(rs);

            //for each of these type, find its entities in yagotype
            query_yagotype = "select * from yagotypes where object = ?;";
            stmt = yagoConnection.prepareStatement(query_yagotype);
            ResultSet new_rs;
            HashSet<String> entitiesOfBuilding = new HashSet<>();
            for (String type: childrenOfBuilding) {
                stmt.setString(1, type);
                new_rs = stmt.executeQuery();
                while (new_rs.next()) {
                    String subject = new_rs.getString("subject");
                    String object = new_rs.getString("object");
                    String predicate = new_rs.getString("predicate");

                    if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                        entitiesOfBuilding.add(subject);
                    }
                }
            }
            rs.close();
            stmt.close();

            childrenOfBuilding.addAll(entitiesOfBuilding);

        } catch (SQLException ex) {
            logger.info("SQL exceptions when running selections!");
            ex.printStackTrace();
        }

        IOUtilities.writeHashSetToFile(childrenOfBuilding, "output/allEntitiesOfBuilding.tsv");
    }

    public static void main(String[] args){
        try {
            // Database setup
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            return;
        }

        ProduceAllTypesUnderBuildings produceAllWNunderBuilding = new ProduceAllTypesUnderBuildings();
        produceAllWNunderBuilding.startWorking();
    }

}
