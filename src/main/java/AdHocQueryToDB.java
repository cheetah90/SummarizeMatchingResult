import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;

public class AdHocQueryToDB {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private static Connection yagoConnection;

    public static Properties PROPERTIES;

    private HashSet<String> childrenOfBuilding = new HashSet<>();

    private AdHocQueryToDB(){
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
            String query_yagotype = "select * from yagotaxonomy where object = ? and subject LIKE '<wordnet_%';";;
            stmt = yagoConnection.prepareStatement(query_yagotype);

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                if (subject.startsWith("<wordnet")) {
                    childrenOfBuilding.add(IOUtilities.extractWNID(subject));

                    stmt.setString(1, subject);
                    new_rs = stmt.executeQuery();
                    processResults(new_rs);
                }
            }
        }
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
            if (PROPERTIES.getProperty("debugLocally").equals("true")) {
                query_yagotype = "select * from yagotaxonomy where object = ? and subject LIKE '<wordnet_%';";
            } else {
                query_yagotype = "SELECT * FROM yagotaxonomy";
            }

            stmt = yagoConnection.prepareStatement(query_yagotype);
            stmt.setString(1, "<wordnet_building_102913152>");
            rs = stmt.executeQuery();
            //mark non-leaf nodes
            processResults(rs);
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            logger.info("SQL exceptions when running selections!");
        }

        IOUtilities.writeHashSetToFile(childrenOfBuilding, "output/childrenOfBuilding.tsv");
    }

    public static void main(String[] args){
        try {
            // Database setup
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            return;
        }

        AdHocQueryToDB adHocQueryToDB= new AdHocQueryToDB();
        adHocQueryToDB.startWorking();
    }
}
