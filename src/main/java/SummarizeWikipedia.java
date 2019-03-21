import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.*;

public class SummarizeWikipedia {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private static Connection yagoConnection;

    private static HashMap<String, HashSet<String>> wikipediaToTypes = new HashMap<>();

    public static Properties PROPERTIES;

    private SummarizeWikipedia(){
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

    private static boolean isValidObject(String typeInfo) {
        return (typeInfo != null &&
                !typeInfo.contains("wikicat_Abbreviations")
                && !typeInfo.contains("wordnet_first_name")
                && !typeInfo.contains("wordnet_surname")
        );
    }

    private static void SummarizeWikipedia(ResultSet rs) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                if (wikipediaToTypes.containsKey(subject)) {
                    wikipediaToTypes.get(subject).add(object);
                } else {
                    HashSet<String> hypernyms = new HashSet<>();
                    hypernyms.add(object);
                    wikipediaToTypes.put(subject, hypernyms);
                }
            }
        }

    }


    private void startSummarization(){

        PreparedStatement stmt;

        try {
            if (PROPERTIES.getProperty("debugLocally").equals("true")) {
                String query_yagotype = "SELECT * FROM subset_yagotypes";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                SummarizeWikipedia(rs);
                rs.close();
                stmt.close();
            } else {
                String query_yagotype = "SELECT * FROM yagotypes";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                SummarizeWikipedia(rs);
                rs.close();
                stmt.close();
            }
        } catch (SQLException ex) {
            logger.info("SQL exceptions when running selections!");
        }
    }

    private void writeToFile(){
        IOUtilities.clearOutputfile("output/wikipedia2hypernyms.tsv");

        BufferedWriter bw;
        FileWriter fw;

        try {
            fw = new FileWriter("output/wikipedia2hypernyms.tsv");
            bw = new BufferedWriter(fw);

            for (String key: wikipediaToTypes.keySet()) {
                String content = key + "\t" + wikipediaToTypes.get(key).toString() + "\n";
                bw.write(content);
            }

            bw.close();

        } catch (IOException exception) {
            logger.error("Error: can't create file: output/wikipedia2hypernyms.tsv");
        }
    }


    public static void main(String[] args){
        try {
            // Database setup
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            return;
        }

        SummarizeWikipedia WikipediaSummarizer = new SummarizeWikipedia();
        WikipediaSummarizer.startSummarization();
        WikipediaSummarizer.writeToFile();
    }
}
