import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;

public class FilterLeafWordNetNode {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private static HashSet<String> leafNode = new HashSet<>();

    private static HashSet<String> nonLeafNode = new HashSet<>();

    private static Connection yagoConnection;

    public static Properties PROPERTIES;

    private FilterLeafWordNetNode(){
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

    private boolean isWordNet(String line) {
        return line.startsWith("<wordnet");
    }

    private String extractWNID(String input) {
        String[] splits = input.split("_");
        String wnid = splits[splits.length-1];
        return wnid.substring(0,wnid.length()-1);
    }

    private void markNonLeafNodes(ResultSet rs) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                if (isWordNet(subject)) {
                    nonLeafNode.add(extractWNID(object));
                }
            }
        }
    }

    private void filterLeafNodes(ResultSet rs) throws SQLException{
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                if (isWordNet(subject) && !nonLeafNode.contains(extractWNID(subject))) {
                    leafNode.add(extractWNID(subject));
                }
            }
        }
    }

    private void startFiltering(){

        PreparedStatement stmt;
        ResultSet rs;
        String query_yagotype;

        try {
            if (PROPERTIES.getProperty("debugLocally").equals("true")) {
                query_yagotype = "SELECT * FROM yagotaxonomy limit 10";

            } else {
                query_yagotype = "SELECT * FROM yagotaxonomy";

            }

            stmt = yagoConnection.prepareStatement(query_yagotype);
            rs = stmt.executeQuery();
            //mark non-leaf nodes
            markNonLeafNodes(rs);
            //filter leaf nodes
            stmt = yagoConnection.prepareStatement(query_yagotype);
            rs = stmt.executeQuery();
            filterLeafNodes(rs);
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            logger.info("SQL exceptions when running selections!");
        }
    }

    private void writeToFile(){
        IOUtilities.clearOutputfile("output/leafNodesWN.tsv");

        BufferedWriter bw;
        FileWriter fw;

        try {
            fw = new FileWriter("output/leafNodesWN.tsv");
            bw = new BufferedWriter(fw);

            for (String key: leafNode) {
                String content = key + "\n";
                bw.write(content);
            }

            bw.close();

        } catch (IOException exception) {
            logger.error("Error: can't create file: output/leafNodesWN.tsv");
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

        FilterLeafWordNetNode filter= new FilterLeafWordNetNode();
        filter.startFiltering();
        filter.writeToFile();
    }
}
