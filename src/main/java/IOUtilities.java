import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class IOUtilities {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private static Properties PROPERTIES;


    private static boolean isValidObject(String typeInfo) {
        return (typeInfo != null &&
                !typeInfo.contains("wikicat_Abbreviations")
                && !typeInfo.contains("wordnet_first_name")
                && !typeInfo.contains("wordnet_surname")
        );
    }

    private static void loadEnWikiResultSet(ResultSet rs, HashMap<String, HashSet<String>> yagoEntities2Types) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                // first add to yagoLowercase2Original
                if (yagoEntities2Types.get(subject) == null) {
                    // the lowercase does not exist
                    HashSet<String> hashSet = new HashSet<>();
                    hashSet.add(object);
                    yagoEntities2Types.put(subject, hashSet);
                } else {
                    yagoEntities2Types.get(subject).add(object);
                }

                if (PROPERTIES.getProperty("debugLocally").equals("true")) {
                    HashSet<String> hashSet = new HashSet<>();
                    hashSet.add("<wikicat_Polish_people>");
                    hashSet.add("<wordnet_canoeist_109891470>");
                    yagoEntities2Types.put("<wikicat_Polish_canoeists>", hashSet);


                    HashSet<String> hashSet1 = new HashSet<>();
                    hashSet1.add("<Traffic_sign>");
                    yagoEntities2Types.put("<Highway_sign>", hashSet1);

                    HashSet<String> hashSet2 = new HashSet<>();
                    hashSet2.add("<wikicat_Color_codes>");
                    hashSet2.add("<wikicat_Traffic_signs>");
                    hashSet2.add("<wikicat_Symbols>");
                    yagoEntities2Types.put("<Traffic_sign>", hashSet2);

                    HashSet<String> hashSet3 = new HashSet<>();
                    hashSet3.add("<wordnet_person_100007846>");
                    yagoEntities2Types.put("<wikicat_Polish_people>", hashSet3);
                }

            }

        }
    }

    private static void loadForeignWikiResultSet(ResultSet rs, HashMap<String, HashSet<String>> yagoEntities2Types) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                // If this is a multilingual word
                if (subject.length() > 5 && subject.substring(1,4).matches("[a-zA-Z]{2}/")) {
                    String strip_subject = "<"+subject.substring(4);

                    // first add to yagoLowercase2Original
                    // if this foreign entity does not exist in en.wiki, add it without the lang code
                    if (yagoEntities2Types.get(strip_subject) == null) {
                        // the lowercase does not exist
                        HashSet<String> hashSet = new HashSet<>();
                        hashSet.add(object);
                        yagoEntities2Types.put(strip_subject, hashSet);
                    } else {
                        // if this foreign entity exist in en.wiki, add it with the lang code
                        if (yagoEntities2Types.get(subject) == null) {
                            // the lowercase does not exist
                            HashSet<String> hashSet = new HashSet<>();
                            hashSet.add(object);
                            yagoEntities2Types.put(subject, hashSet);
                        } else {
                            yagoEntities2Types.get(subject).add(object);
                        }
                    }
                }
            }
        }
    }

    static void loadYagotoMemory(HashMap<String, HashSet<String>> yagoEntities2Type){

        try {
            if (PROPERTIES == null) {
                try {
                    //Load properties file
                    PROPERTIES.load(new InputStreamReader(new FileInputStream("./src/main/resources/config.properties"), "UTF8"));
                } catch (IOException exception) {
                    return;
                }
            }


            Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                    PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

            PreparedStatement stmt;

            // local debug mode: only load subset of types
            if (PROPERTIES.getProperty("debugLocally").equals("true")) {
                String query_yagotype = "SELECT * FROM subset_yagotypes";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                loadEnWikiResultSet(rs, yagoEntities2Type);
                rs.close();
                stmt.close();

            } else {
                // load all dataset
                // 1) load the enwiki yagotypes
                String query_yagotype = "SELECT * FROM yagotypes_enwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                loadEnWikiResultSet(rs, yagoEntities2Type);
                rs.close();
                stmt.close();

                // 2) load the foreign yagotypes
                query_yagotype = "SELECT * FROM yagotypes_foreignwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                rs = stmt.executeQuery();
                //Load the resultset
                loadForeignWikiResultSet(rs, yagoEntities2Type);
                rs.close();
                stmt.close();

                // 3 Load the yagotaxonomy
                String query_yagotaxonomy = "SELECT * FROM YAGOTAXONOMY";
                stmt = yagoConnection.prepareStatement(query_yagotaxonomy);
                rs = stmt.executeQuery();
                loadEnWikiResultSet(rs, yagoEntities2Type);
                rs.close();
                stmt.close();
            }

            logger.info("Finished loading yago to memory!");

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }
}