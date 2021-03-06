import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.*;

public class IOUtilities {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    public static Properties PROPERTIES;

    static void appendLinetoFile(String strLine, String outputFileName){
        try {
            Writer output = new BufferedWriter(new FileWriter(outputFileName, true));
            output.append(strLine);
            output.append("\n");
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static void clearOutputfile(String outputFileName){
        try {
            Writer output = new BufferedWriter(new FileWriter(outputFileName, false));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static boolean isValidObject(String typeInfo) {
        return (typeInfo != null &&
                !typeInfo.contains("wikicat_Abbreviations")
                && !typeInfo.contains("wordnet_first_name")
                && !typeInfo.contains("wordnet_surname")
        );
    }

    private static String extractWNIDAndRecordMapping(String original_tag,
                                                      HashMap<String, String> yagoWNID2Names) {
        String[] splits = original_tag.split("_");
        String wnid= splits[splits.length-1];
        wnid = wnid.substring(0, wnid.length()-1);

        // save the mapping
        String name = "";
        for (int i = 1; i < (splits.length -1); i++){
            name += splits[i] + "_";
        }

        if (yagoWNID2Names.get(wnid) == null) {
            yagoWNID2Names.put(wnid, name);
        }

        return wnid;
    }

    private static void loadToJavaObjects(String subject,
                                          String object,
                                          HashMap<String, HashSet<String>> yagoWNID2Hypernyms,
                                          HashMap<String, String> yagoWNID2Names){
        // first add to yagoLowercase2Original
        if (subject.startsWith("<wordnet_")) {
            subject = extractWNIDAndRecordMapping(subject, yagoWNID2Names);
        }

        if (object.startsWith("<wordnet_")) {
            object = extractWNIDAndRecordMapping(object, yagoWNID2Names);
        }

        if (yagoWNID2Hypernyms.get(subject) == null) {
            // the lowercase does not exist
            HashSet<String> hashSet = new HashSet<>();
            hashSet.add(object);
            yagoWNID2Hypernyms.put(subject, hashSet);
        } else {
            yagoWNID2Hypernyms.get(subject).add(object);
        }
    }

    static String extractWNID(String input) {
        String[] splits = input.split("_");
        String wnid = splits[splits.length-1];
        return wnid.substring(0,wnid.length()-1);
    }

    static void writeHashSetToFile(HashSet<String> hashSet, String outputfilename){
        IOUtilities.clearOutputfile(outputfilename);

        BufferedWriter bw;
        FileWriter fw;

        try {
            fw = new FileWriter(outputfilename);
            bw = new BufferedWriter(fw);

            for (String key: hashSet) {
                String content = key + "\n";
                bw.write(content);
            }

            bw.close();

        } catch (IOException exception) {
            logger.error("Error: can't create file: " + outputfilename);
        }
    }

    private static void loadEnWikiResultSet(ResultSet rs,
                                            HashMap<String, HashSet<String>> yagoWNID2Hypernyms,
                                            HashMap<String, String> yagoWNID2Names
                                            ) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                loadToJavaObjects(subject, object, yagoWNID2Hypernyms, yagoWNID2Names);
            }

        }

        if (PROPERTIES.getProperty("debugLocally").equals("true")) {
            loadToJavaObjects("<wordnet_epistemology_106166748>", "<wordnet_philosophy_106158346>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<wordnet_logic_106163751>", "<wordnet_philosophy_106158346>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<wordnet_philosophy_106158346>", "<wordnet_humanistic_discipline_106153846>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<wordnet_metaphysics_106162653>", "<wordnet_philosophy_106158346>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<Book_cover>", "<wikicat_Books>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<Book_cover>", "<wikicat_Magazines>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<wikicat_Books>", "<wordnet_book_106410904>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<wikicat_Magazines>", "<wordnet_magazine_106595351>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<wordnet_magazine_106595351>", "<wordnet_publication_106589574>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<wordnet_book_106410904>", "<wordnet_publication_106589574>", yagoWNID2Hypernyms, yagoWNID2Names);
            loadToJavaObjects("<wordnet_publication_106589574>", "<wordnet_work_104599396>", yagoWNID2Hypernyms, yagoWNID2Names);

        }
    }

    private static void loadForeignWikiResultSet(ResultSet rs,
                                                 HashMap<String, HashSet<String>> yagoWNID2Hypernyms,
                                                 HashMap<String, String> yagoWNID2Names) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                // If this is a multilingual word
                if (subject.length() > 5 && subject.substring(1,4).matches("[a-zA-Z]{2}/")) {
                    String strip_subject = "<"+subject.substring(4);

                    loadToJavaObjects(strip_subject, object, yagoWNID2Hypernyms, yagoWNID2Names);
                }
            }
        }
    }

    static void loadContextTagstoMemory(HashSet<String> contextTags){
        String line;
        String fileName="";

        try {
            // Read context-location tags
            fileName = "./data/context-location-tags.txt";
            BufferedReader br = new BufferedReader(new FileReader(fileName));

            while ((line = br.readLine()) != null) {
                // process the line.
                contextTags.add(line);
            }


            // Read context-time tags
            fileName = "./data/context-time-tags.txt";
            br = new BufferedReader(new FileReader(fileName));
            while ((line = br.readLine()) != null) {
                // process the line.
                contextTags.add(line);
            }

        } catch (IOException exception) {
            logger.error("Error: failed to read a line from " + fileName);
            exception.printStackTrace();
        }
    }

    private static void loadMapping(ResultSet resultSet, HashMap<String, String> word2WorNetID) throws SQLException{
        while (resultSet.next()){
            String wordnet_id = resultSet.getString("wordnet_id");
            String word = resultSet.getString("word");

            word2WorNetID.putIfAbsent(word, "<wordnet_" + word + "_" + wordnet_id + ">");

        }
    }

    static void loadWordNetIDtoWordMapping(HashMap<String, String> word2WorNetID){
        try {
            Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4ImageNet.name"),
                    PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

            PreparedStatement stmt;

            String query_yagotype = "SELECT * FROM imagenet_synsets_wnid_to_word";
            stmt = yagoConnection.prepareStatement(query_yagotype);
            ResultSet rs = stmt.executeQuery();
            loadMapping(rs, word2WorNetID);
            rs.close();
            stmt.close();

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    static String reconstructWNSynsetsName(String WNID,
                                           HashMap<String, String> yagoWNID2Names){
        return "<wordnet_" + yagoWNID2Names.get(WNID) + WNID + ">";
    }

    private static void loadYaogHyponymFromResultSet(ResultSet rs,
                                             HashMap<String, HashSet<String>> entity2Hyponyms) throws SQLException{
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                if (entity2Hyponyms.containsKey(object)) {
                    entity2Hyponyms.get(object).add(subject);
                } else {
                    HashSet<String> hyponyms = new HashSet<>();
                    hyponyms.add(subject);
                    entity2Hyponyms.put(object, hyponyms);
                }
            }
        }
    }

    private static void loadYaogHyponymFromForeignResultSet(ResultSet rs,
                                                    HashMap<String, HashSet<String>> entity2Hyponyms) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                // If this is a multilingual word
                if (subject.length() > 5 && subject.substring(1,4).matches("[a-zA-Z]{2}/")) {
                    String strip_subject = "<"+subject.substring(4);

                    if (entity2Hyponyms.containsKey(object)) {
                        entity2Hyponyms.get(object).add(strip_subject);
                    } else {
                        HashSet<String> hyponyms = new HashSet<>();
                        hyponyms.add(strip_subject);
                        entity2Hyponyms.put(object, hyponyms);
                    }
                }
            }
        }

    }

    static List<String> loadArrayListFromtoString(String strLine, String leftDelimiter, String rightDelimiter) {
        strLine = strLine.substring(1,strLine.length()-1);

        ArrayList<String> tagsList = new ArrayList<>();

        String[] tagsParsed = strLine.split(rightDelimiter+", \\"+leftDelimiter);
        for (String tag: tagsParsed) {
            if (!tag.startsWith(leftDelimiter)) {
                tag = leftDelimiter + tag;
            }

            if (!tag.endsWith(rightDelimiter)) {
                tag = tag + rightDelimiter;
            }

            tagsList.add(tag);
        }

        return tagsList;
    }

    static boolean isParentCats(String tag) {
        return tag.startsWith("<[{");
    }

    static void splitRegularCatandParentCats(String tagsLine, List<String> regularCats, List<List<String>> parentCats){
        tagsLine = tagsLine.substring(1,tagsLine.length()-1);

        List<String> tagsArray = loadArrayListFromtoString(tagsLine, "<", ">");

        for (String tag: tagsArray) {
            if (isParentCats(tag)) {
                tag = tag.substring(1,tag.length()-1);
                List<String> new_parentTags = new ArrayList<>();
                for (String parTag: loadArrayListFromtoString(tag, "{", "}")) {
                    new_parentTags.add(parTag.replace('{', '<').replace('}', '>'));
                }
                parentCats.add(new_parentTags);
            } else {
                regularCats.add(tag);
            }
        }
    }

    static void loadYagoHyponymToMemory(HashMap<String, HashSet<String>> entity2Hyponyms) {
        try {
            if (PROPERTIES == null) {
                try {
                    PROPERTIES = new Properties();
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
                loadYaogHyponymFromResultSet(rs, entity2Hyponyms);
                rs.close();
                stmt.close();

            } else {
                // load all dataset
                // 1) load the enwiki yagotypes
                String query_yagotype = "SELECT * FROM yagotypes_enwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                loadYaogHyponymFromResultSet(rs, entity2Hyponyms);
                rs.close();
                stmt.close();

                // 2) load the foreign yagotypes
                query_yagotype = "SELECT * FROM yagotypes_foreignwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                rs = stmt.executeQuery();
                //Load the resultset
                loadYaogHyponymFromForeignResultSet(rs, entity2Hyponyms);
                rs.close();
                stmt.close();

                // 3 Load the yagotaxonomy
                String query_yagotaxonomy = "SELECT * FROM YAGOTAXONOMY";
                stmt = yagoConnection.prepareStatement(query_yagotaxonomy);
                rs = stmt.executeQuery();
                loadYaogHyponymFromResultSet(rs, entity2Hyponyms);
                rs.close();
                stmt.close();
            }

            logger.info("Finished loading yago to memory!");

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    static void loadYagotoMemory(HashMap<String,HashSet<String>> yagoWNID2Hypernyms,
                                 HashMap<String, String> yagoWNID2Names){

        try {
            if (PROPERTIES == null) {
                try {
                    PROPERTIES = new Properties();
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
                loadEnWikiResultSet(rs, yagoWNID2Hypernyms, yagoWNID2Names);
                rs.close();
                stmt.close();

            } else {
                // load all dataset
                // 1) load the enwiki yagotypes
                String query_yagotype = "SELECT * FROM yagotypes_enwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                loadEnWikiResultSet(rs, yagoWNID2Hypernyms, yagoWNID2Names);
                rs.close();
                stmt.close();

                // 2) load the foreign yagotypes
                query_yagotype = "SELECT * FROM yagotypes_foreignwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                rs = stmt.executeQuery();
                //Load the resultset
                loadForeignWikiResultSet(rs, yagoWNID2Hypernyms, yagoWNID2Names);
                rs.close();
                stmt.close();

                // 3 Load the yagotaxonomy
                String query_yagotaxonomy = "SELECT * FROM YAGOTAXONOMY";
                stmt = yagoConnection.prepareStatement(query_yagotaxonomy);
                rs = stmt.executeQuery();
                loadEnWikiResultSet(rs, yagoWNID2Hypernyms, yagoWNID2Names);
                rs.close();
                stmt.close();
            }

            logger.info("Finished loading yago to memory!");

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }
}
