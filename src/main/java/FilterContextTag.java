import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.*;

public class FilterContextTag {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private final static HashMap<String, HashSet<String>> yagoEntities2Types = new HashMap<>();

    private final static HashSet<String> contextTimeTags = new HashSet<>();

    private final static HashSet<String> contextLocationTags = new HashSet<>();

    private static final Properties PROPERTIES = new Properties();

    FilterContextTag(){
        try {
            //Load properties file
            PROPERTIES.load(new InputStreamReader(new FileInputStream("./src/main/resources/config.properties"), "UTF8"));
        } catch (IOException exception) {
            return;
        }

        loadYagotoMemory();
    }

    private boolean isValidObject(String typeInfo) {
        return (typeInfo != null &&
                !typeInfo.contains("wikicat_Abbreviations")
                && !typeInfo.contains("wordnet_first_name")
                && !typeInfo.contains("wordnet_surname")
        );
    }

    private void loadEnWikiResultSet(ResultSet rs) throws SQLException {
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

    private void loadForeignWikiResultSet(ResultSet rs) throws SQLException {
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

    private void loadYagotoMemory(){
        try {
            Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                    PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

            PreparedStatement stmt;

            // local debug mode: only load subset of types
            if (PROPERTIES.getProperty("debugLocally").equals("true")) {
                String query_yagotype = "SELECT * FROM subset_yagotypes";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                loadEnWikiResultSet(rs);
                rs.close();
                stmt.close();

            } else {
                // load all dataset
                // 1) load the enwiki yagotypes
                String query_yagotype = "SELECT * FROM yagotypes_enwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                loadEnWikiResultSet(rs);
                rs.close();
                stmt.close();

                // 2) load the foreign yagotypes
                query_yagotype = "SELECT * FROM yagotypes_foreignwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                rs = stmt.executeQuery();
                //Load the resultset
                loadForeignWikiResultSet(rs);
                rs.close();
                stmt.close();

                // 3 Load the yagotaxonomy
                String query_yagotaxonomy = "SELECT * FROM YAGOTAXONOMY";
                stmt = yagoConnection.prepareStatement(query_yagotaxonomy);
                rs = stmt.executeQuery();
                loadEnWikiResultSet(rs);
                rs.close();
                stmt.close();
            }

            logger.info("Finished loading yago to memory!");

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    private String typeOfTag(String tag, boolean needPrint) {
        if (tag.equals("<wordnet_calendar_month_115209413>")
                || tag.equals("<wordnet_year_115203791>") || contextTimeTags.contains(tag)){
            return "context-time";
        }

        if (tag.equals("<wordnet_administrative_district_108491826>")
                || tag.startsWith("<wikicat_Populated_places") || contextLocationTags.contains(tag)){
            return "context-location";
        }

        HashSet<String> hypernymSet = yagoEntities2Types.get(tag);

        if (needPrint){
            if (hypernymSet == null) {
                System.out.println(tag + "'s hyperhymSet is empty.");
            } else {
                System.out.println(tag + "'s hyperhymSet is: " + hypernymSet.toString());
            }

        }

        if (hypernymSet!=null) {
            // none of the set is context
            // iterate through each
            List<String> hypernymsList = new ArrayList<>(hypernymSet);
            HashSet<String> typesofHypernyms = new HashSet<>();
            for (String hypernym: hypernymsList) {
                typesofHypernyms.add(typeOfTag(hypernym, needPrint));
            }

            if (needPrint) {
                System.out.println("Tag= "+ tag + "| Its hypernyms are: " + typesofHypernyms.toString());
            }

            // Only or contain?
            // Contain: <2014> <August_2014> has event
            if (typesofHypernyms.contains("context-location")) {
                return "context-location";
            } else if (typesofHypernyms.contains("context-time")) {
                return "context-time";
            }

        }
        return "content";

    }

    private void clearOutputfile(String outputFileName){
        try {
            Writer output = new BufferedWriter(new FileWriter(outputFileName, false));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeHashSettoFile(HashSet hashSet, String outputFile){
        //clearOutputfile
        clearOutputfile(outputFile);

        BufferedWriter bw;
        FileWriter fw;

        Iterator iterator = hashSet.iterator();

        try {
            fw = new FileWriter(outputFile);
            bw = new BufferedWriter(fw);

            while (iterator.hasNext()) {
                String tag = (String) iterator.next();
                tag += "\n";
                bw.write(tag);

            }

            bw.close();

        } catch (IOException exception) {
            logger.error("Error: can't create file: " + outputFile);
        }
    }

    private void startFiltering(){
        Set<String> keySet= yagoEntities2Types.keySet();

        for (String tag: keySet)
        {
            String typeOfCurrentTag;
            if (tag.equals("<August_2014>")) {
                typeOfCurrentTag = typeOfTag(tag, true);
            } else {
                typeOfCurrentTag = typeOfTag(tag, false);
            }

            logger.info(tag + "is " + typeOfCurrentTag);

            switch (typeOfCurrentTag) {
                case "context-time": {
                    contextTimeTags.add(tag);
                    break;
                }
                case "context-location": {
                    contextLocationTags.add(tag);
                    break;
                }
            }
        }

        writeHashSettoFile(contextTimeTags, "./context-time-tags.txt");
        writeHashSettoFile(contextLocationTags, "./context-location-tags.txt");
    }

    public static void main(String[] args){
        try {
            // Database setup
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            return;
        }

        FilterContextTag filterContextTag = new FilterContextTag();
        filterContextTag.startFiltering();

    }
}
