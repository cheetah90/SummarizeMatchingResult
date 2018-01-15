import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ResultSummarizer {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private static final Properties PROPERTIES = new Properties();

    private final static HashMap<String, HashSet<String>> yagoEntities2Types = new HashMap<>();

    private final static HashMap<String, Double> summarizationWeight = new HashMap<>();

    private final static HashMap<String, Integer> summarizationCount = new HashMap<>();

    private ResultSummarizer(){
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
                && !typeInfo.contains("owl:Thing")
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

    private void clearOutputfile(String outputFileName){
        try {
            Writer output = new BufferedWriter(new FileWriter(outputFileName, false));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int getLineNumberofFile(String file_ImageNames) {
        try {
            Process p = Runtime.getRuntime().exec("wc -l " + file_ImageNames);
            p.waitFor();

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = reader.readLine();
            return Integer.parseInt(line.trim().split(" ")[0]);

        } catch (Exception exception) {
            exception.printStackTrace();
        }

        return 0;

    }

    private boolean isWordNetSynset(String object) {
        return object.startsWith("<wordnet_");
    }

    private void processTag(String tag, Double weight) {
        if (yagoEntities2Types.get(tag) == null) {
            logger.error("Error - tag does not exist: " + tag);
            return;
        }

        // Handle redirect
        HashSet<String> objectsHashSet = yagoEntities2Types.get(tag);
        List<String> objectsList = new ArrayList<>(objectsHashSet);
        int size_of_objectsList = objectsList.size();



        // If this is a redirect, process the original tag
        for (String object: objectsList) {
            // If this is a wordnet, then update the summarization results
            if (isWordNetSynset(object)) {
                // Update the summarizationCount
                if (summarizationCount.get(object) == null) {
                    summarizationCount.put(object, 1);
                } else {
                    int currentCount = summarizationCount.get(object);
                    summarizationCount.put(object, currentCount+1);
                }

                // Update summarizationWeight
                if (summarizationWeight.get(object) == null) {
                    summarizationWeight.put(object, weight/size_of_objectsList);
                } else {
                    double currentWeight = summarizationWeight.get(object);
                    summarizationWeight.put(object, currentWeight+ weight/size_of_objectsList);
                }
            } else {
                processTag(object, weight/size_of_objectsList);
            }
        }


    }

    private void writeHashMaptoFile(HashMap summarization, String outputFile){
        //clearOutputfile
        clearOutputfile(outputFile);

        BufferedWriter bw;
        FileWriter fw;

        Iterator iterator = summarization.entrySet().iterator();

        try {
            fw = new FileWriter(outputFile);
            bw = new BufferedWriter(fw);

            while (iterator.hasNext()) {
                Map.Entry pair = (Map.Entry) iterator.next();
                String content = pair.getKey() + "\t" + pair.getValue() + "\n";
                bw.write(content);
                iterator.remove();
            }

            bw.close();

        } catch (IOException exception) {
            logger.error("Error: can't create file: " + outputFile);
        }
    }

    private void writeToFile(){
        // Write the count summary
        writeHashMaptoFile(summarizationCount, "./summary_by_count.tsv");

        // Write the weight summary
        writeHashMaptoFile(summarizationWeight, "./summary_by_weight.tsv");
    }

    private void startSummarization(){


        // Create ThreadPool
        //ExecutorService pool = Executors.newFixedThreadPool(Integer.parseInt(PROPERTIES.getProperty("maxThreadPool")));

        String fileInput = "./output_per_tag.tsv";

        try {
            // Buffered read the file
            BufferedReader br = new BufferedReader(new FileReader(fileInput));
            String line;

            while ((line = br.readLine()) != null) {
                try {
                    String tag = line.split("\t")[2];
                    processTag(tag, 1.0);
                } catch (Exception exception) {
                    logger.error("Error parsing line: " + line);
                }
            }
        } catch (Exception exception) {
            logger.error("filenames.txt does not exist!");
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


        } catch (SQLException exception) {
            exception.printStackTrace();
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

        ResultSummarizer resultSummarizer = new ResultSummarizer();
        resultSummarizer.startSummarization();
        resultSummarizer.writeToFile();
    }

}
