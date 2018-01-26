import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.*;

public class AddMissingWordNettoYago {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private static final Properties PROPERTIES = new Properties();

    private static final HashMap<String, String> word2WorNetID = new HashMap<>();

    private static final HashSet<String> contextTags = new HashSet<>();

    private static final HashMap<String, String> missingMappoing = new HashMap<>();

    private void loadContextTagstoMemory(){
        String line;
        String fileName="";

        try {
            // Read context-location tags
            fileName = "./context-location-tags.txt";
            BufferedReader br = new BufferedReader(new FileReader(fileName));

            while ((line = br.readLine()) != null) {
                // process the line.
                contextTags.add(line);
            }


            // Read context-time tags
            fileName = "./context-time-tags.txt";
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

    private AddMissingWordNettoYago(){
        try {
            //Load properties file
            PROPERTIES.load(new InputStreamReader(new FileInputStream("./src/main/resources/config.properties"), "UTF8"));
        } catch (IOException exception) {
            return;
        }

        loadWordNetIDtoWordMapping();

        loadContextTagstoMemory();

    }


    private void loadMapping(ResultSet resultSet) throws SQLException{
        while (resultSet.next()){
            String wordnet_id = resultSet.getString("wordnet_id");
            String word = resultSet.getString("word");

            word2WorNetID.putIfAbsent(word, "<wordnet_" + word + "_" + wordnet_id + ">");

        }
    }

    boolean isValidEntity(String entity) {
        // If not a contextLocation entity
        if (contextTags.contains(entity)) {
            return false;
        }

        //If all uppercase
        if (entity.toUpperCase().equals(entity) && entity.length() > 3) {
            return false;
        }

        // If not too short
        if (entity.length()<5) {
            return false;
        }

        return true;

    }

    private void clearOutputfile(String outputFileName){
        try {
            Writer output = new BufferedWriter(new FileWriter(outputFileName, false));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startWorking(){
        try {
            Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                    PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

            PreparedStatement stmt;

            String tableName = PROPERTIES.getProperty("debugLocally").equals("true")?"subset_yagotypes":"yagotypes";

            String query_yagotype = "SELECT * FROM " + tableName;
            stmt = yagoConnection.prepareStatement(query_yagotype);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String entity = rs.getString("subject");
                if (entity != null){
                    String lower_entity = entity.substring(1, entity.length()-1).toLowerCase();
                    String synset = word2WorNetID.get(lower_entity);
                    if (synset != null && isValidEntity(entity)) {
                        // <Yago entity, wordnet synset>
                        missingMappoing.putIfAbsent(entity, synset);
                    }
                } else {
                    System.out.println("Subject is null. Weird!");
                }

            }

            rs.close();
            stmt.close();

        } catch (SQLException exception) {
            exception.printStackTrace();
        }

    }

    private void loadWordNetIDtoWordMapping(){
        try {
            Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4ImageNet.name"),
                    PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

            PreparedStatement stmt;

            String query_yagotype = "SELECT * FROM imagenet_synsets_wnid_to_word";
            stmt = yagoConnection.prepareStatement(query_yagotype);
            ResultSet rs = stmt.executeQuery();
            loadMapping(rs);
            rs.close();
            stmt.close();

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    private void WriteHashMapToFile(HashMap<String, String> hashMap, String outputFile) {
        //clearOutputfile
        clearOutputfile(outputFile);

        BufferedWriter bw;
        FileWriter fw;

        try {
            fw = new FileWriter(outputFile);
            bw = new BufferedWriter(fw);


            Iterator iterator = hashMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry pair = (Map.Entry)iterator.next();
                String line = pair.getKey() + "\t" + pair.getValue();
                line += "\n";
                bw.write(line);

                iterator.remove(); // avoids a ConcurrentModificationException
            }

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


    public static void main(String[] args){
        try {
            // Database setup
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            return;
        }

        AddMissingWordNettoYago addMissingWordNettoYago = new AddMissingWordNettoYago();
        addMissingWordNettoYago.startWorking();
        addMissingWordNettoYago.WriteHashMapToFile(missingMappoing,"./missing_wordnet.txt");


    }
}
