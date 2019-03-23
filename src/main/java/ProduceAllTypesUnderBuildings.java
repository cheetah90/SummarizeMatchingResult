import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class ProduceAllTypesUnderBuildings {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private HashSet<String> childrenOfBuilding = new HashSet<>();

    private HashMap<String, HashSet<String>> entity2Hyponyms = new HashMap<>();

    private ProduceAllTypesUnderBuildings(){
        IOUtilities.loadYagoHyponymToMemory(entity2Hyponyms);
    }

    private void findAllHyponyms(String yagoEntity) {
        HashSet<String> hyponyms = entity2Hyponyms.get(yagoEntity);

        for (String one_hyponym: hyponyms) {
            childrenOfBuilding.add(one_hyponym);
            logger.info("Find children: " + yagoEntity);

            if (entity2Hyponyms.containsKey(one_hyponym)) {
                findAllHyponyms(one_hyponym);
            }
        }
    }

    private void startWorking(){
        String wordnetID = "<wordnet_building_102913152>";

        findAllHyponyms(wordnetID);

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
