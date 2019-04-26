import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class FindAllHyponymsUnderAWordnetSynset {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private HashSet<String> hyponymsOfSynset = new HashSet<>();

    private HashMap<String, HashSet<String>> entity2Hyponyms = new HashMap<>();

    private FindAllHyponymsUnderAWordnetSynset(){
        IOUtilities.loadYagoHyponymToMemory(entity2Hyponyms);
    }

    private void findAllHyponyms(String yagoEntity) {
        HashSet<String> hyponyms = entity2Hyponyms.get(yagoEntity);
        logger.info("Find children: " + hyponyms.toString());

        for (String one_hyponym: hyponyms) {
            hyponymsOfSynset.add(one_hyponym);

            if (entity2Hyponyms.containsKey(one_hyponym)) {
                findAllHyponyms(one_hyponym);
            }
        }
    }

    private void startWorking(String wordnetID){

        findAllHyponyms(wordnetID);

        IOUtilities.writeHashSetToFile(hyponymsOfSynset, "output/tmp_allhyponyms.tsv");
    }

    public static void main(String[] args){
        try {
            // Database setup
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            return;
        }

        System.out.println(args[0]);
        FindAllHyponymsUnderAWordnetSynset produceAllWNunderBuilding = new FindAllHyponymsUnderAWordnetSynset();
        produceAllWNunderBuilding.startWorking(args[0]);
    }

}
