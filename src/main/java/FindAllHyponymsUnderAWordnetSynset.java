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

    private boolean isWordNetLeafNode(String one_yagoentity){
        if (! one_yagoentity.startsWith("<wordnet_")) {return false;}

        // does not have children
        if (! entity2Hyponyms.containsKey(one_yagoentity)) {
            return true;
        }

        // none of its children start with wordnet
        HashSet<String> hyponyms = entity2Hyponyms.get(one_yagoentity);
        for (String one_hyponym: hyponyms) {
            if (one_hyponym.startsWith("<wordnet_")) {
                return false;
            }
        }

        return true;
    }

    private void findAllHyponyms(String yagoEntity, boolean onlyLeafWordnetNode) {
        HashSet<String> hyponyms = entity2Hyponyms.get(yagoEntity);
        logger.info("Find children: " + hyponyms.toString());

        for (String one_hyponym: hyponyms) {
            if (onlyLeafWordnetNode) {
                // look ahead to check if this is a leafnode
                if (isWordNetLeafNode(one_hyponym)){
                    hyponymsOfSynset.add(one_hyponym);
                }
            }
            else {
                hyponymsOfSynset.add(one_hyponym);
            }

            if (entity2Hyponyms.containsKey(one_hyponym)) {
                findAllHyponyms(one_hyponym, onlyLeafWordnetNode);
            }
        }
    }

    private void startWorking(String wordnetID, boolean onlyLeafWordnetNode){

        findAllHyponyms(wordnetID, onlyLeafWordnetNode);

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
        boolean onlyLeafWordnetNode = args[1].equals("true");
        produceAllWNunderBuilding.startWorking(args[0], onlyLeafWordnetNode);
    }

}
