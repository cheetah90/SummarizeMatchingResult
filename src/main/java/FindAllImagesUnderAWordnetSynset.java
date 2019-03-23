import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;

public class FindAllImagesUnderAWordnetSynset {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private HashSet<String> hyponymsOfSynset = new HashSet<>();

    private HashMap<String, HashSet<String>> entity2Hyponyms = new HashMap<>();

    private FindAllImagesUnderAWordnetSynset(){
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

    private boolean containsHyponyms(List<String> tags) {
        for (String tag: tags) {
            if (hyponymsOfSynset.contains(tag)) {
                return true;
            }
        }

        return false;
    }

    private void findAllImages() {
        String fileInput = "output/replaced_entities_per_img_parcat.tsv";
        String output = "output/images_under_node.tsv";
        IOUtilities.clearOutputfile(output);

        try {
            // Buffered read the file
            BufferedReader br = new BufferedReader(new FileReader(fileInput));
            String a_line;
            int line_counter = 0;

            while ((a_line = br.readLine()) != null) {
                if (line_counter % 10000 == 0) {
                    logger.info("Finished processing: " + line_counter);
                }

                try {
                    String[] splits = a_line.split("\t");
                    if (splits.length == 3) {
                        String tagsLine = splits[2];
                        List<String> regularTags = new ArrayList<>();
                        List<List<String>> parentTags = new ArrayList<>();
                        List<String> parentTags_flattened = new ArrayList<>();
                        IOUtilities.splitRegularCatandParentCats(tagsLine, regularTags, parentTags);
                        for (List<String> one_parent_tag: parentTags) {
                            parentTags_flattened.addAll(one_parent_tag);
                        }

                        if (containsHyponyms(regularTags) || containsHyponyms(parentTags_flattened)) {
                            IOUtilities.appendLinetoFile(a_line, output);
                        }
                    }
                } catch (StackOverflowError ex) {
                    logger.error("SOF for line:" + a_line);
                }

                line_counter++;
            }

        } catch (IOException exception) {
            logger.error("filenames.txt does not exist!");
        }
    }

    private void startWorking(String wordnetID){

        findAllHyponyms(wordnetID);

        IOUtilities.writeHashSetToFile(hyponymsOfSynset, "output/tmp_allhyponyms.tsv");

        findAllImages();
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
        FindAllImagesUnderAWordnetSynset produceAllWNunderBuilding = new FindAllImagesUnderAWordnetSynset();
        produceAllWNunderBuilding.startWorking(args[0]);
    }

}
