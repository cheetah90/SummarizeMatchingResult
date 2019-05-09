import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ResultSummarizer {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private final static HashMap<String, HashSet<String>> yagoWNID2Hypernyms = new HashMap<>();

    private final static HashMap<String, String> yagoWNID2Names = new HashMap<>();

    private final static HashMap<String, Double> summaryWeight = new HashMap<>();

    private final static HashMap<String, Integer> summaryCount = new HashMap<>();

    private HashSet<String> synSetforImage = new HashSet<>();


    private boolean isWordNetSynset(String object) {
        return  object.startsWith("<wordnet_");
    }

    private String extractWNID(String original_tag) {
        String[] splits = original_tag.split("_");
        String WNID = splits[splits.length-1];
        WNID = WNID.substring(0, WNID.length()-1);

        return WNID;
    }

    private boolean isValidTag(String tag) {
        // if it's too short, it's often unmeaningful
        if (tag.length() < 3) {
            return false;
        }

        // if it starts with "yago", it's bad since it won't be counted in the weights
        if (tag.startsWith("<yago")) {
            return false;
        }

        // If it does not exist, it's bad
        if (yagoWNID2Hypernyms.get(tag) == null) {
            if (!tag.equals("owl:Thing")) {
                logger.error("Error - tag does not exist: " + tag);
            }
            return false;
        }

        return true;

    }

    private List<String> filterAllTags(List<String> tagsArray){
        ArrayList<String> validTags = new ArrayList<>();

        for (String tag: tagsArray) {
            if (isWordNetSynset(tag)){
                tag = extractWNID(tag);
            }

            if (isValidTag(tag)) {
                validTags.add(tag);
            }
        }

        return validTags;
    }

    private void processTagsRecursively(List<String> tagsArray, double sum_of_weights) {
        // Filter invalid tags
        List<String> validTagsArray = filterAllTags(tagsArray);

        // Process each tag
        for (String tag: validTagsArray) {
            // If not, then process each of its object
            processOneTag(tag, sum_of_weights/(double) validTagsArray.size());
        }

    }

    private void processOneTag(String tag, Double weight) {
        // Hack to deal with the number inconsistencies of wordnet_postage. Fixed in MatchYago
//        if (tag.startsWith("<wordnet_postage")) {
//            tag = "<wordnet_postage_106796119>";
//        }

        // If this is a wordnet, then update the summarization results
        if (isWNID(tag)) {
            // If a new synset, add the count
            if (!synSetforImage.contains(tag)) {
                updateSummaryCount(tag);
            }
//            updateSummaryWeight(tag, weight);

            // Add this synset to hash set
            synSetforImage.add(tag);
        }

        HashSet<String> objectsHashSet = yagoWNID2Hypernyms.get(tag);
        List<String> parentYagoEntities = new ArrayList<>(objectsHashSet);

        if (parentYagoEntities.size() != 0) {
            processTagsRecursively(parentYagoEntities, weight);
        }
    }

    private boolean isWNID(String object) {
        return isInteger(object);
    }

    private void updateSummaryCount(String tag){
        // Update the summarizationCount
        if (summaryCount.get(tag) == null) {
            summaryCount.put(tag, 1);
        } else {
            int currentCount = summaryCount.get(tag);
            summaryCount.put(tag, currentCount+1);
        }
    }

    private void updateSummaryWeight(String tag, Double weight){
        if (summaryWeight.get(tag) == null) {
            summaryWeight.put(tag, weight);
        } else {
            double currentWeight = summaryWeight.get(tag);
            summaryWeight.put(tag, currentWeight+ weight);
        }
    }

    private boolean isInteger(String s) {
        boolean isValidInteger = false;

        try {
            Integer.parseInt(s);
            isValidInteger = true;
        } catch (NumberFormatException ex) {
            // just pass
        }
        return isValidInteger;
    }

    private ResultSummarizer(){

        IOUtilities.loadYagotoMemory(yagoWNID2Hypernyms, yagoWNID2Names);

        ProcessBatchImageRunnable.setyagoWNID2Hypernyms(yagoWNID2Hypernyms);

    }

    private void writeHashMaptoFile(HashMap summarization, String outputFile){
        //clearOutputfile
        IOUtilities.clearOutputfile(outputFile);

        BufferedWriter bw;
        FileWriter fw;

        Iterator iterator = summarization.entrySet().iterator();

        try {
            fw = new FileWriter(outputFile);
            bw = new BufferedWriter(fw);

            while (iterator.hasNext()) {
                Map.Entry pair = (Map.Entry) iterator.next();
                String content = IOUtilities.reconstructWNSynsetsName((String) pair.getKey(), yagoWNID2Names) + "\t" + pair.getValue() + "\n";
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
        writeHashMaptoFile(summaryCount, "./output/summary_by_count.tsv");

        // Write the weight summary
        writeHashMaptoFile(summaryWeight, "./output/summary_by_weight.tsv");
    }

    private void startSummarization(){
        String fileInput = "content_tags.tsv";
        int line_counter = 0;

        try {
            // Buffered read the file
            BufferedReader br = new BufferedReader(new FileReader(fileInput));
            String a_line;

            while ((a_line = br.readLine()) != null) {
                if (line_counter % 10000 == 0) {
                    logger.info("Finished processing: " + line_counter);
                }

                synSetforImage.clear();

                try {
                    String[] splits = a_line.split("\t");
                    if (splits.length == 3) {
                        String tagsLine = splits[2];
                        List<String> regularTags = new ArrayList<>();
                        List<List<String>> parentTags = new ArrayList<>();
                        IOUtilities.splitRegularCatandParentCats(tagsLine, regularTags, parentTags);

                        //Process regular tags
                        double weights_regularTags = ((double) regularTags.size()) / (regularTags.size() + parentTags.size());
                        processTagsRecursively(regularTags, weights_regularTags);
                        for (List<String> one_parentTag: parentTags) {
                            processTagsRecursively(one_parentTag, ((double) 1) / (regularTags.size() + parentTags.size()));
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
