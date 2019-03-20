import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;

public class ResultSummarizer {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private final static HashMap<String, HashSet<String>> yagoWNID2Hypernyms = new HashMap<>();

    private final static HashMap<String, String> yagoWNID2Names = new HashMap<>();

    private final static HashMap<String, Double> summarizationWeight = new HashMap<>();

    private final static HashMap<String, Integer> summarizationCount = new HashMap<>();

    private final static HashSet<String> contextTags = new HashSet<>();

    private HashSet<String> synSetforImage = new HashSet<>();

    private ResultSummarizer(){

        IOUtilities.loadYagotoMemory(yagoWNID2Hypernyms, yagoWNID2Names);

        IOUtilities.loadContextTagstoMemory(contextTags);
    }

    private boolean isWordNetSynset(String object) {
        return  object.startsWith("<wordnet_");
    }

    private boolean isWNID(String object) {
        return isInteger(object);
    }

    private void updateSummaryCount(String tag){
        // If a new synset, add the count
        if (!synSetforImage.contains(tag)) {
            // Update the summarizationCount
            if (summarizationCount.get(tag) == null) {
                summarizationCount.put(tag, 1);
            } else {
                int currentCount = summarizationCount.get(tag);
                summarizationCount.put(tag, currentCount+1);
            }
        }
    }

    private void updateSummaryWeight(String tag, Double weight){
        if (summarizationWeight.get(tag) == null) {
            summarizationWeight.put(tag, weight);
        } else {
            double currentWeight = summarizationWeight.get(tag);
            summarizationWeight.put(tag, currentWeight+ weight);
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

    private void processOneTag(String tag, Double weight) {
        // Hack to deal with the number inconsistencies of wordnet_postage. Fixed in MatchYago
//        if (tag.startsWith("<wordnet_postage")) {
//            tag = "<wordnet_postage_106796119>";
//        }

        // If this is a wordnet, then update the summarization results
        if (isWNID(tag)) {
            updateSummaryCount(tag);
            updateSummaryWeight(tag, weight);

            // Add this synset to hash set
            synSetforImage.add(tag);
        }

        HashSet<String> objectsHashSet = yagoWNID2Hypernyms.get(tag);
        List<String> parentYagoEntities = new ArrayList<>(objectsHashSet);

        if (parentYagoEntities.size() != 0) {
            processTagsRecursively(parentYagoEntities, weight);
        }
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
        writeHashMaptoFile(summarizationCount, "./output/summary_by_count.tsv");

        // Write the weight summary
        writeHashMaptoFile(summarizationWeight, "./output/summary_by_weight.tsv");
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

        // If it does not exist, it's bad
        if (yagoWNID2Hypernyms.get(tag) == null) {
            if (!tag.equals("owl:Thing")) {
                logger.error("Error - tag does not exist: " + tag);
            }
            return false;
        }

        // If it's a context tag, it's bad
        if (IOUtilities.PROPERTIES.getProperty("excludeContextTags").equals("true")) {
            if (contextTags.contains(tag)) {
                return false;
            }
        }

        return true;

    }

    private List<String> loadArrayListFromtoString(String strLine, String leftDelimiter, String rightDelimiter) {
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

    private boolean isParentCats(String tag) {
        return tag.startsWith("<[{");
    }

    private void splitRegularCatandParentCats(String tagsLine, List<String> regularCats, List<List<String>> parentCats){
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


    private void startSummarization(){
        String fileInput = "./output/replaced_entities_per_img_parcat.tsv";

        try {
            // Buffered read the file
            BufferedReader br = new BufferedReader(new FileReader(fileInput));
            String line;
            int counter = 0;

            while ((line = br.readLine()) != null) {
                try {
                    if (counter % 10000 == 0) {
                        logger.info("Finished processing: " + counter);
                    }

                    synSetforImage.clear();

                    String tagsLine = line.split("\t")[2];
                    List<String> regularTags = new ArrayList<>();
                    List<List<String>> parentTags = new ArrayList<>();
                    splitRegularCatandParentCats(tagsLine, regularTags, parentTags);

                    //Process regular tags
                    double weights_regularTags = ((double) regularTags.size()) / (regularTags.size() + parentTags.size());
                    processTagsRecursively(regularTags, weights_regularTags);
                    for (List<String> one_parentTag: parentTags) {
                        processTagsRecursively(one_parentTag, ((double) 1) / (regularTags.size() + parentTags.size()));
                    }

                    counter++;
                } catch (StackOverflowError e) {
                    logger.error("StackOverflowError happens at: " + line);
                }
            }
        } catch (Exception exception) {
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
