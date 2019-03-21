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

    private final static HashMap<String, Double> finalSummaryWeight = new HashMap<>();

    private final static HashMap<String, Integer> summaryCount = new HashMap<>();

    private final static HashSet<String> contextTags = new HashSet<>();

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
            updateSummaryWeight(tag, weight);

            // Add this synset to hash set
            synSetforImage.add(tag);
        } else {
            HashSet<String> objectsHashSet = yagoWNID2Hypernyms.get(tag);
            List<String> parentYagoEntities = new ArrayList<>(objectsHashSet);

            if (parentYagoEntities.size() != 0) {
                processTagsRecursively(parentYagoEntities, weight);
            }
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

        IOUtilities.loadContextTagstoMemory(contextTags);

    }

    private void writeHashMaptoFile(){

        BufferedWriter bw;
        FileWriter fw;

        String outputfile_count = "./output/summary_by_count.tsv";
        IOUtilities.clearOutputfile(outputfile_count);

        String outputfile_weight = "./output/summary_by_weight.tsv";


        try {
            fw = new FileWriter(outputfile_count);
            bw = new BufferedWriter(fw);

            for (String key: summaryCount.keySet()) {
                String content = IOUtilities.reconstructWNSynsetsName(key, yagoWNID2Names) + "\t" + summaryCount.get(key) + "\n";
                bw.write(content);
            }
            bw.close();

            writeWeightHashMapToFile(summaryWeight, outputfile_weight);

        } catch (IOException exception) {
            logger.error("Error: can't create file: ");
        }

    }

    private void writeWeightHashMapToFile(HashMap<String, Double> weightHashmap, String outputfile) {
        IOUtilities.clearOutputfile(outputfile);
        BufferedWriter bw;
        FileWriter fw;

        try {
            fw = new FileWriter(outputfile);
            bw = new BufferedWriter(fw);

            for (String key: weightHashmap.keySet()) {
                String content = IOUtilities.reconstructWNSynsetsName(key, yagoWNID2Names) + "\t" + weightHashmap.get(key) + "\n";
                bw.write(content);
            }
            bw.close();

        } catch (IOException exception) {
            logger.error("Error: can't create file: ");
        }

    }

    private void recursivelyUpdate(String key, double weight) {
        // first, update the corresponding weight in the final summary
        if (finalSummaryWeight.containsKey(key)){
            finalSummaryWeight.put(key, finalSummaryWeight.get(key) + weight);
        } else {
            finalSummaryWeight.put(key, weight);
        }

        // then get the parents of the keys
        HashSet<String> parentsHashset =  yagoWNID2Hypernyms.get(key);
        List<String> parents_array = new ArrayList<>(parentsHashset);
        parents_array = filterAllTags(parents_array);

        logger.info("Current tag is :" + yagoWNID2Names.get(key) + "|||| Weight = " + weight);
        logger.info("Its valid parents are: " + parents_array.toString());

        if (parents_array != null) {
            for (String parent: parents_array) {
                if (!parent.startsWith("<yago") && !parent.startsWith("owl:")){
                    recursivelyUpdate(parent, weight / parents_array.size());
                }
            }
        }

    }

    private void summarizeWeightsInWNOntology(){
        for (String key: summaryWeight.keySet()) {
            recursivelyUpdate(key, summaryWeight.get(key));
        }

        writeWeightHashMapToFile(finalSummaryWeight, "output/final_summary_weight.tsv");

    }

    private void summarizeToNearestWN(){
        String fileInput = "./output/replaced_entities_per_img_parcat.tsv";
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
                    String tagsLine = a_line.split("\t")[2];
                    List<String> regularTags = new ArrayList<>();
                    List<List<String>> parentTags = new ArrayList<>();
                    splitRegularCatandParentCats(tagsLine, regularTags, parentTags);

                    //Process regular tags
                    double weights_regularTags = ((double) regularTags.size()) / (regularTags.size() + parentTags.size());
                    processTagsRecursively(regularTags, weights_regularTags);
                    for (List<String> one_parentTag: parentTags) {
                        processTagsRecursively(one_parentTag, ((double) 1) / (regularTags.size() + parentTags.size()));
                    }

                } catch (StackOverflowError ex) {
                    logger.error("SOF for line:" + a_line);
                }

                line_counter++;
            }

        } catch (IOException exception) {
            logger.error("filenames.txt does not exist!");
        }

        writeHashMaptoFile();
    }

    private void startSummarization(){
        summarizeToNearestWN();

        summarizeWeightsInWNOntology();

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
    }

}
