import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.*;

public class ResultSummarizer {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private final static HashMap<String, HashSet<String>> yagoEntities2Types = new HashMap<>();

    private final static HashMap<String, Double> summarizationWeight = new HashMap<>();

    private final static HashMap<String, Integer> summarizationCount = new HashMap<>();

    private final static HashSet<String> contextTags = new HashSet<>();

    private HashSet<String> synSetforImage = new HashSet<>();

    private ResultSummarizer(){

        IOUtilities.loadYagotoMemory(yagoEntities2Types);

        IOUtilities.loadContextTagstoMemory(contextTags);
    }

    private boolean isWordNetSynset(String object) {
        return object.startsWith("<wordnet_");
    }

    private void processOneTag(String tag, Double weight) {
        // Hack to deal with the number inconsistencies of wordnet_postage. Fixed in MatchYago
        if (tag.startsWith("<wordnet_postage")) {
            tag = "<wordnet_postage_106796119>";
        }

        // If this is a wordnet, then update the summarization results
        if (isWordNetSynset(tag)) {
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

            // Update summarizationWeight
            if (summarizationWeight.get(tag) == null) {
                summarizationWeight.put(tag, weight);
            } else {
                double currentWeight = summarizationWeight.get(tag);
                summarizationWeight.put(tag, currentWeight+ weight);
            }

            // Add this synset to hash set
            synSetforImage.add(tag);


        } else {
            // If not, then recursively check its object
            HashSet<String> objectsHashSet = yagoEntities2Types.get(tag);
            List<String> objectsList = new ArrayList<>(objectsHashSet);

            // If not, then process each of its object
            processTagsRecursively(objectsList, weight);
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

    private boolean isValidTag(String tag) {
        // if it's too short, it's often unmeaningful
        if (tag.length() < 3) {
            return false;
        }

        // If it does not exist, it's a bad
        if (yagoEntities2Types.get(tag) == null) {
            logger.error("Error - tag does not exist: " + tag);
            return false;
        }

        // If it's a context tag, it's bad
        if (contextTags.contains(tag)) {
            return false;
        }

        return true;

    }

    private List<String> parseTagsofImage(String strLine) {
        ArrayList<String> tagsList = new ArrayList<>();

        String[] tagsParsed = strLine.split(">, <");
        for (String tag: tagsParsed) {
            if (!tag.startsWith("<")) {
                tag = "<" + tag;
            }

            if (!tag.endsWith(">")) {
                tag = tag + ">";
            }

            tagsList.add(tag);
        }

        return tagsList;
    }

    private List<String> filterAllTags(List<String> tagsArray){
        ArrayList<String> validTags = new ArrayList<>();

        for (String tag: tagsArray) {
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


    private void startSummarization(){
        // Create ThreadPool
        //ExecutorService pool = Executors.newFixedThreadPool(Integer.parseInt(PROPERTIES.getProperty("maxThreadPool")));

        String fileInput = "./output_per_img.tsv";

        try {
            // Buffered read the file
            BufferedReader br = new BufferedReader(new FileReader(fileInput));
            String line;

            while ((line = br.readLine()) != null) {
                synSetforImage.clear();

                String tagsLine = line.split("\t")[2];
                tagsLine = tagsLine.substring(1,tagsLine.length()-1);

                List<String> tagsArray = parseTagsofImage(tagsLine);

                processTagsRecursively(tagsArray, 1.0);

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
