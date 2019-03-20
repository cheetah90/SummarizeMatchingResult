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

    private final static HashSet<String> contextTags = new HashSet<>();

    final static ArrayList<HashMap<String, Integer>> array_summarizationCount = new ArrayList<>();

    final static ArrayList<HashMap<String, Double>> array_summarizationWeight = new ArrayList<>();

    private ResultSummarizer(){

        IOUtilities.loadYagotoMemory(yagoWNID2Hypernyms, yagoWNID2Names);

        IOUtilities.loadContextTagstoMemory(contextTags);

        ProcessBatchImageRunnable.setyagoWNID2Hypernyms(yagoWNID2Hypernyms);

        ProcessBatchImageRunnable.setcontextTags(contextTags);

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

    private void summarizeCount(ArrayList<HashMap<String, Integer>> arraySummarization, HashMap<String, Integer> finalSummarization) {
        for (HashMap<String, Integer> one_img_summary: arraySummarization) {
            for (String key: one_img_summary.keySet()) {
                if (finalSummarization.containsKey(key)) {
                    finalSummarization.put(key, finalSummarization.get(key) + one_img_summary.get(key));
                } else {
                    finalSummarization.put(key, one_img_summary.get(key));
                }
            }
        }
    }

    private void summarizeWeight(ArrayList<HashMap<String, Double>> arraySummarization, HashMap<String, Double> finalSummarization) {
        for (HashMap<String, Double> one_img_summary: arraySummarization) {
            for (String key: one_img_summary.keySet()) {
                if (finalSummarization.containsKey(key)) {
                    finalSummarization.put(key, finalSummarization.get(key) + one_img_summary.get(key));
                } else {
                    finalSummarization.put(key, one_img_summary.get(key));
                }
            }
        }
    }

    private void startSummarization(){
        String fileInput = "./output/replaced_entities_per_img_parcat.tsv";

        ExecutorService pool = null;
        int line_counter = 0;

        try {
            // Buffered read the file
            BufferedReader br = new BufferedReader(new FileReader(fileInput));
            ArrayList<String> batch_imageCats = null;
            String a_line;

            while ((a_line = br.readLine()) != null) {
                // Split by 100k lines
                if (line_counter % Integer.parseInt(IOUtilities.PROPERTIES.getProperty("numImgsInSplit"))== 0) {
                    // if reachings 1m, shutdown the pool, wait until all tasks have completed
                    if (pool != null) {
                        // report the current status after this split.
                        System.out.println("Started processing " + line_counter);
                        logger.info("Started processing " + line_counter);

                        //Finished creating the threads
                        pool.shutdown();
                        // Wait until these tasks finished
                        if (!pool.awaitTermination(100, TimeUnit.MINUTES)) {
                            System.out.println("Error: reached the ExecutorService timeout!");
                            logger.error("Error: reached the ExecutorService timeout!");
                            pool.shutdownNow();
                        }
                    }

                    pool = Executors.newFixedThreadPool(Integer.parseInt(IOUtilities.PROPERTIES.getProperty("maxThreadPool")));
                }

                // For every 100, assign to a thread
                if (line_counter % Integer.parseInt(IOUtilities.PROPERTIES.getProperty("numImgsInBatch"))== 0) {
                    if (batch_imageCats != null && batch_imageCats.size() > 0) {
                        pool.execute(new ProcessBatchImageRunnable(new ArrayList<>(batch_imageCats)));
                    }
                    //Create a new batch_imageCats for next batch
                    batch_imageCats = new ArrayList<>();
                }

                batch_imageCats.add(a_line);

                line_counter++;

            }

            // process the remaining batch
            pool.execute(new ProcessBatchImageRunnable(new ArrayList<>(batch_imageCats)));
            //Finished creating the threads
            pool.shutdown();
            // Wait until these tasks finished
            if (!pool.awaitTermination(100, TimeUnit.MINUTES)) {
                System.out.println("Error: reached the ExecutorService timeout!");
                logger.error("Error: reached the ExecutorService timeout!");
                pool.shutdownNow();
            }

        } catch (IOException exception) {
            logger.error("filenames.txt does not exist!");
        } catch (InterruptedException exception) {
            logger.error("Caught InterruptedException");
        }

        // Finished processing, summarize the results
        summarizeCount(array_summarizationCount, summaryCount);
        summarizeWeight(array_summarizationWeight, summaryWeight);

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
