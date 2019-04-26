import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class ProcessBatchImageRunnable implements Runnable {

    private static final Logger logger = LogManager.getLogger(ProcessBatchImageRunnable.class);

    private ArrayList<String> originalImgCatsArray;

    private static HashMap<String, HashSet<String>> yagoWNID2Hypernyms = null;

    private static HashSet<String> contextTags = null;

    private HashMap<String, Double> summarizationWeight = null;

    private HashMap<String, Integer> summarizationCount = null;

    static final Object LockSaveSummarizationResults = new Object();

    private HashSet<String> synSetforImage = new HashSet<>();

    static void setyagoWNID2Hypernyms(HashMap<String, HashSet<String>> yagoWNID2Hypernyms) {
        ProcessBatchImageRunnable.yagoWNID2Hypernyms = yagoWNID2Hypernyms;
    }

    static void setcontextTags(HashSet<String> contextTags) {
        ProcessBatchImageRunnable.contextTags = contextTags;
    }



    ProcessBatchImageRunnable(ArrayList<String> originalImageCatsArray) {
        this.originalImgCatsArray = originalImageCatsArray;
    }

    public void run() {

    }

}
