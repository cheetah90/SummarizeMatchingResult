import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.*;

public class FilterContextTag {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private final static HashMap<String, HashSet<String>> yagoEntities2Types = new HashMap<>();

    private final static HashSet<String> contextTimeTags = new HashSet<>();

    private final static HashSet<String> contextLocationTags = new HashSet<>();



    private FilterContextTag(){
        IOUtilities.loadYagotoMemory(yagoEntities2Types);
    }


    private String typeOfTag(String tag, boolean needPrint) {
        if (tag.equals("<wordnet_calendar_month_115209413>")
                || tag.equals("<wordnet_year_115203791>") || contextTimeTags.contains(tag)){
            return "context-time";
        }

        if (tag.equals("<wordnet_administrative_district_108491826>")
                || tag.startsWith("<wikicat_Populated_places") || contextLocationTags.contains(tag)){
            return "context-location";
        }

        HashSet<String> hypernymSet = yagoEntities2Types.get(tag);

        if (needPrint){
            if (hypernymSet == null) {
                System.out.println(tag + "'s hyperhymSet is empty.");
            } else {
                System.out.println(tag + "'s hyperhymSet is: " + hypernymSet.toString());
            }

        }

        if (hypernymSet!=null) {
            // none of the set is context
            // iterate through each
            List<String> hypernymsList = new ArrayList<>(hypernymSet);
            HashSet<String> typesofHypernyms = new HashSet<>();
            for (String hypernym: hypernymsList) {
                typesofHypernyms.add(typeOfTag(hypernym, needPrint));
            }

            if (needPrint) {
                System.out.println("Tag= "+ tag + "| Its hypernyms are: " + typesofHypernyms.toString());
            }

            // Only or contain?
            // Contain: <2014> <August_2014> has event
            if (typesofHypernyms.contains("context-location")) {
                return "context-location";
            } else if (typesofHypernyms.contains("context-time")) {
                return "context-time";
            }

        }
        return "content";

    }

    private void clearOutputfile(String outputFileName){
        try {
            Writer output = new BufferedWriter(new FileWriter(outputFileName, false));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeHashSettoFile(HashSet hashSet, String outputFile){
        //clearOutputfile
        clearOutputfile(outputFile);

        BufferedWriter bw;
        FileWriter fw;

        Iterator iterator = hashSet.iterator();

        try {
            fw = new FileWriter(outputFile);
            bw = new BufferedWriter(fw);

            while (iterator.hasNext()) {
                String tag = (String) iterator.next();
                tag += "\n";
                bw.write(tag);

            }

            bw.close();

        } catch (IOException exception) {
            logger.error("Error: can't create file: " + outputFile);
        }
    }

    private void startFiltering(){
        Set<String> keySet= yagoEntities2Types.keySet();

        for (String tag: keySet)
        {
            String typeOfCurrentTag;
            if (tag.equals("<August_2014>")) {
                typeOfCurrentTag = typeOfTag(tag, true);
            } else {
                typeOfCurrentTag = typeOfTag(tag, false);
            }

            logger.info(tag + "is " + typeOfCurrentTag);

            switch (typeOfCurrentTag) {
                case "context-time": {
                    contextTimeTags.add(tag);
                    break;
                }
                case "context-location": {
                    contextLocationTags.add(tag);
                    break;
                }
            }
        }

        writeHashSettoFile(contextTimeTags, "./context-time-tags.txt");
        writeHashSettoFile(contextLocationTags, "./context-location-tags.txt");
    }

    public static void main(String[] args){
        try {
            // Database setup
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            return;
        }

        FilterContextTag filterContextTag = new FilterContextTag();
        filterContextTag.startFiltering();

    }
}
