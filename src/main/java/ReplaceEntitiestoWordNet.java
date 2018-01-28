import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class ReplaceEntitiestoWordNet {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private static final Properties PROPERTIES = new Properties();

    private static final HashMap<String, String> entities2WordNet = new HashMap<>();

    static void loadEntities2WordNetMapping(HashMap<String, String> entities2WordNet){
        String line;
        String fileName="";

        try {
            // Read context-location tags
            fileName = "./data/missing_wordnet.txt";
            BufferedReader br = new BufferedReader(new FileReader(fileName));

            while ((line = br.readLine()) != null) {
                // process the line.
                String[] mapping = line.split("\t");
                entities2WordNet.putIfAbsent(mapping[0], mapping[1]);
            }


        } catch (IOException exception) {
            logger.error("Error: failed to read a line from " + fileName);
            exception.printStackTrace();
        }
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



    private void startWorking(){
        String outputFileName = "./output/replaced_entities_per_img.tsv";

        IOUtilities.clearOutputfile(outputFileName);

        String line;
        String fileName="";

        try {
            // Read context-location tags
            fileName = "./data/output_per_img.tsv";
            BufferedReader br = new BufferedReader(new FileReader(fileName));

            while ((line = br.readLine()) != null) {
                // process the line.

                String[] rawArray = line.split("\t");
                String strTag = rawArray[2];
                strTag = strTag.substring(1,strTag.length()-1);

                List<String> tagsArray = parseTagsofImage(strTag);
                ArrayList<String> new_tagsArray = new ArrayList<>();

                for (String tag: tagsArray) {
                    if (entities2WordNet.keySet().contains(tag)){
                        new_tagsArray.add(entities2WordNet.get(tag));
                    } else {
                        new_tagsArray.add(tag);
                    }
                }

                String strOutputLine = rawArray[0]+"\t"+rawArray[1]+"\t" + new_tagsArray.toString();

                IOUtilities.appendLinetoFile(strOutputLine, outputFileName);
            }


        } catch (IOException exception) {
            logger.error("Error: failed to read a line from " + fileName);
            exception.printStackTrace();
        }
    }

    private ReplaceEntitiestoWordNet(){
        loadEntities2WordNetMapping(entities2WordNet);
    }

    public static void main(String[] args){
        try {
            // Database setup
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            return;
        }

        ReplaceEntitiestoWordNet replaceEntitiestoWordNet = new ReplaceEntitiestoWordNet();
        replaceEntitiestoWordNet.startWorking();

    }
}
