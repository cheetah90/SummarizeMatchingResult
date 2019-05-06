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

    private List<String> loadArrayListFromtoString(String strLine, String leftDelimiter, String rightDelimiter) {
        strLine = strLine.substring(1,strLine.length()-1);

        ArrayList<String> tagsList = new ArrayList<>();
        String delimiter = rightDelimiter + ", \\" + leftDelimiter;
        String[] tagsParsed = strLine.split(delimiter);
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

    private boolean isParentCats(String tag) {
        return tag.startsWith("<[{");
    }

    private void startWorking(){
        String outputFileName = "./output/replaced_entities_per_img_parcat.tsv";

        IOUtilities.clearOutputfile(outputFileName);

        String line;
        String fileName="";

        try {
            // Read output file
            fileName = "./data/output_per_img_parcat.tsv";
            BufferedReader br = new BufferedReader(new FileReader(fileName));

            int counter = 0;

            while ((line = br.readLine()) != null) {
                if (counter % 100000 == 0 ) {
                    logger.info("Finished processing line: " + counter);
                }

                // process the linee

                String[] rawArray = line.split("\t");
                if (rawArray.length == 3) {
                    String strTag = rawArray[2];


                    List<String> tagsArray = loadArrayListFromtoString(strTag, "<", ">");
                    ArrayList<String> new_tagsArray = new ArrayList<>();

                    for (String tag: tagsArray) {
                        if (isParentCats(tag)) {
                            tag = tag.substring(1,tag.length()-1);
                            List<String> parentTags = new ArrayList<>();
                            for (String parentTag: loadArrayListFromtoString(tag, "{", "}")){
                                if (entities2WordNet.keySet().contains(parentTag)){
                                    parentTags.add(entities2WordNet.get(parentTag.replace('{', '<').replace('}', '>')));
                                } else {
                                    parentTags.add(parentTag.replace('<', '{').replace('>', '}'));
                                }
                            }

                            new_tagsArray.add("<" + parentTags.toString() + ">");
                        } else {
                            if (entities2WordNet.keySet().contains(tag)){
                                new_tagsArray.add(entities2WordNet.get(tag));
                            } else {
                                new_tagsArray.add(tag);
                            }
                        }
                    }

                    String strOutputLine = rawArray[0]+"\t"+rawArray[1]+"\t" + new_tagsArray.toString();

                    IOUtilities.appendLinetoFile(strOutputLine, outputFileName);

                    counter ++;
                } else {
                    logger.error("Error: this line is malformated." + line);
                }
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
