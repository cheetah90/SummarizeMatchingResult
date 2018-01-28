import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.PreparedStatement;
import java.util.*;

public class AddMissingWordNettoYago {

    private static final Logger logger = LogManager.getLogger(ResultSummarizer.class);

    private final static HashMap<String, HashSet<String>> yagoEntities2Types = new HashMap<>();

    private AddMissingWordNettoYago(){

        IOUtilities.loadYagotoMemory(yagoEntities2Types);

    }

    private void startWorking(){
        String line;
        String fileName="";

        try {
            // Read context-location tags
            fileName = "./data/missing_wordnet.txt";
            BufferedReader br = new BufferedReader(new FileReader(fileName));

            while ((line = br.readLine()) != null) {
                // process the line.
                String[] mapping = line.split("\t");
                String wordnet_synset = mapping[1];

                if (!yagoEntities2Types.containsKey(wordnet_synset)){
                    logger.info(wordnet_synset);
                }

            }


        } catch (IOException exception) {
            logger.error("Error: failed to read a line from " + fileName);
            exception.printStackTrace();
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

        AddMissingWordNettoYago addMissingWordNettoYago = new AddMissingWordNettoYago();
        addMissingWordNettoYago.startWorking();

    }
}
