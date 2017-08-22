package cris.dynamic.backup.randomizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;

public class ConstraintsRandomizer {

    private static String                               inputFileName               = "system.system";

    private static PrintWriter                          writer;

    /**
     * Storage Randomization
     */
    private static boolean                              randomizeStorage            = true;
    private static int                                  numRandomStorageConstraints = 2;

    private static boolean                              randomizeServer             = false;
    private static boolean                              randomizeStartTime          = false;
    private static boolean                              randomizeEndTime            = false;

    //	private final ArrayList<String>	servers						= new ArrayList<String>();
    private static ArrayList<String>              storageDevices;

    private static ArrayList<String>              backups;

    private static Map<String, ArrayList<String>> storageConstraints;

    /**
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    public static void generateConstraints(final String inputFileName, final int numRandomStorageConstraints)
            throws FileNotFoundException, UnsupportedEncodingException, IOException {
        final String outputFileName = generateOutputFileName(inputFileName);
        storageDevices = new ArrayList<String>();
        backups = new ArrayList<String>();
        storageConstraints = new HashMap<String, ArrayList<String>>();

        writer = new PrintWriter(outputFileName, "UTF-8");
        writer.println("#" + outputFileName);
        writer.println("#Associated with: " + inputFileName);

        parseSystem(inputFileName);

        if (numRandomStorageConstraints < storageDevices.size()) {
            randomizeStorageConstraints(numRandomStorageConstraints);
            randomizeServerConstraints();
            randomizeStartTimeConstraint();
            randomizeEndTimeConstraint();
        }

        /*
         * Print constraints
         */
        writer.println("RandomizedStorageConstraints=" + numRandomStorageConstraints);
        for (final String backupName : backups) {
            if (numRandomStorageConstraints < storageDevices.size()) {
                writer.println(backupName + "(" + getStorageString(backupName) + ", " + getServerString(backupName) +
                        ", " + getStartString(backupName) + ", " + getEndString(backupName) + ")");
            } else {
                writer.println(backupName + "( *, " + getServerString(backupName) +
                        ", " + getStartString(backupName) + ", " + getEndString(backupName) + ")");
            }

        }

        writer.close();
    }

    public static void main(String[] args) throws IOException {

        generateConstraints(inputFileName, numRandomStorageConstraints);
    }

    private static String generateOutputFileName(final String inputFile) {
        //Let the outputFile name be the inputFile + _N where N counts up.
        final String inputFileName = FilenameUtils.removeExtension(inputFile);
        String outputFileName = "";
        boolean fileExists = true;
        int n = 1;
        while (fileExists) {
            final File f = new File(inputFileName + "_" + n + ".constraint");
            if (!f.exists()) {
                outputFileName = inputFileName + "_" + n + ".constraint";
                fileExists = false;
            }
            ++n;
        }
        return outputFileName;
    }

    private static String getEndString(String backupName) {
        // TODO
        return "*";
    }

    private static String getServerString(String backupName) {
        // TODO
        return "*";
    }

    private static String getStartString(String backupName) {
        // TODO
        return "*";
    }

    private static String getStorageString(String backupName) {
        final ArrayList<String> stus = storageConstraints.get(backupName);
        if (stus.size() == 0) {
            return "*";
        }
        String toReturn = "{";
        toReturn += stus.get(0);
        for (int i = 1; i < stus.size(); i++) {
            toReturn += " | " + stus.get(i);
        }
        toReturn += "}";
        return toReturn;
    }

    private static void giveRandomStorageConstraint(final String backupName, final ArrayList<String> storageList, final int numConstraints) {
        final Random randomGen = new Random();
        int numAdded = 0;
        while (numAdded != numConstraints) {
            if (storageList.size() == 0) {

                throw new RuntimeException("Unable to add the requested number of valid storage units for backup: " + backupName + ".");
            }
            final int randomIndex = randomGen.nextInt(storageList.size());
            //Add it and increment number added
            storageConstraints.get(backupName).add(storageList.get(randomIndex));
            ++numAdded;
            storageList.remove(randomIndex);
        }
    }

    private static void parseBackup(final String line) {
        String tempLine = line.substring(line.indexOf('(') + 1, line.indexOf(')')); //get data without parenthesis
        tempLine = tempLine.replaceAll("\\s+", ""); //remove all white space leaving comma separated values
        final String[] splitLine = tempLine.split(",");

        backups.add(splitLine[0]);
        storageConstraints.put(splitLine[0], new ArrayList<String>());

    }

    private static void parseLine(String line, int lineCount) {
        final String type = line.substring(0, line.indexOf('('));

        switch (type) {
        case "media_server":
            //Do nothing atm
            break;
        case "stu":
            parseStorageDevice(line);
            break;
        case "client":
            //Do nothing atm
            break;
        case "backup":
            parseBackup(line);
            break;
        default:
            throw new RuntimeException("Invalid input type line: " + lineCount + " type: " + type);
        }
    }

    private static void parseStorageDevice(final String line) {
        String tempLine = line.substring(line.indexOf('(') + 1, line.indexOf(')')); //get data without parenthesis
        tempLine = tempLine.replaceAll("\\s+", ""); //remove all white space leaving comma separated values
        final String[] splitLine = tempLine.split(",");

        storageDevices.add(splitLine[0]); //just store the name
    }

    private static void parseSystem(String filePath) throws IOException {
        BufferedReader reader = null;
        String line = "";
        int lineCount = 0;
        try {
            reader = new BufferedReader(new FileReader(filePath));

            while ((line = reader.readLine()) != null) {
                //do the thing
                line = line.trim();
                if (!"".equals(line) && line.charAt(0) != '#') { //ignore this line
                    parseLine(line, lineCount);
                }
                lineCount++; //increment line count for error message
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

    }

    private static void randomizeEndTimeConstraint() {
        if (randomizeEndTime) {
            throw new RuntimeException("Randomize End Constraint not implemented.");
        }

    }

    private static void randomizeServerConstraints() {
        if (randomizeServer) {
            throw new RuntimeException("Randomize Server not implemented.");
        }

    }

    private static void randomizeStartTimeConstraint() {
        if (randomizeStartTime) {
            throw new RuntimeException("Randomize Start Constraint not implemented.");
        }

    }

    private static void randomizeStorageConstraints(final int numRandomStorageConstraints) {
        if (randomizeStorage) {
            for (final String backupName : backups) {
                giveRandomStorageConstraint(backupName, new ArrayList<String>(storageDevices), numRandomStorageConstraints);
            }
        }

    }

}
