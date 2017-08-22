package cris.dynamic.backup.parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

public class StorageSwitcherParser {

    //    private static String      inputFile  = "G:\\Documents\\ResearchData\\GoodData\\DatasetDynamic\\DynamicAlgorithmV3\\100_6\\sys_2\\system_2_try6.log";
    //    private static String      inputFile  = "G:\\Documents\\ResearchData\\GoodData\\DatasetRandom\\RandomWithMaxV2\\100_6\\sys_2\\system_2_try6.log";
    private static String      inputFile  = "G:\\Documents\\ResearchData\\GoodData\\DatsetNoAffinity\\output\\DynamicAlgorithmNoAffinity\\100_6\\sys_2\\system_2_try6.log";
    private static String      outputFile = "C:\\Users\\Brandon\\Desktop\\SwitchDistLBF.csv";

    private static PrintWriter writer;

    public static void main(final String[] args) throws IOException {

        final HashMap<String, String> backupMap = new HashMap<String, String>();
        BufferedReader reader = null;
        String line = "";
        try {
            writer = new PrintWriter(outputFile, "UTF-8");
            reader = new BufferedReader(new FileReader(inputFile));

            writer.print(inputFile);
            boolean firstDay = true;
            int switchCount = 0;
            while ((line = reader.readLine()) != null) {
                if (line.contains("All backups completed")) {
                    /**
                     * End of day. Start new day.
                     */
                    writer.print("," + switchCount);
                    firstDay = false;
                    switchCount = 0;
                }
                if (line.contains("started to")) {
                    final String[] splitLine = line.split(" ");
                    final String backupName = splitLine[2];
                    final String storageName = splitLine[8];
                    if (firstDay) {
                        backupMap.put(backupName, storageName);
                    } else {
                        if (!backupMap.get(backupName).equals(storageName)) {
                            /**
                             * Not the same backup. Switch occured.
                             */

                            switchCount++;
                            backupMap.put(backupName, storageName);
                        } //else no switch occurred, nothing required.
                    }
                }

            }

        } finally {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
        }
    }
}
