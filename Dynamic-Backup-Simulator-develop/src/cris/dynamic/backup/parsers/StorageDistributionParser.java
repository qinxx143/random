package cris.dynamic.backup.parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class StorageDistributionParser {

    //    private static String      inputFile  = "G:\\Documents\\ResearchData\\GoodData\\DatasetDynamic\\DynamicAlgorithmV3\\100_6\\sys_2\\system_2_try6.log";
    //    private static String      inputFile  = "G:\\Documents\\ResearchData\\GoodData\\DatasetRandom\\RandomWithMaxV2\\100_6\\sys_2\\system_2_try6.log";
    private static String      inputFile  = "G:\\Documents\\ResearchData\\GoodData\\DatsetNoAffinity\\output\\DynamicAlgorithmNoAffinity\\100_6\\sys_2\\system_2_try6.log";
    private static String      outputFile = "C:\\Users\\Brandon\\Desktop\\new_distributionNoAffinity.csv";
    //    private static String      inputFile  = "system6_100/system6_100_5_3.log";
    //    private static String      outputFile = "distribution.csv";

    private static PrintWriter writer;

    public static void main(final String[] args) throws IOException {
        //map with each backup that holds a map of each storage device seen with a frequency
        final HashMap<String, HashMap<String, Integer>> backupKeyMap = new HashMap<String, HashMap<String, Integer>>(); //<BackupName, HashMap<StorageName, Count>>
        final HashSet<String> storageNames = new HashSet<String>();

        BufferedReader reader = null;
        String line = "";
        try {
            reader = new BufferedReader(new FileReader(inputFile));

            while ((line = reader.readLine()) != null) {
                //do the thing
                if (line.contains("started to")) {
                    final String[] splitLine = line.split(" ");
                    final String backupName = splitLine[2];
                    final String storageName = splitLine[8];
                    storageNames.add(storageName);
                    if (backupKeyMap.containsKey(backupName)) {
                        final HashMap<String, Integer> storageKeyMap = backupKeyMap.get(backupName);
                        if (storageKeyMap.containsKey(storageName)) {
                            storageKeyMap.put(storageName, storageKeyMap.get(storageName) + 1); //increment count
                        } else {
                            storageKeyMap.put(storageName, 1);
                        }
                    } else {
                        final HashMap<String, Integer> countMap = new HashMap<String, Integer>();
                        countMap.put(storageName, 1);
                        backupKeyMap.put(backupName, countMap);
                    }
                }
            }
            //print
            writer = new PrintWriter(outputFile, "UTF-8");

            //header
            writer.print("backupName,");
            final ArrayList<String> storageNamesList = new ArrayList<String>(storageNames);
            for (int i = 0; i < storageNamesList.size(); i++) {
                writer.print(storageNamesList.get(i) + ",");
            }
            writer.print("\n");

            for (final Map.Entry<String, HashMap<String, Integer>> backupEntry : backupKeyMap.entrySet()) {
                writer.print(backupEntry.getKey() + ",");
                for (int i = 0; i < storageNamesList.size(); i++) {
                    if (backupEntry.getValue().containsKey(storageNamesList.get(i))) {
                        writer.print(backupEntry.getValue().get(storageNamesList.get(i)));
                    }
                    writer.print(",");
                }
                writer.print("\n");
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
