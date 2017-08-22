package cris.dynamic.backup.parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class BackupLengthDistributionParser {

    //    private static String      inputFile  = "G:\\Documents\\ResearchData\\GoodData\\DatasetDynamic\\DynamicAlgorithmV3\\200_6\\sys_0\\system_0_try6.log";
    //    private static String      inputFile  = "G:\\Documents\\ResearchData\\GoodData\\DatasetRandom\\RandomWithMaxV2\\100_6\\sys_2\\system_2_try6.log";
    private static String      inputFile  = "G:\\Documents\\ResearchData\\GoodData\\DatsetNoAffinity\\output\\DynamicAlgorithmNoAffinity\\200_6\\sys_0\\system_0_try6.log";
    private static String      outputFile = "C:\\Users\\Brandon\\Desktop\\3.csv";

    private static PrintWriter writer;

    public static void main(final String[] args) throws IOException {
        final ArrayList<Integer> timeList = new ArrayList<Integer>();


        BufferedReader reader = null;
        String line = "";
        try {
            reader = new BufferedReader(new FileReader(inputFile));

            while ((line = reader.readLine()) != null) {
                //do the thing
                if (line.contains("All backups completed.")) {
                    final String[] splitLine = line.split(" ");
                    final String timestamp = splitLine[0];
                    final String[] splitTimeStamp = timestamp.split(":");
                    final int time = 3600 * Integer.parseInt(splitTimeStamp[0]) + 60 * Integer.parseInt(splitTimeStamp[1]) + Integer.parseInt(splitTimeStamp[2]);
                    timeList.add(time);
                }
            }
            //print
            writer = new PrintWriter(outputFile, "UTF-8");

            //header
            writer.println("day,value");

            //
            for (int i = 0; i < timeList.size(); i++) {
                writer.println((i + 1) + "," + timeList.get(i));
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
