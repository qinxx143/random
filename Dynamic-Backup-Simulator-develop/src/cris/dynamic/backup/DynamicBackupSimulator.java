package cris.dynamic.backup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.FilenameUtils;

import cris.dynamic.backup.algorithm.DynamicAlgorithmNoAffinity;
import cris.dynamic.backup.algorithm.DynamicAlgorithmSizeUnknown;
import cris.dynamic.backup.algorithm.DynamicAlgorithmV3;
import cris.dynamic.backup.algorithm.RandomWithMaxV2;
import cris.dynamic.backup.algorithm.Scheduler;
import cris.dynamic.backup.system.BackupSystem;

public class DynamicBackupSimulator {

    private static final int    stepSize             = 100;                                       //milliseconds per step

    private static final int    iterations           = 5;

    private static final String scheduler            = "DynamicAlgorithmV3";   //             = new DynamicAlgorithmV3();
    //	private static final Scheduler	scheduler				= new RandomWithMaxV2(15);

    //    private static final int       randomizedConstraints = 3;
    private static final int    windowSizeMultiplier = -1;

    private static final long   overallBackupWindow  = -1;                                        //overall backup window size in milliseconds

    private static final String systemConfigFile     = "system_3.system";
    private static final String systemConstraintFile = "system_3_1.constraint";
    private static final String dataLogFile          = "./logFile.csv";

    public static void main(String[] args) throws IOException {

        final String outputFile = generateOutputFileName(systemConfigFile);
        runSimulation(systemConfigFile, systemConstraintFile, scheduler, outputFile);

    }

    public static void runSimulation(final String systemConfigFile, final String systemConstraintFile,
            final String schedulerString, final String outputFile) throws FileNotFoundException, UnsupportedEncodingException, IOException {

        final PrintWriter writer = new PrintWriter(outputFile, "UTF-8");

        final BackupSystem system = new BackupSystem(writer, systemConfigFile, systemConstraintFile, getScheduler(schedulerString),
                windowSizeMultiplier, overallBackupWindow);
        simulate(system, iterations);
        system.printFinalOutput(systemConfigFile, systemConstraintFile, outputFile, dataLogFile, iterations);
        writer.close();
    }

    public static void simulate(final BackupSystem system, int iterations) {
        for (int i = 0; i < iterations; i++) {
            while (system.getCompletedBackups() < system.getTotalBackups()) {
                system.step(stepSize);
            }
            system.writeCompletionStatistics();
            if (i < iterations - 1) {
                system.nextIteration();
            }
        }
    }

    private static String generateOutputFileName(final String inputFile) {
        //Let the outputFile name be the inputFile + _N where N counts up.
        String inputFileName = FilenameUtils.getName(inputFile);
        inputFileName = FilenameUtils.removeExtension(inputFileName);
        String outputFileName = "";
        boolean fileExists = true;
        int n = 1;
        while (fileExists) {
            final File f = new File(inputFileName + "_" + n + ".log");
            if (!f.exists()) {
                outputFileName = inputFileName + "_" + n + ".log";
                fileExists = false;
            }
            ++n;
        }
        return outputFileName;
    }

    private static Scheduler getScheduler(String schedulerString) {
        switch (schedulerString) {
        case "RandomWithMaxV2":
            return new RandomWithMaxV2(15);
        case "DynamicAlgorithmV3":
            return new DynamicAlgorithmV3();
        case "DynamicAlgorithmNoAffinity":
            return new DynamicAlgorithmNoAffinity();
        case "DynamicAlgorithmSizeUnknown":
            return new DynamicAlgorithmSizeUnknown();
        default:
            throw new RuntimeException("Unsupported Algorithm: " + schedulerString);

        }

    }

}
