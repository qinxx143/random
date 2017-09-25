package cris.dynamic.backup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

import com.sun.istack.internal.FinalArrayList;

import cris.dynamic.backup.algorithm.DynamicAlgorithmNoAffinity;
import cris.dynamic.backup.algorithm.DynamicAlgorithmSizeUnknown;
import cris.dynamic.backup.algorithm.DynamicAlgorithmV3;
import cris.dynamic.backup.algorithm.RandomWithMaxV2;
import cris.dynamic.backup.algorithm.Scheduler;
import cris.dynamic.backup.system.BackupSystem;
import cris.dynamic.backup.system.RestoreSystem;

public class DynamicBackupSimulator {

    private static final int    stepSize             = 100;                                       //milliseconds per step

    public static final int    iterations           = 5;

    private static final String scheduler            = "DynamicAlgorithmV3";   //             = new DynamicAlgorithmV3();

    private static final int    windowSizeMultiplier = -1;

    private static final long   overallBackupWindow  = -1;                                        //overall backup window size in milliseconds

    private static final String systemConfigFile     = "system_4.system";
    private static final String systemConstraintFile = "system_4_1.constraint";
    private static final String systemRestoreFile    = "restore.system";
    private static final String dataLogFile          = "./logFile.csv";

    public static PrintWriter allWriter;
    
    public static void main(String[] args) throws Exception {

        final String outputFile = generateOutputFileName(systemConfigFile);
        runSimulation(systemConfigFile, systemConstraintFile, scheduler, outputFile);
        
        //test restore
//		DynamicAlgorithmV3 scheduler= new DynamicAlgorithmV3();
//		try {
//			RestoreSystem restoreSystem = new RestoreSystem("restore.system");
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}	//TODO map cannot put the same restore for different days
//		scheduler.getNewRestores(0);

    }

    public static void runSimulation(final String systemConfigFile, final String systemConstraintFile,
            final String schedulerString, final String outputFile) throws Exception {

        final PrintWriter writer = new PrintWriter(outputFile, "UTF-8");
        final PrintWriter snapshotChainsWriter = new PrintWriter("snapshotChain.log", "UTF-8");
        final PrintWriter recoverWriter = new PrintWriter("recover.log", "UTF-8");

        Scheduler scheduler = getScheduler(schedulerString);
        final BackupSystem system = new BackupSystem(writer,snapshotChainsWriter, systemConfigFile, systemConstraintFile, scheduler,
                windowSizeMultiplier, overallBackupWindow);
        final RestoreSystem restoreSystem = new RestoreSystem(recoverWriter,systemRestoreFile,scheduler);
        
        simulate(system, restoreSystem, iterations);

        system.printFinalOutput(systemConfigFile, systemConstraintFile, outputFile, dataLogFile, iterations);
        writer.close();
        snapshotChainsWriter.close();
        recoverWriter.close();
    }

    public static void simulate(final BackupSystem system, final RestoreSystem restoreSystem, int iterations) {
        for (int i = 0; i < iterations; i++) {
       	
      	long maxTime = ((24*60*60*1000)/stepSize)-1;
      	for(int time=0;time < maxTime;time++){
      		if(system.getCompletedBackups() < system.getTotalBackups()){
      			system.step(stepSize);
      		}
      		if(restoreSystem.getCompletedBackups() < restoreSystem.getTotalBackups()){
      			restoreSystem.step(stepSize);
      			
      		}
      	}
          system.writeCompletionStatistics();
          restoreSystem.writeCompletionStatistics();
          
          if (i < iterations - 1) {
              system.nextIteration();
              restoreSystem.nextIteration();
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
