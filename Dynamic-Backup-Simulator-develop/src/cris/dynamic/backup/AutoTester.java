package cris.dynamic.backup;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;

import cris.dynamic.backup.randomizer.ConstraintsRandomizer;
import cris.dynamic.backup.randomizer.SystemRandomizer;

public class AutoTester {

    private static String     initialDirectory        = "G:\\Documents\\ResearchData\\Dataset3";

    private static String[]   algorithmList           = new String[] { "DynamicAlgorithmNoAffinity" };
    //    private static String[]   algorithmList           = new String[] { "DynamicAlgorithmNoAffinity" };

    private static String[][] systemSizes          = new String[][] { //{num_clients, num_servers}
        { "100", "2" },
        { "100", "4" },
        { "100", "6" },
        { "200", "2" },
        { "200", "4" },
        { "200", "6" },
        { "300", "2" },
        { "300", "4" },
        { "300", "6" }
    };

    private static int        numRandomSystems        = 3;

    private static String[]   numRandomConstraints = new String[] { "1", "1/2", "all" };

    private static int        numDifferentConstraints = 3;

    public static void generateData(final File inputsDir) throws IOException {

        /**
         * http://www.avajava.com/tutorials/lessons/how-do-i-use-a-filefilter-to-display-only-the-directories-within-a-directory.html
         */
        final FileFilter directoryFilter = new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        };

        /**
         * Create the output folder
         */
        final String outputDirName = inputsDir.getAbsolutePath().replace("inputs", "output");
        System.out.println(outputDirName);
        final File outputDir = new File(outputDirName);
        outputDir.mkdir();

        /**
         * For each algorithm, copy input files tree and run simulations
         */
        for (int a = 0; a < algorithmList.length; a++) {
            final String algorithmDirName = FilenameUtils.concat(outputDirName, algorithmList[a]);
            final File algorithmDir = new File(algorithmDirName);
            FileUtils.copyDirectory(inputsDir, algorithmDir, DirectoryFileFilter.DIRECTORY);


            /**
             * As you traverse the tree in the input folder, keep track of the
             * Analogous location in the output folder.
             */
            final File[] systemSizeFolders = inputsDir.listFiles(directoryFilter);
            for (int i = 0; i < systemSizeFolders.length; i++) {

                System.out.println("Starting Size folder: " + (i + 1) + " / " + systemSizeFolders.length);
                final File[] systemFolders = systemSizeFolders[i].listFiles(directoryFilter);
                final String outputSystemFolder = FilenameUtils.concat(algorithmDir.getAbsolutePath(), systemSizeFolders[i].getName());

                for (int j = 0; j < systemFolders.length; j++) {

                    System.out.println("      Starting system: " + (j + 1) + " / " + systemFolders.length);
                    final File[] inputFiles = systemFolders[j].listFiles();
                    final String outputFile = FilenameUtils.concat(outputSystemFolder, systemFolders[j].getName());

                    /**
                     * Go through the constraint input files and make a list of them
                     * and notate the system file.
                     */
                    String systemFile = "";
                    String systemFileName = "";
                    final ArrayList<String> constraintFiles = new ArrayList<String>();
                    for (int k = 0; k < inputFiles.length; k++) {
                        final File inputFile = inputFiles[k];
                        if (FilenameUtils.isExtension(inputFile.getAbsolutePath(), "system")) {
                            systemFile = inputFile.getAbsolutePath();
                            systemFileName = FilenameUtils.getBaseName(inputFile.getName());
                        } else {
                            constraintFiles.add(inputFile.getAbsolutePath());
                        }
                    }

                    /**
                     * Go through each constraint file and run a simulation.
                     */
                    for (int k = 0; k < constraintFiles.size(); k++) {
                        System.out.println("          const: " + (k + 1) + " / " + constraintFiles.size());
                        DynamicBackupSimulator.runSimulation(systemFile, constraintFiles.get(k), algorithmList[a],
                                FilenameUtils.concat(outputFile, systemFileName + "_try" + String.valueOf(k) + ".log"));
                    }

                }

                System.out.println("Completed Size folder: " + (i + 1) + " / " + systemSizeFolders.length);
            }
        }

    }

    public static File generateInputFiles() throws NumberFormatException, IOException {
        final File initialDirectoryFile = new File(initialDirectory);
        if (!initialDirectoryFile.exists()) {
            if (!initialDirectoryFile.mkdir()) {
                System.out.println("Failed to create directory!");
                throw new RuntimeException("directory already exists");
            }
        }


        /**
         * First, generate input files
         */
        final String inputFileName = FilenameUtils.concat(initialDirectory, "inputs");
        final File inputFile = new File(inputFileName);
        inputFile.mkdir();

        /**
         * for each system
         */
        for (int j = 0; j < systemSizes.length; j++) {
            final String[] currentSize = systemSizes[j];
            final String systemString = currentSize[0] + "_" + currentSize[1];

            final String systemFileName = FilenameUtils.concat(inputFileName, systemString);
            final File systemFile = new File(systemFileName);
            systemFile.mkdir();

            /**
             * for each different randomized system of the above size
             */
            for (int k = 0; k < numRandomSystems; k++) {
                final String randomSystemFileName = FilenameUtils.concat(systemFileName, "sys_" + String.valueOf(k));
                final File randomSystemFile = new File(randomSystemFileName);
                randomSystemFile.mkdir();

                //create the system file
                SystemRandomizer.generateSystemFile(FilenameUtils.concat(randomSystemFileName, "system_" + String.valueOf(k)), Integer.parseInt(currentSize[1]),
                        Integer.parseInt(currentSize[1]), Integer.parseInt(currentSize[0]));

                /**
                 * For each number of constraints, make N constraint files
                 */
                for (int l = 0; l < numRandomConstraints.length; l++) {
                    int numConstraints = 0;
                    switch (numRandomConstraints[l]) {
                    case "1":
                        numConstraints = 1;
                        break;
                    case "1/2":
                        numConstraints = (int) (.5 * Integer.parseInt(currentSize[1]));
                        break;
                    case "all":
                        numConstraints = Integer.parseInt(currentSize[1]);
                        break;
                    default:
                        throw new RuntimeException("numRandomConstraints: " + numRandomConstraints[l] + " not added to switch.");
                    }

                    for (int m = 0; m < numDifferentConstraints; m++) {
                        ConstraintsRandomizer.generateConstraints(FilenameUtils.concat(randomSystemFileName, "system_" + String.valueOf(k) + ".system"), numConstraints);
                    }
                }
            }
        }

        System.out.println("Input files generated.");
        return inputFile;
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        //        final File inputFiles = generateInputFiles();

        System.out.println("Starting simulation.");
        final File inputFiles = new File("./inputs");
        //        inputFiles = new File("G:\\Documents\\ResearchData\\Dataset1\\input");
        generateData(inputFiles);

        //        final File initialDirectoryFile = new File(initialDirectory);
        //        if (!initialDirectoryFile.exists()) {
        //            if (!initialDirectoryFile.mkdir()) {
        //                System.out.println("Failed to create directory!");
        //                throw new RuntimeException("directory already exists");
        //            }
        //        }
        //
        //        final String outputFileName = FilenameUtils.concat(initialDirectory, "output");
        //        final File outputFile = new File(outputFileName);
        //        outputFile.mkdir();
        //
        //        /**
        //         * for each algorithm
        //         */
        //        for (int i = 0; i < algorithmList.length; i++) {
        //            final String currentAlgorithm = algorithmList[i];
        //
        //            final String algorithmFileName = FilenameUtils.concat(outputFileName, currentAlgorithm);
        //            final File algorithmFile = new File(algorithmFileName);
        //            algorithmFile.mkdir();
        //
        //            /**
        //             * for each system
        //             */
        //            for(int j = 0; j < systemSizes.length; j++){
        //                final String[] currentSize = systemSizes[j];
        //                final String systemString = currentSize[0] + "_" + currentSize[1];
        //
        //                final String systemFileName = FilenameUtils.concat(algorithmFileName, systemString);
        //                final File systemFile = new File(systemFileName);
        //                systemFile.mkdir();
        //
        //                /**
        //                 * for each different randomized system of the above size
        //                 */
        //                for (int k = 0; k < numRandomSystems; k++) {
        //                    final String randomSystemFileName = FilenameUtils.concat(systemFileName, "sys_" + String.valueOf(k));
        //                    final File randomSystemFile = new File(randomSystemFileName);
        //                    randomSystemFile.mkdir();
        //
        //                    String inputsDirectory = FilenameUtils.concat(initialDirectory, inputFileName);
        //                    inputsDirectory = FilenameUtils.concat(inputsDirectory, systemString);
        //                    inputsDirectory = FilenameUtils.concat(inputsDirectory, "sys_" + String.valueOf(k));
        //
        //                    final List<String> constraintFiles = new ArrayList<String>();
        //                    String sysFilePath = "";
        //                    String sysFileName = "";
        //                    final File[] files = new File(inputsDirectory).listFiles();
        //                    for (final File file : files) {
        //                        if (file.isFile()) {
        //                            if (file.getName().contains(".system")) {
        //                                sysFilePath = file.getAbsolutePath();
        //                                sysFileName = FilenameUtils.removeExtension(file.getName());
        //                            } else {
        //                                constraintFiles.add(file.getAbsolutePath());
        //                            }
        //                        }
        //                    }
        //                    System.out.println("sysFile: " + sysFilePath);
        //                    for (int l = 0; l < constraintFiles.size(); l++) {
        //                        DynamicBackupSimulator.runSimulation(sysFilePath, constraintFiles.get(l), currentAlgorithm, FilenameUtils.concat(randomSystemFileName, sysFileName + "_" + String.valueOf(l) + ".log"));
        //                    }
        //
        //                }
        //            }
        //
        //
        //
        //        }

        //        DynamicBackupSimulator.runSimulation("SampleInputFile.txt", "SampleInputFile_4.constraint", "DynamicAlgorithmV3");
    }
}
