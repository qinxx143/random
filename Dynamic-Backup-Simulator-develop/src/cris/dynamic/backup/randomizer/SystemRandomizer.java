package cris.dynamic.backup.randomizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

import org.apache.commons.math3.distribution.ParetoDistribution;

import cris.dynamic.backup.system.Helper;

public class SystemRandomizer {
    private static String      outputFileName             = "system_5";

    //Settings
    private static int         numServers                 = 2;

    private static int         numStorageUnits            = 3;

    private static double      stuMeanThroughput          = 100;

    private static double      stuThroughputDeviation     = 20;

    private static int         stuMaxData                 = 1000 * 10;

    public static int         numClients                 = 10;

    private static double      clientsMeanThroughput      = 75;

    private static double      clientsThroughputDeviation = 10;

    private static int         backupsPerClient           = 1;
    //Pareto Distribution
    private static double      backupsScale               = 100000;
    private static double      backupsShape               = 2;

    private static String      throughputVariance         = ".05";
    
    private static int         RPORange                   =5;
    

    private static PrintWriter writer;
    private static int         currentServerNum;
    private static int         currentStorageUnitNum;
    private static int         currentClientNum;
    private static int         currentBackupNum;

    /**
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    public static void generateSystemFile(final String outputFileName, final int numServers, final int numStorageUnits, final int numClients)
            throws FileNotFoundException, UnsupportedEncodingException {
        currentServerNum = 0;
        currentStorageUnitNum = 0;
        currentClientNum = 0;
        currentBackupNum = 0;

        final File f = new File(outputFileName + ".system");
        if (f.exists()) {
            throw new RuntimeException("A file of that name already exists. Delete file or change outputFile name in randomizer.");
        }
        writer = new PrintWriter(outputFileName + ".system", "UTF-8");

        writeParameters(outputFileName, numServers, numStorageUnits, numClients);

        generateServers(numServers);

        generateStorageUnits(numStorageUnits, numServers);

        generateClients(numClients);

        generateBackups(numClients);

        writer.close();
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

        generateSystemFile(outputFileName, numServers, numStorageUnits, numClients);
    }

    private static void generateBackups(final int numClients) {
        if (backupsPerClient < 1) {
            throw new RuntimeException("You need at least 1 backup per client");
        }

        writer.println("#Backups");
        writer.println("#backup(*name*, *client*, *data_size(MB)*，criticality, *RTO*(s), *RPO(day)*)");

        for (int i = 0; i < backupsPerClient; i++) { //for each backup per client
            int tempClientNum = 0;
            for (int j = 0; j < numClients; j++) { //for each client
                final String backupName = "backup" + currentBackupNum;
                currentBackupNum++;
                final String associatedClient = "client" + tempClientNum;
                tempClientNum++;

                double dataSize = -1;
                while (dataSize < 0.1) {
                    dataSize = getParetoValue(backupsScale, backupsShape);
                }
                
                double criticality= 0.5 * Math.random() + 0.5;
                String RTO = Helper.convertToTimestamp(getRTO(dataSize, criticality) * 1000);
                
                int RPO = getRPO(criticality);

                writer.println("backup(" + backupName + ", " + associatedClient + ", " + String.valueOf(dataSize) + ", " + criticality + ", " + RTO +", " + RPO + ")");
            }

        }

    }

    private static void generateClients(final int numClients) {
        if (numClients < 1) {
            throw new RuntimeException("You need at least 1 client");
        }

        writer.println("#Clients");
        writer.println("#client(*name*, *throughput(MB/s)*, *throughput_variance(%)*)");

        final Random randomizer = new Random();
        for (int i = 0; i < numClients; i++) {
            final String clientName = "client" + currentClientNum;
            currentClientNum++;
            double throughput = -1;
            while (throughput < 0.1) {
                throughput = getGaussianValue(clientsMeanThroughput, clientsThroughputDeviation, randomizer);
            }
            writer.println("client(" + clientName + ", " + String.valueOf(throughput) + ", " + throughputVariance + ")");
        }
        writer.println();

    }

    private static void generateServers(final int numServers) {
        if (numServers < 1) {
            throw new RuntimeException("Must have at least 1 server.");
        }

        writer.println("#Media Servers");
        writer.println("#media_server(*name*)");

        for (int i = 0; i < numServers; i++) {
            writer.println("media_server(server" + currentServerNum + ")");
            currentServerNum++;
        }
        writer.println();

    }

    private static void generateStorageUnits(final int numStorageUnits, final int numServers) {
        if (numStorageUnits < numServers) {
            throw new RuntimeException("There is less than one storage unit per server.");
        }

        if (numStorageUnits < 1) {
            throw new RuntimeException("You need at least 1 storage unit");
        }

        writer.println("#Storage Units");
        writer.println("#stu(*name*,*media_server_name*, *throughput(MB/s)*, *throughput_variance(%)*, *max_data(MB)*, *current_data(MB)*)");

        int tempServerNum = 0;
        final Random randomizer = new Random();
        for (int i = 0; i < numStorageUnits; i++) {
            final String storageName = "storage" + currentStorageUnitNum;
            final String serverName = "server" + tempServerNum;
            double throughput = -1;
            while (throughput < 0.1) {
                throughput = getGaussianValue(stuMeanThroughput, stuThroughputDeviation, randomizer);
            }
            writer.println("stu(" + storageName + ", " + serverName + ", " + String.valueOf(throughput) + ", " + throughputVariance + ", " + stuMaxData + " , 0)");

            if (tempServerNum == numServers - 1 || numServers == 1) {
                tempServerNum = 0;
            } else {
                tempServerNum++;
            }
            currentStorageUnitNum++;
        }
        writer.println();

    }

    private static double getGaussianValue(double mean, double variance, Random random) {
        return mean + random.nextGaussian() * variance;
    }

    private static double getParetoValue(double backupsScale,
            double backupsShape) {
        final ParetoDistribution distribution = new ParetoDistribution(backupsScale, backupsShape);
        return distribution.sample();
    }
    
    private static long getRTO(double dataSize, double criticality ) {
    		return (long) ((dataSize / (stuMeanThroughput /5) ) * (1.5-criticality));
    }
    
    private static int getRPO(double criticality) {
    		int result = (int) ((1 - criticality) * 10);
    		return result + 1;
    }
  

    private static void writeParameters(final String outputFileName, final int numServers, final int numStorageUnits, final int numClients) {
        writer.println("#outputFileName = " + outputFileName);
        writer.println("#numServers = " + numServers);
        writer.println("#numStorageUnits = " + numStorageUnits);
        writer.println("#stuMeanThroughput = " + stuMeanThroughput);
        writer.println("#stuMaxData = " + stuMaxData);
        writer.println("#numClients = " + numClients);
        writer.println("#clientsMeanThroughput = " + clientsMeanThroughput);
        writer.println("#clientsThroughputDeviation = " + clientsThroughputDeviation);
        writer.println("#backupsPerClient = " + backupsPerClient);
        writer.println("#backupsScale = " + backupsScale);
        writer.println("#backupsShape = " + backupsShape);
        //		writer.println("#backupsMeanSize = " + backupsMeanSize);
        //		writer.println("#backupsSizeDeviation = " + backupsSizeDeviation);
        writer.println("#throughputVariance = " + throughputVariance);
        writer.println("#backupsScale = " + backupsScale);
        writer.println("");

    }
}
