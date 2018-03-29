package cris.dynamic.backup.system;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.SnapshotChain;
import cris.dynamic.backup.algorithm.NaturalReflex;
import cris.dynamic.backup.algorithm.Scheduler;
import cris.dynamic.backup.client.Client;
import cris.dynamic.backup.infrastructure.Constraint;
import cris.dynamic.backup.infrastructure.Constraints;
import cris.dynamic.backup.infrastructure.Events;
import cris.dynamic.backup.infrastructure.LogEvent;
import cris.dynamic.backup.infrastructure.LogEvent.LogBuilder;
import cris.dynamic.backup.infrastructure.NaturalReflexHistorical;
import cris.dynamic.backup.server.MediaServer;
import cris.dynamic.backup.server.StorageDevice;
import sun.print.PrinterJobWrapper;

public class BackupSystem {

    private static boolean                   isIncremental       = true;

    private Map<String, Backup>              backups;
    private Map<String, Backup>              completedBackupsMap;
    private Map<String, Backup>              wholeBackupsMap;
    private Map<String,Backup>               unBackupsMap;
    private Map<String, SnapshotChain>       backupToSnapshotMap;
    private static Map<String, Map<String, SnapshotChain>>       snapshotChainMap = new HashMap<String,Map<String, SnapshotChain>>(); //day to SnapshotChain
    private Map<String, Integer>             fullBackupFrequencyMap;
    private static Map<String, NaturalReflexHistorical> NaturalMap; 
    private final Map<String, MediaServer>   servers;
    private final Map<String, Client>        clients;
    private final Map<String, StorageDevice> storageDevices;
    private final Constraints                constraints;

    private final HashMap<String, Long>      unutilizedStorageMap;

    private final Scheduler                  scheduler;

    //    private int                              numRemainingBackups;
    private int                              numActiveBackups    = 0;
    private int                              numCompletedBackups = 0;
    private int                              numTotalBackups;
    private int                              iterationNumber     = 1;
    private  static Map<String, Integer>     estimatedRestoreTime ;
    private final PrintWriter                writer;
    private final PrintWriter                snapshotChainsWriter;

    //Metrics
    private double                           totalDataBackedUp     = 0;
    private double                           dailyDataBackedUp     = 0;
    private long                             totalUnutilizedTime = 0;
    private double                           totalBackupTime       = 0;
    private int                              missedWindows       = 0;
    private double                           totalSystemThroughput = 0;

    private int                              numRandomStorageConstraints;

    private long                             time                = 0;

    public BackupSystem(final PrintWriter writer, final PrintWriter snapshotChainsWriter, final String systemConfigFile, String systemConstraintFile, final Scheduler scheduler,
            final int windowsizeMultiplier, final long overallWindowSize) throws IOException {
        backups = new HashMap<String, Backup>();
        servers = new HashMap<String, MediaServer>();
        clients = new HashMap<String, Client>();
        storageDevices = new HashMap<String, StorageDevice>();
        
        NaturalMap =new HashMap<String,NaturalReflexHistorical>();
        estimatedRestoreTime = new HashMap<String,Integer>();
        fullBackupFrequencyMap = new HashMap<String,Integer>();
        wholeBackupsMap = new HashMap<String,Backup>();
        unBackupsMap = new HashMap<String,Backup>();
        backupToSnapshotMap = new HashMap<String,SnapshotChain>();
        completedBackupsMap = new HashMap<String, Backup>();
        constraints = new Constraints(windowsizeMultiplier, overallWindowSize);
        parseInputFiles(systemConfigFile, systemConstraintFile);
        constraints.fillWindowAndConstraints(new ArrayList<String>(storageDevices.keySet()), clients, backups);

        //give backups their constraints and make incremental if needed ---> make it false to let it process full backup on the 1st day.
        for (final Map.Entry<String, Backup> backupEntry : backups.entrySet()) {
            backupEntry.getValue().setConstraint(constraints.getConstraint(backupEntry.getKey()));
            if (false) {
                backupEntry.getValue().makeIncrementalBackup();
            }
        }

        unutilizedStorageMap = new HashMap<String, Long>();

        numTotalBackups = backups.size();
        //        numRemainingBackups = numTotalBackups;
        this.writer = writer;
        this.snapshotChainsWriter = snapshotChainsWriter;
        this.scheduler = scheduler;
        fillScheduler();

    }

    /**
     * @return the activeBackups
     */
    public int getActiveBackups() {
        return numActiveBackups;
    }

    public  Backup getBackup(final String backupName) {
        return backups.get(backupName);
    }

    public  Map<String, Backup> getBackups() {
        return backups;
    }

    public Client getClient(final String clientName) {
        return clients.get(clientName);
    }

    public Map<String, Client> getClients() {
        return clients;
    }

    /**
     * @return the completedBackups
     */
    public int getCompletedBackups() {
        return numCompletedBackups;
    }

    /**
     * @return the iterationNumber
     */
    public int getIterationNumber() {
        return iterationNumber;
    }

    public MediaServer getMediaServer(final String serverName) {
        return servers.get(serverName);
    }

    public Map<String, MediaServer> getServers() {
        return servers;
    }

    /**
     * @return the totalBackups
     */
    public int getTotalBackups() {
        return numTotalBackups;
    }

    /**
     * @return the writer
     */
    public PrintWriter getWriter() {
        return writer;
    }


    public void nextIteration() {
        //test NR table
    		//System.out.println(NaturalMap.get("backup2").getIncreSize());
      	//System.out.println(NaturalMap.get("backup2").getfullSize());
    		
        if(iterationNumber ==1) {
          	backups = completedBackupsMap;
        		wholeBackupsMap.putAll(backups);
        }else {
        		backups.putAll(wholeBackupsMap);
        }
        
        iterationNumber++;
        backupToSnapshotMap = new HashMap<String,SnapshotChain>();
        
        for (final Map.Entry<String, Backup> entry : wholeBackupsMap.entrySet()) {
        		Backup value = entry.getValue();
        	    NaturalReflex naturalReflex = new NaturalReflex(entry.getValue(), clients.get(entry.getValue().getClientName()),storageDevices.get(entry.getValue().getStorageName()));
        	    //set up backup Type by scheduler
        	    String tempBackupType = scheduler.computeBackupTpye("Differential");
        	    entry.getValue().setBackupType(tempBackupType);
        	    
        	    //set up full backup frequency by NaturalReflex;       	    
        		if (entry.getValue().getBackupType() == "Differential") {
        			if(iterationNumber >=2 && entry.getValue().getDailyBackupType()=="full" && entry.getValue().getProgressDay() == iterationNumber-1) {
        				Random random = new Random();
        				int number = random.nextInt(15)+1;
        				//int tempFullBackupFrequency = scheduler.computeFullBackupFrequency(7);
        				//int tempFullBackupFrequency = naturalReflex.computeFullBackupFrequency();
        				//int tempFullBackupFrequency = naturalReflex.dynamicFullBackupFequency();
        				int tempFullBackupFrequency = naturalReflex.dynamicOptimalFullBackupFequency(0.9);
        				System.out.println(entry.getKey()+" "+ tempFullBackupFrequency);
        				int time = naturalReflex.getEstimiatedRestoreTime();  
        				estimatedRestoreTime.put(entry.getKey(), time);
        				fullBackupFrequencyMap.put(entry.getKey(), tempFullBackupFrequency);
        				entry.getValue().setFullBackupFrequency(tempFullBackupFrequency);
        				
        			}else {
        				entry.getValue().setFullBackupFrequency(fullBackupFrequencyMap.get(entry.getKey()));
        			}
        			
        		}
        		//System.out.println(iterationNumber + " "+ entry.getKey()+" FBF: "+ entry.getValue().getFullBackupFrequency() );
        	    //set up backup frequency
            entry.getValue().setBackupFrequency(naturalReflex.computeFrequency(entry.getValue().getRPO()));
            //System.out.println(entry.getValue().getBackupFrequency());
            if ((iterationNumber - 1) % entry.getValue().getBackupFrequency() !=0 || (iterationNumber - 1 ) < entry.getValue().getBackupFrequency()) {
            		backups.remove(entry.getKey());
            		unBackupsMap.put(entry.getKey(), entry.getValue());	
            }
        }
               
        wholeBackupsMap = new HashMap<String,Backup>();
        wholeBackupsMap.putAll(backups);
        wholeBackupsMap.putAll(unBackupsMap);
 
        //TODO logging?
        writer.println("\r\nIteration " + iterationNumber + "\r\n");
        snapshotChainsWriter.println("\n");
        //reset backups --->Brandom
//        for (final Map.Entry<String, Backup> entry : backups.entrySet()) {
//            entry.getValue().resetBackup(.05, false);  
//            //System.out.println(entry.getKey() + " " + entry.getValue().getBackupFrequency());
//            
//        }
        
        //get the backupTpye and reset the backup based on the full backup frequency
        for (final Map.Entry<String, Backup> entry : backups.entrySet()) {
        		if (entry.getValue().getBackupType() == "Differential") {
        			if((iterationNumber - 1) % (entry.getValue().getFullBackupFrequency()*entry.getValue().getBackupFrequency() )!=0 || (iterationNumber -1) < entry.getValue().getFullBackupFrequency()) {
        				entry.getValue().resetBackup(0, true);
        				entry.getValue().setDailyBackupType("incre");
        				//System.out.println(iterationNumber + " "+ "incre");
        			}else {
        				entry.getValue().resetBackup(0.05, false);
        				//System.out.println(iterationNumber + " "+ "full");
        				entry.getValue().setDailyBackupType("full");
        			}
        		}
              
            //System.out.println(entry.getKey() + " " + entry.getValue().getBackupFrequency());
            
        }
        
        scheduler.incrementDay();
        scheduler.setBackups(backups);

        //reset completedBackups
        completedBackupsMap = new HashMap<String, Backup>();

        numTotalBackups = backups.size();
        //        numRemainingBackups = numTotalBackups;
        numActiveBackups = 0;
        numCompletedBackups = 0;
        dailyDataBackedUp = 0;
        time = 0;
    }

    public void parseInputFiles(String systemConfigFile, String systemConstraintFile) throws IOException {
        parseSystemConfig(systemConfigFile);
        parseSystemConstraints(systemConstraintFile);
    }

    //#inputFileName,#resultsFileName,#numStus,#numClients,#numBackups,#numIterations,#numRandomStorageConstraints,#windowSize,#schedulerName,#totalDataBackedUp,#totalBackupTime,#throughput,#throughputUtilization,#unutilizedTime,#unutilizedPercentage,#missedWindows
    public void printFinalOutput(final String systemConfigFile, final String systemConstraintsFile, final String outputFile, final String dataLogFile, final int iterations) {
        try (PrintWriter logFile = new PrintWriter(new BufferedWriter(new FileWriter(dataLogFile, true)))) {
            logFile.println(systemConfigFile.replaceFirst(".*/([^/?]+).*", "$1") + "," //input file name
                    + outputFile + "," //results file name
                    + storageDevices.size() + "," //num stus
                    + clients.size() + "," //num clients
                    + numTotalBackups + "," //num backups
                    + iterations + "," //number of iterations
                    + numRandomStorageConstraints + "," //num random storage constraints 
                    + constraints.getWindowSize() + "," //window size in milliseconds
                    + scheduler.getName() + "," //scheduler name
                    + totalDataBackedUp + "," //total data backed up in megabytes
                    + totalBackupTime + "," //total backup time in milliseconds
                    + totalDataBackedUp / (totalBackupTime / 1000) + "," //throughput in MB/s
                    + (totalDataBackedUp / (totalBackupTime / 1000)) / totalSystemThroughput + "," //throughput utilization
                    + totalUnutilizedTime + "," //unutilized storage time in milliseconds
                    + totalUnutilizedTime / totalBackupTime * 100 + "," //unutilized time divided by total backup time (result can be above 100%)
                    + missedWindows); //number of missed backups
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }

        writer.println("System Config File: " + systemConfigFile);
        writer.println("Constraints File: " + systemConstraintsFile);
        writer.println("Number of Storage Devices:" + storageDevices.size());
        writer.println("Total theoretical throughput: " + totalSystemThroughput);
        writer.println("Number of Clients:" + clients.size());
        writer.println("Number of Backups:" + numTotalBackups);
        writer.println("Number of Iterations: " + iterations);
        writer.println("Number of Randomized Constraints:" + numRandomStorageConstraints);
        writer.println("Backup Window Size:" + constraints.getWindowSize());
        writer.println("Scheduler:" + scheduler.getName());
        writer.println("Total Data Backed Up:" + totalDataBackedUp);
        writer.println("Total Backup Time:" + totalBackupTime);
        writer.println("Total Throughput:" + totalDataBackedUp / (totalBackupTime / 1000));
        writer.println("Throughput Utilization:" + (totalDataBackedUp / (totalBackupTime / 1000)) / totalSystemThroughput);
        writer.println("Total Unutilization Time:" + totalUnutilizedTime);
        //        writer.println("Percent Unutilized Time:" + totalUnutilizedTime / totalBackupTime * 100);
        writer.println("Missed Backup Windows: " + missedWindows);

    }

    /**
     * @param iterationNumber
     *            the iterationNumber to set
     */
    public void setIterationNumber(int iterationNumber) {
        this.iterationNumber = iterationNumber;
    }

    public void step(int timeStep) { //time = milliseconds
        //Do we need to start backups?
        //start backup		

        //Only start backups every second (1000 milliseconds).
        if (time % 1000 == 0) {

            //scheduler
            final Map<String, String> backupsToStart = scheduler.getNewBackups(time); //Map<Backup, StorageDevice>


            //start backups
            for (final Map.Entry<String, String> backupAssignment : backupsToStart.entrySet()) {
                final String backupName = backupAssignment.getKey();
                final String storageName = backupAssignment.getValue();

                //set active and start time
                backups.get(backupName).setActive(true);
                backups.get(backupName).setStartTime(time);

                //assign it to the storage device
                storageDevices.get(storageName).addActiveBackup(backups.get(backupName));

                //assign the storage device to it
                backups.get(backupName).setStorageName(storageName);

                //assign server of storage device
                //give it a server
                final String serverName = storageDevices.get(storageName).getServerName();
                backups.get(backupName).setServerName(serverName);
                servers.get(serverName).addBackup(backupName);

                //                numRemainingBackups--;
                numActiveBackups++;
                if (iterationNumber == 1) { //TODO hard code to assign the backup type to the backup
                		backups.get(backupName).setBackupType(scheduler.computeBackupTpye("Differential"));
                		backups.get(backupName).setDailyBackupType("full");
                	
                }
                printLog(writer, new LogBuilder(Events.BACKUP_START, time)
                .backupType(backups.get(backupName).getBackupType())
                .dailyBackupType(backups.get(backupName).getDailyBackupType())
                .backup(backupName)
                .storage(storageName)
                .dataSize(backups.get(backupName).getDataSize())
                .build());
            }
        }

        //calculate the throughput for each backup
        final HashMap<String, Double> throughputMap = new HashMap<String, Double>(); //<String (backupName), Double (backupThroughput)>
        for (final Map.Entry<String, StorageDevice> storageEntry : storageDevices.entrySet()) {

            if (storageEntry.getValue().getActiveBackups().size() == 0) {
                /**
                 * Unutilized Storage
                 */
                if (unutilizedStorageMap.containsKey(storageEntry.getKey())) { //If it is not already in the unutilizedMap
                    unutilizedStorageMap.put(storageEntry.getKey(), Long.valueOf((unutilizedStorageMap.get(storageEntry.getKey()).longValue() + timeStep)));
                } else {
                    unutilizedStorageMap.put(storageEntry.getKey(), Long.valueOf(timeStep));
                }
                totalUnutilizedTime += timeStep;
            } else {
                /**
                 * Storage is utilized, do backup transfer.
                 */
                final ArrayList<String> backupList = Helper.cloneArrayList(storageEntry.getValue().getActiveBackups());
                final ArrayList<Double> throughputs = new ArrayList<Double>();
                for (int i = 0; i < backupList.size(); i++) {
                    //get the client associated with the backup associated with the name at index i in backupList
                    final double throughput = clients.get(backups.get(backupList.get(i)).getClientName()).computeThroughput();
                    //add throughput to ArrayList, note order is important and used
                    throughputs.add(Double.valueOf(throughput));
                }
                //For each storage device, allocate the availableThroughput to the backups
                //smallest value gets first choice, with max available being 1/N the throughput
                //after each backup takes its bandwidth, available and max to be taken is recalculated
                //therefore if there is leftovers after even distribution it gets distributed fairly
                double availableThroughput = storageEntry.getValue().computeThroughput();
                while (!throughputs.isEmpty()) {
                    final double maxAllowed = availableThroughput / backupList.size();
                    final int index = Helper.findMinIndex(throughputs);
                    if (throughputs.get(index) >= maxAllowed) {
                        //allocate max allowed
                        throughputMap.put(backupList.get(index), maxAllowed);
                        availableThroughput = availableThroughput - maxAllowed;
                    } else {
                        //allocate requested
                        throughputMap.put(backupList.get(index), throughputs.get(index));
                        availableThroughput = availableThroughput - throughputs.get(index).doubleValue();
                    }
                    //remove backup from contention
                    backupList.remove(index);
                    throughputs.remove(index);
                }
            }
        }

        //time step progress backups
        for (final Iterator<Map.Entry<String, Backup>> iter = backups.entrySet().iterator(); iter.hasNext();) {
            final Map.Entry<String, Backup> backupEntry = iter.next();
            if (backupEntry.getValue().isActive()) {
                //It is active
                throughputMap.get(backupEntry.getKey());
                final boolean completed = backupEntry.getValue().step(timeStep, throughputMap.get(backupEntry.getKey()));
                if (completed) {
                    //if that step completes the backup

                    backupEntry.getValue().setActive(false);
                    backupEntry.getValue().setEndTime(time + timeStep);
                    backupEntry.getValue().setCompleted(true);
                    backupEntry.getValue().setProgressDay(iterationNumber);
                    numActiveBackups--;
                    numCompletedBackups++;
                    totalDataBackedUp += (long) backupEntry.getValue().getDataSize();
                    dailyDataBackedUp += backupEntry.getValue().getDataSize();

                    //give scheduler historical data
                    scheduler.notateHistoricalData(backupEntry.getValue());

                    //give natural reflex data 
                    //notateNaturelReflexTableData(backupEntry.getValue());
                    
                    if (backupEntry.getValue().missedBackupWindow(time + timeStep)) {
                        ++missedWindows;
                    }
                    
                    //put the completed backup on the snapshot chains for restore
                    SnapshotChain snapshotChain = new SnapshotChain();
                    snapshotChain.setBackupName(backupEntry.getKey());
                    snapshotChain.setBackupType(backupEntry.getValue().getBackupType());
                    snapshotChain.setDailyBackupType(backupEntry.getValue().getDailyBackupType());
                    snapshotChain.setIterationNumber(iterationNumber);
                    snapshotChain.setStorageName(backupEntry.getValue().getStorageName());
                    snapshotChain.setDataSize(backupEntry.getValue().getDataSize());
                    snapshotChain.setClientName(backupEntry.getValue().getClientName());
                    snapshotChain.setServerName(backupEntry.getValue().getServerName());
                    snapshotChain.setRPO(backupEntry.getValue().getRPO());
                    snapshotChain.setRTO(backupEntry.getValue().getRTO());
                    backupToSnapshotMap.put(backupEntry.getKey(), snapshotChain);
                    snapshotChainMap.put(String.valueOf(iterationNumber), backupToSnapshotMap);
                    
                    //record the completed backup historical data on the naturalreflex historical 
                    if(null == NaturalMap.get(backupEntry.getKey())) {
                    	    NaturalReflexHistorical historicalvalues = new NaturalReflexHistorical(backupEntry.getValue());
                        historicalvalues.notateNaturalReflexHistorical();                 
                        NaturalMap.put(backupEntry.getKey(), historicalvalues);
                    } else {
                     	NaturalMap.get(backupEntry.getKey()).updateNRHistorical(backupEntry.getValue());
                    	    NaturalMap.get(backupEntry.getKey()).notateNaturalReflexHistorical();
                    }
                    
                    
                    
                    
                    printLog(snapshotChainsWriter, new LogBuilder(Events.SNAPSHOTCHAINS, time + timeStep)
                    .backup(snapshotChain.getBackupName())
                    .iterationNumber(snapshotChain.getIterationNumber())	
                    .backupType(snapshotChain.getBackupType())
                    .dailyBackupType(snapshotChain.getDailyBackupType())
                    .dataSize(snapshotChain.getDataSize())
                    .storage(snapshotChain.getStorageName())
                    .build());
                    

                    printLog(writer, new LogBuilder(Events.BACKUP_COMPLETED, time + timeStep)
                    .backupType(backupEntry.getValue().getBackupType())
                    .dailyBackupType(backupEntry.getValue().getDailyBackupType())
                    .backup(backupEntry.getKey())
                    .storage(backupEntry.getValue().getStorageName())
                    .duration(backupEntry.getValue().getDuration())
                    .throughput(backupEntry.getValue().getOverallThroughput())
                    .build());

                    //remove from storage device
                    storageDevices.get(backupEntry.getValue().getStorageName()).removeActiveBackup(backupEntry.getKey());

                    //remove from server
                    servers.get(backupEntry.getValue().getServerName()).removeBackup(backupEntry.getKey());

                    //TODO remove if I change my mind
                    completedBackupsMap.put(backupEntry.getKey(), backupEntry.getValue());

                    iter.remove();
                }
            }
        }

        //update smallest remaining and total remaining backups for storage devices
        for (final StorageDevice stu : storageDevices.values()) {
            stu.updateSmallestBackup(backups);
            stu.updateTotalActiveBackup(backups);
        }

        //increment time
        time += timeStep;
        totalBackupTime += timeStep;
    }

    public void writeCompletionStatistics() {
        printLog(writer, new LogBuilder(Events.ALL_BACKUPS_COMPLETED, time).dataSize(dailyDataBackedUp).build());

    }

    private void fillScheduler() {
        scheduler.setBackups(backups);
        scheduler.setClients(clients);
        scheduler.setServers(servers);
        scheduler.setStorageDevices(storageDevices);
        scheduler.setConstraints(constraints);
    }

    private void parseBackup(final String line) {
        String tempLine = line.substring(line.indexOf('(') + 1, line.indexOf(')')); //get data without parenthesis
        tempLine = tempLine.replaceAll("\\s+", ""); //remove all white space leaving comma separated values
        final String[] splitLine = tempLine.split(",");

        final Backup backup = new Backup(splitLine[0], Double.parseDouble(splitLine[2]), splitLine[4], Integer.parseInt(splitLine[5]));
        final Client associatedClient = clients.get(splitLine[1]);
        if (associatedClient == null) {
            throw new RuntimeException("Client not defined for Backup: " + splitLine[0]);
        }

        //parse constraints
        constraints.addConstraint(backup.getName(), new Constraint(backup.getName()));

        //assign client and vice versa
        backup.setClientName(associatedClient.getName());
        clients.get(splitLine[1]).addBackup(backup.getName());

        //add to the map
        backups.put(backup.getName(), backup);
        
    }
    
    private void parseClient(final String line) {
        String tempLine = line.substring(line.indexOf('(') + 1, line.indexOf(')')); //get data without parenthesis
        tempLine = tempLine.replaceAll("\\s+", ""); //remove all white space leaving comma separated values
        final String[] splitLine = tempLine.split(",");

        final Client client = new Client(splitLine[0], Double.parseDouble(splitLine[1]), Double.parseDouble(splitLine[2]));
        clients.put(client.getName(), client);
    }

    private void parseConfigLine(String line, int lineCount) {
        final String type = line.substring(0, line.indexOf('('));

        switch (type) {
        case "media_server":
            parseMediaServer(line);
            break;
        case "stu":
            parseStorageDevice(line);
            break;
        case "client":
            parseClient(line);
            break;
        case "backup":
            parseBackup(line);
            break;
        default:
            throw new RuntimeException("Invalid input type line: " + lineCount + " type: " + type);
        }
    }

    private void parseConstraintLine(String line, int lineCount) {
        if (line.contains("RandomizedStorageConstraints=")) {
            numRandomStorageConstraints = Integer.parseInt(line.substring(line.indexOf("=") + 1));
        } else {
            final String backupName = line.substring(0, line.indexOf('('));
            String tempLine = line.substring(line.indexOf('(') + 1, line.indexOf(')')); //get data without parenthesis
            tempLine = tempLine.replaceAll("\\s+", ""); //remove all white space leaving comma separated values
            final String[] splitLine = tempLine.split(",");
            constraints.getConstraint(backupName);
            String storageConstraints = splitLine[0];
            if (storageConstraints.contains("{")) {
                storageConstraints = storageConstraints.substring(storageConstraints.indexOf('{') + 1, storageConstraints.indexOf('}'));
                final String[] splitStorageConstraints = storageConstraints.split("\\|");
                for (int i = 0; i < splitStorageConstraints.length; i++) {
                    constraints.getConstraint(backupName).addStorageConstraint(splitStorageConstraints[i]);
                }
            }
            //TODO parse other constraints
        }

    }

    private void parseMediaServer(final String line) {
        String tempLine = line.substring(line.indexOf('(') + 1, line.indexOf(')')); //get data without parenthesis
        tempLine = tempLine.replaceAll("\\s+", ""); //remove all white space leaving comma separated values
        final String[] splitLine = tempLine.split(",");

        final MediaServer server = new MediaServer(splitLine[0]);
        servers.put(splitLine[0], server);
    }

    private void parseStorageDevice(final String line) {
        String tempLine = line.substring(line.indexOf('(') + 1, line.indexOf(')')); //get data without parenthesis
        tempLine = tempLine.replaceAll("\\s+", ""); //remove all white space leaving comma separated values
        final String[] splitLine = tempLine.split(",");

        final StorageDevice stu = new StorageDevice(splitLine[0], splitLine[1], Double.parseDouble(splitLine[2]),
                Double.parseDouble(splitLine[3]), Double.parseDouble(splitLine[4]), Double.parseDouble(splitLine[5]));

        final MediaServer associatedServer = servers.get(splitLine[1]);
        if (associatedServer == null) {
            throw new RuntimeException("Media Server not defined for stu: " + splitLine[0]);
        }
        associatedServer.addStorageDevice(stu.getName());
        storageDevices.put(stu.getName(), stu);
        totalSystemThroughput += stu.getThroughput();

    }

    private void parseSystemConfig(String systemConfigFile) throws IOException {
        BufferedReader reader = null;
        String line = "";
        int lineCount = 0;
        try {
            reader = new BufferedReader(new FileReader(systemConfigFile));

            while ((line = reader.readLine()) != null) {
                //do the thing
                line = line.trim();
                if (!"".equals(line) && line.charAt(0) != '#') { //ignore this line
                    parseConfigLine(line, lineCount);
                }
                lineCount++; //increment line count for error message
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

    }

    private void parseSystemConstraints(String systemConstraintFile) throws IOException {
        BufferedReader reader = null;
        String line = "";
        int lineCount = 0;
        try {
            reader = new BufferedReader(new FileReader(systemConstraintFile));

            while ((line = reader.readLine()) != null) {
                //do the thing
                line = line.trim();
                if (!"".equals(line) && line.charAt(0) != '#') { //ignore this line
                    parseConstraintLine(line, lineCount);
                }
                lineCount++; //increment line count for error message
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

    }

    private void printLog(final PrintWriter writer, final LogEvent event) {
        writer.println(event.toString());

    }
    
    public static Map<String, Map<String, SnapshotChain>> getSnapshotChainMap(){
    		return snapshotChainMap;
    }
    
    public static Map<String, Integer> getEstimatedReatoreTime(){
    		return estimatedRestoreTime;
    }
    
    public static Map<String, NaturalReflexHistorical> getNaturalMap(){
    		return NaturalMap;
    }
    


}
