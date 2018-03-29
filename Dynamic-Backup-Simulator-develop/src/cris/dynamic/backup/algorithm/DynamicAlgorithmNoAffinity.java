package cris.dynamic.backup.algorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.Restore;
import cris.dynamic.backup.SnapshotChain;
import cris.dynamic.backup.infrastructure.HistoricalDataPoint;
import cris.dynamic.backup.server.StorageDevice;
import cris.dynamic.backup.system.BackupSystem;
import cris.dynamic.backup.system.Helper;
import cris.dynamic.backup.system.RestoreSystem;

public class DynamicAlgorithmNoAffinity extends Scheduler {

    private static int                                                     day;
    private static int                                                     maxBackupPerSTU              = 5;
    private static int                                                     maxRestorePerSTU              = 5;
    private static int                                                     historicalDataExpirationTime = 25;
    private static int                                                     presentedOptions             = 5;
   

    private final Map<String, ArrayList<HistoricalDataPoint>>              backupHistoricalThroughputs;      //<String(backupName), ArrayList<Throughputs>(throughput values for that backup)>
    private Map<String, Restore>                                           unScheduledRestoresMap; 
    private ArrayList<Restore>                                             pieceRestoresList;
    private ArrayList<Restore>                                              unScheduledRestoresList;
    private Map<String, ArrayList<Restore>>                          pieceRestoresListMap; // Restore Name --> the pieceRestoreList for the restore Name    
    private Map<String, ArrayList<Restore>>                                readyPieceRestoresMap;

    public DynamicAlgorithmNoAffinity() {
        day = 0;
        new HashMap<String, Map<String, ArrayList<HistoricalDataPoint>>>();
        backupHistoricalThroughputs = new HashMap<String, ArrayList<HistoricalDataPoint>>();
        unScheduledRestoresMap = new HashMap<String,Restore>();
        pieceRestoresList = new ArrayList<Restore>();
        pieceRestoresListMap = new HashMap<String, ArrayList<Restore>>();
        readyPieceRestoresMap = new HashMap<String, ArrayList<Restore>>();
        unScheduledRestoresList = new ArrayList<Restore>();
    }

    @Override
    public String getName() {
        return "DynamicAlgorithmNoAffinity";
    }

    /**
     * getNewBackups
     */
    @Override
    public Map<String, String> getNewBackups(final long currentTime) {

        Map<String, Backup> backupsCopy = new HashMap<String, Backup>();
        backupsCopy.putAll(super.getBackups());

        /**
         * Step 1. Update Backup Priority
         */
        backupsCopy = removeActiveAndCompleted(backupsCopy);
        final ArrayList<Backup> backupList = new ArrayList<Backup>(backupsCopy.values());

        /**
         * Backup Priority Determination
         */
        Collections.sort(backupList, new Comparator<Backup>() { //sort by priority

            @Override
            public int compare(final Backup backup0, final Backup backup1) {
                final double priority0 = calculateBackupPriority(backup0);
                final double priority1 = calculateBackupPriority(backup1);
                return Double.compare(priority0, priority1);
            }

            private double calculateBackupPriority(final Backup backup) {
                if (backupHistoricalThroughputs.get(backup.getName()).isEmpty()) {
                    //No historical data, priority set to 0.
                    return 0;
                }

                //There is historical data, use that to determine priority
                final double historicalThroughput = calculateExpectedThroughput(backupHistoricalThroughputs.get(backup.getName()));

                if (backup.getConstraint().getEndConstraint() == -1) { //no end constraints determined LongestBackupFirst mode
                    return 1 / (backup.getDataSize() / historicalThroughput); //longest backup first

                } else { //We have an end constraint and we use it to determine who is added first
                    return ((currentTime - backup.getConstraint().getEndConstraint()) - (backup.getDataSize() / historicalThroughput))
                            / (backup.getDataSize() / historicalThroughput);

                }

            }

        });

        /**
         * Step 2. Update Storage Priority
         */
        Map<String, StorageDevice> storageCopy = new HashMap<String, StorageDevice>();
        storageCopy.putAll(super.getStorageDevices());
        storageCopy = removeIneligibleStorage(storageCopy, maxBackupPerSTU);
        final ArrayList<StorageDevice> stuList = new ArrayList<StorageDevice>();
        stuList.addAll(storageCopy.values());

        final Map<String, String> backupAssignments = new HashMap<String, String>();

        //Sort storage devices:
        Collections.sort(stuList, new Comparator<StorageDevice>() {

            @Override
            public int compare(final StorageDevice stu0, final StorageDevice stu1) {
                final double priority0 = calculateStoragePriority(stu0);
                final double priority1 = calculateStoragePriority(stu1);
                return Double.compare(priority0, priority1);
            }

            private double calculateStoragePriority(final StorageDevice stu) {
                double priorityToReturn = 0;
                final ArrayList<String> activeBackups = stu.getActiveBackups();
                for (int i = 0; i < activeBackups.size(); i++) {
                    final Backup backup = DynamicAlgorithmNoAffinity.super.getBackups().get(activeBackups.get(i));
                    if (!backupHistoricalThroughputs.get(backup.getName()).isEmpty()) {
                        //If there is historical data estimate how much time is left.
                        priorityToReturn += backup.getDataLeft() / calculateExpectedThroughput(backupHistoricalThroughputs.get(backup.getName()));
                    } else {
                        //If there is no historical data use data size to estimate time. This should only occur the first day.
                        priorityToReturn += backup.getDataLeft();
                    }
                }

                return priorityToReturn;
            }

        });

        for (int i = 0; i < stuList.size(); i++) {
            /**
             * Now we have a sorted list of storage devices.
             */
            //Take the top storage device.
            final StorageDevice prioritySTU = stuList.get(i);

            //Take the top valid backup.
            Backup chosenBackup = null;

            for (int j = 0; j < backupList.size(); j++) {
                if (backupList.get(j).getConstraint().isStorageValid(prioritySTU.getName())) {
                    chosenBackup = backupList.get(j);
                    break;
                }
            }

            if (chosenBackup != null) {
                backupAssignments.put(chosenBackup.getName(), prioritySTU.getName()); //add assignment to return
                return backupAssignments;
            }

        }



        return backupAssignments; //empty case
    }
    
    //restore 
    public Map<String, Restore> getNewRestores(long currentTime) {
    	    //backup information 
        Map<String, Backup> backups = new HashMap<String, Backup>();
        backups.putAll(super.getBackups());
        
    	    //find the storage device available (<5 active restores)
		Map<String, StorageDevice> storageCopy = new HashMap<String, StorageDevice>();
		storageCopy.putAll(super.getStorageDevices());
		storageCopy = removeActiveRestoreStorage(storageCopy, maxRestorePerSTU);
		
		//read all the restore request of the current day
	    Map<String, Restore> restoresCopy = new HashMap<String, Restore>();
	    Map<String, Restore> restoresAssignment = new HashMap<String,Restore>();
	    restoresCopy = RestoreSystem.getDayToRestores().get(String.valueOf(day+1)); 
    	
    	    // check if there is scheduled but partial uncompleted restore
    	    Map<String, Restore> returnRestore = new HashMap<String,Restore>();
//    	    if(!readyPieceRestoresMap.isEmpty()) {
//    	    		for(Map.Entry<String,ArrayList<Restore>> readyEntry : readyPieceRestoresMap.entrySet()) {
//    	    			String readyRestoreName = readyEntry.getKey();
//    	    			ArrayList<Restore> redayRestoreList = readyEntry.getValue();
//    	    			int returnRestoreIndex =0;
//    	    			for (int i = 0 ; i <= redayRestoreList.size(); i++) {
//    	    				if (storageCopy.containsKey(redayRestoreList.get(i).getStorageName())) {
//    	    					returnRestoreIndex = i ; 
//    	    					continue;
//    	    				}
//    	    			}
//    	    			returnRestore.put(readyRestoreName, redayRestoreList.get(returnRestoreIndex));
//    	    			pieceRestoresListMap.get(readyRestoreName).remove(returnRestoreIndex);
//    	    			readyPieceRestoresMap = new HashMap<String, ArrayList<Restore>>();
//    	    			if(redayRestoreList.isEmpty()) {
//    	    				pieceRestoresListMap.remove(readyRestoreName);
//    	    			}
//    	    			
//    	    			return returnRestore;
//    	    		} 			
//    	    }
    	    
    	    if(!readyPieceRestoresMap.isEmpty()) {
	    		for(Map.Entry<String,ArrayList<Restore>> readyEntry : readyPieceRestoresMap.entrySet()) {
	    			String readyRestoreName = readyEntry.getKey();
	    			ArrayList<Restore> redayRestoreList = readyEntry.getValue();
	    			returnRestore.put(readyRestoreName, redayRestoreList.get(0));
	    			pieceRestoresListMap.get(readyRestoreName).remove(0);
	    			readyPieceRestoresMap = new HashMap<String, ArrayList<Restore>>();
	    			if(redayRestoreList.isEmpty()) {
	    				pieceRestoresListMap.remove(readyRestoreName);
	    			}
	    			
	    			return returnRestore;
	    		} 			
	    }
    		
    	
        // check if there is requested but unscheduled restore
    		Map<String, Restore> selectedRestore = new HashMap<String,Restore>();
    		String selectedRestoreName = "";
    		Restore selectedRestoreValue = new Restore();
    		
    		boolean goToRequestTable = true;
    		if(!unScheduledRestoresList.isEmpty()) {
    			
    			for (int i = 0 ; i < unScheduledRestoresList.size() ; i++) {   				
    				Restore restore = unScheduledRestoresList.get(i);
    				if (storageCopy.containsKey(restore.getStorageName())) {
    					selectedRestoreName = restore.getRestoreName();			
    	        	        selectedRestoreValue = restore;
    	        	        selectedRestore.put(selectedRestoreName,selectedRestoreValue);
    	        	        unScheduledRestoresList.remove(0);
    	        	        goToRequestTable = false;
    	        	        break;
    				}
    			}
    		    
    		}
    		
    		if (goToRequestTable){//if there is no unscheduled requested restore
        	    //find the restore request matched with the current time
        	    for (final Map.Entry<String, Restore> restoreCopyEntry : restoresCopy.entrySet()) {
        	    	    long associatedRequestTime = (long) Helper.converToTimeSeconds(restoreCopyEntry.getValue().getRequestTime());
        	    		if ( associatedRequestTime == currentTime/1000) {
        	    			restoresAssignment.put(restoreCopyEntry.getKey(),restoreCopyEntry.getValue());
        	    		}
        	    }
        	    
        	    if (restoresAssignment.isEmpty()) {
        	    	    return returnRestore;       	    	
        	    }
        	    
        	    //remove the active and completed restore 
        	    restoresAssignment = removeActiveAndCompletedRestore(restoresAssignment);
        	    
        	    //randomly select one of the restore request 
        	    unScheduledRestoresMap.putAll(restoresAssignment);  			
    		    selectedRestoreName = getRandomRestore(unScheduledRestoresMap);
        	    selectedRestoreValue = restoresAssignment.get(selectedRestoreName);
        	    selectedRestore.put(selectedRestoreName,selectedRestoreValue);
        	    unScheduledRestoresMap.remove(selectedRestoreName);
    			
    		}
    		
		//read the snapshot chains map
		Map<String, Map<String, SnapshotChain>>snapshotChainMap = BackupSystem.getSnapshotChainMap();
		String restoreNameID = selectedRestoreName.substring(7, selectedRestoreName.length());
		String associatedBackupName = "backup" + restoreNameID;
		
		
		
		//to find the restore associated snapshotChainMap from last full backup day1, day2, ----> current day
		pieceRestoresList =new ArrayList<Restore>();		
		for ( int i =1  ; i <= (day +1) ; i++) {
			String temDay = String.valueOf(i);
			if(null != snapshotChainMap.get(temDay)) {
				
				for (final Map.Entry<String, SnapshotChain> entry : snapshotChainMap.get(temDay).entrySet()) {
					if (entry.getValue().getBackupName().equals(associatedBackupName) ) {
						Restore pieceRestore = new Restore(selectedRestoreName,entry.getValue().getDataSize(),entry.getValue().getStorageName());
						pieceRestore.setDataBeBackupDay(i);
						pieceRestore.setClientName(entry.getValue().getClientName());
						pieceRestore.setServerName(entry.getValue().getServerName());
						pieceRestore.setRPO(entry.getValue().getRPO());
						pieceRestore.setRTO(entry.getValue().getRTO());
						if(entry.getValue().getDailyBackupType() == "full" && i > 1) {
							pieceRestoresList = new ArrayList<Restore>();							
						}
						pieceRestoresList.add(pieceRestore);			


					}						
				}		
				
			}
				
		}
		
		if (storageCopy.containsKey(pieceRestoresList.get(0).getStorageName())) {
			returnRestore.put(selectedRestoreName, pieceRestoresList.get(0)) ;
			pieceRestoresList.remove(0);
			if (!pieceRestoresList.isEmpty()) {
				pieceRestoresListMap.put(selectedRestoreName,pieceRestoresList);	
			}
		}else {
			unScheduledRestoresList.add(pieceRestoresList.get(0));
			
		}

			
    		return returnRestore;
    	
    }
    

    @Override
    public void incrementDay() {
        unScheduledRestoresMap = new HashMap<String,Restore>();
        pieceRestoresList = new ArrayList<Restore>();
        pieceRestoresListMap = new HashMap<String, ArrayList<Restore>>();
        readyPieceRestoresMap = new HashMap<String, ArrayList<Restore>>();
        unScheduledRestoresList = new ArrayList<Restore>();
        ++day;
    }

    /**
     * 
     */
    @Override
    public void notateHistoricalData(final Backup completedBackup) {
        backupHistoricalThroughputs.get(completedBackup.getName())
        .add(new HistoricalDataPoint(day, day + historicalDataExpirationTime, completedBackup.getOverallThroughput(), completedBackup.getDataSize()));

    }

    /**
     * 
     */
    @Override
    public void removeOldHistoricalData(final int day) {
        //expire backup historical data points
        for (final ArrayList<HistoricalDataPoint> list : backupHistoricalThroughputs.values()) {
            for (final Iterator<HistoricalDataPoint> iter = list.iterator(); iter.hasNext();) {
                final HistoricalDataPoint point = iter.next();
                if (point.isExpired(day)) {
                    iter.remove();
                }
            }
        }
    }

    /**
     * @param backups
     *            the backups to set
     */
    @Override
    public void setBackups(Map<String, Backup> backups) {
        super.setBackups(backups);
        if (day == 0) {
            for (final String backupName : backups.keySet()) {
                backupHistoricalThroughputs.put(backupName, new ArrayList<HistoricalDataPoint>());
            }
        }
    }

    /**
     * Calculates the average value of a list of doubles.
     * 
     * @param dataPoints
     * @return average
     */
    private double calculateExpectedThroughput(List<HistoricalDataPoint> dataPoints) {
        double sum = 0;
        double weightSum = 0;
        if (dataPoints != null && !dataPoints.isEmpty()) {
            for (final HistoricalDataPoint dataPoint : dataPoints) {
                sum += dataPoint.getWeightedThroughput(day);
                weightSum += dataPoint.getWeight(day);
            }
            return sum / weightSum;
        }
        return 1; //This is an important 1. It makes it so that if no data is present, backups are assumed to be as long as the amount of data.
    }



    /**
     * Returns a map with active and completed backups removed.
     * 
     * @param backupsCopy
     * @return
     */
    private Map<String, Backup> removeActiveAndCompleted(final Map<String, Backup> backupsCopy) {
        final Map<String, Backup> toReturn = new HashMap<String, Backup>();
        for (final Map.Entry<String, Backup> backupEntry : backupsCopy.entrySet()) {
            if (!backupEntry.getValue().isActive() && !backupEntry.getValue().isCompleted()) {
                toReturn.put(backupEntry.getKey(), backupEntry.getValue());
            }
        }
        return toReturn;
    }

    /**
     * Removes storage devices that are capped on backups.
     * 
     * @param storageCopy
     * @param maxPerSTU
     * @return
     */
    private Map<String, StorageDevice> removeIneligibleStorage(final Map<String, StorageDevice> storageCopy, final int maxPerSTU) {
        final Map<String, StorageDevice> toReturn = new HashMap<String, StorageDevice>();
        for (final Map.Entry<String, StorageDevice> stuEntry : storageCopy.entrySet()) {
            if (stuEntry.getValue().getActiveBackups().size() < maxPerSTU) {
                toReturn.put(stuEntry.getKey(), stuEntry.getValue());
            }
        }
        return toReturn;
    }
    
    public Map<String, Restore> removeActiveAndCompletedRestore(final Map<String, Restore> restoresAssignment) {
        final Map<String, Restore> toReturn = new HashMap<String, Restore>();
        for (final Map.Entry<String, Restore> restoreEntry : restoresAssignment.entrySet()) {
            if (!restoreEntry.getValue().isActive() && !restoreEntry.getValue().isCompleted()) {
                toReturn.put(restoreEntry.getKey(), restoreEntry.getValue());
            }
        }
        return toReturn;
    }
    
    private Map<String, StorageDevice> removeActiveRestoreStorage(final Map<String, StorageDevice> storageCopy, final int maxPerSTU) {
    	final Map<String, StorageDevice> toReturn = new HashMap<String, StorageDevice>();
    	for (final Map.Entry<String, StorageDevice> stuEntry : storageCopy.entrySet()) {
    		if (stuEntry.getValue().getActiveRestores().size() < maxPerSTU) {
    			toReturn.put(stuEntry.getKey(), stuEntry.getValue());
    		}
    	}
    	return toReturn;
    }
    
    public static String getRandomRestore (Map<String, Restore> restoresAssignment) {  
    Random random = new Random();    
    String returnRestore = "";
    	int rn = random.nextInt(restoresAssignment.size());  
        int i = 0;  
        for (final Map.Entry<String, Restore> entry : restoresAssignment.entrySet()) {  
            if(i==rn){ 
               returnRestore = entry.getKey() ;  
            }  
            i++;  
        } 
        return returnRestore;
    }  
    
    public void updatePieceRestoresMap(Restore restore) {
		readyPieceRestoresMap.putAll(pieceRestoresListMap);
		for(Map.Entry<String, ArrayList<Restore>> restoreList: pieceRestoresListMap.entrySet()) {
			if(restoreList.getKey() != restore.getRestoreName()) {
				readyPieceRestoresMap.remove(restoreList.getKey());
			}
		}   		
    }

}
