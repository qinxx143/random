package cris.dynamic.backup.algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.Restore;
import cris.dynamic.backup.SnapshotChain;
import cris.dynamic.backup.server.StorageDevice;
import cris.dynamic.backup.system.BackupSystem;
import cris.dynamic.backup.system.Helper;
import cris.dynamic.backup.system.RestoreSystem;

public class RandomWithMaxV2 extends Scheduler {

    private int activeBackups;
    private int backupsRemaining;
    private static int                                                     maxRestorePerSTU              = 5;
    private static int                                                     day;
    private int maxActive;
    
    private final Map<String, Restore>                                     unScheduledRestoresMap; 
    private ArrayList<Restore>                                             pieceRestoresList;
    private ArrayList<Restore>                                              unScheduledRestoresList;
    private final Map<String, ArrayList<Restore>>                          pieceRestoresListMap; // Restore Name --> the pieceRestoreList for the restore Name    
    private Map<String, ArrayList<Restore>>                                readyPieceRestoresMap;

    public RandomWithMaxV2(int maxActive) {
    	    day = 0;
        this.maxActive = maxActive;
        unScheduledRestoresMap = new HashMap<String,Restore>();
        pieceRestoresList = new ArrayList<Restore>();
        pieceRestoresListMap = new HashMap<String, ArrayList<Restore>>();
        readyPieceRestoresMap = new HashMap<String, ArrayList<Restore>>();
        unScheduledRestoresList = new ArrayList<Restore>();
    }

    /**
     * @return the maxActive
     */
    public int getMaxActive() {
        return maxActive;
    }

    @Override
    public String getName() {
        return "RandomWithMaxV2(" + maxActive + ")";
    }
    
    public void incrementDay() {
        ++day;
    }

    /**
     * Returns a list of backup and storage device pairs.
     */
    @Override
    public Map<String, String> getNewBackups(final long currentTime) {
        updateActiveAndRemaining();

        Map<String, Backup> backupsCopy = new HashMap<String, Backup>();
        backupsCopy.putAll(super.getBackups());
        backupsCopy = removeActiveAndCompleted(backupsCopy);
        final ArrayList<Backup> backupList = new ArrayList<Backup>(backupsCopy.values());

        final Map<String, StorageDevice> storageCopy = new HashMap<String, StorageDevice>();
        storageCopy.putAll(super.getStorageDevices());
        final ArrayList<StorageDevice> stuList = new ArrayList<StorageDevice>();
        stuList.addAll(storageCopy.values());

        final Map<String, String> backupAssignments = new HashMap<String, String>();
        final Random random = new Random();
        if (backupList.size() != 0 && activeBackups < maxActive) {
            final String selectedBackup = backupList.get(random.nextInt(backupList.size())).getName();

            boolean isValidSTU = false;
            String storageName = "";
            while (!isValidSTU) {
                storageName = stuList.get(random.nextInt(stuList.size())).getName();
                if (super.getConstraints().isValidStorageUnit(selectedBackup, storageName)) {
                    isValidSTU = true;
                }
            }

            backupAssignments.put(selectedBackup, storageName);
        }

        return backupAssignments;
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


    /**
     * @param maxActive
     *            the maxActive to set
     */
    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
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

    private void updateActiveAndRemaining() {
        activeBackups = 0;
        backupsRemaining = 0;
        for (final Map.Entry<String, Backup> backupEntry : super.getBackups().entrySet()) {
            if (backupEntry.getValue().isActive()) {
                activeBackups++;
            } else {
                if (!backupEntry.getValue().isCompleted()) {
                    backupsRemaining++;
                }
            }
        }

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
