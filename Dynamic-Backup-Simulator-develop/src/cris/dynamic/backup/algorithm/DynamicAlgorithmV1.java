package cris.dynamic.backup.algorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.server.StorageDevice;

public class DynamicAlgorithmV1 extends Scheduler {

    private static int                                        day;
    private static int                                        maxBackupPerSTU = 5;

    private final Map<String, Map<String, ArrayList<Double>>> storageAssociationThroughputs;
    private final Map<String, ArrayList<Double>>              backupHistoricalThroughputs;

    public DynamicAlgorithmV1() {
        day = 0;
        storageAssociationThroughputs = new HashMap<String, Map<String, ArrayList<Double>>>();
        backupHistoricalThroughputs = new HashMap<String, ArrayList<Double>>();

    }

    @Override
    public String getName() {
        return "DynamicAlgorithmV1";
    }

    /**
     * Returns a list of backup and storage device pairs.
     */
    @Override
    public Map<String, String> getNewBackups(final long currentTime) {

        Map<String, Backup> backupsCopy = new HashMap<String, Backup>();
        backupsCopy.putAll(super.getBackups());

        //Step 1. Update Backup Priority
        backupsCopy = removeActiveAndCompleted(backupsCopy);
        //		for(Backup backup : backupsCopy.values()){
        //			backup.calculatePriorityValue(currentTime, calculateAverage(backupHistoricalThroughputs.get(backup.getName())));
        //		}
        final ArrayList<Backup> backupList = new ArrayList<Backup>(backupsCopy.values());
        Collections.sort(backupList, new Comparator<Backup>() {

            @Override
            public int compare(final Backup backup0, final Backup backup1) {
                final double priority0 = calculateBackupPriority(backup0);
                final double priority1 = calculateBackupPriority(backup1);
                return Double.compare(priority0, priority1);
            }

            private double calculateBackupPriority(final Backup backup) {
                final double historicalThroughput = calculateAverage(backupHistoricalThroughputs.get(backup.getName()));
                if (backup.isActive() || backup.isCompleted() || historicalThroughput == -1) {
                    return 0;
                } else {
                    return ((currentTime - backup.getConstraint().getEndConstraint()) - (backup.getDataSize() / historicalThroughput))
                            / (backup.getDataSize() / historicalThroughput);
                }
            }

        }); //sort by priority

        //Step 2. Update Storage Priority
        Map<String, StorageDevice> storageCopy = new HashMap<String, StorageDevice>();
        storageCopy.putAll(super.getStorageDevices());
        storageCopy = removeIneligibleStorage(storageCopy, maxBackupPerSTU);
        final ArrayList<StorageDevice> stuList = new ArrayList<StorageDevice>();
        stuList.addAll(storageCopy.values());
        Collections.sort(stuList);

        final Map<String, String> backupAssignments = new HashMap<String, String>();

        int maxCounter = maxBackupPerSTU;
        ArrayList<StorageDevice> tempStorageList = new ArrayList<StorageDevice>();
        while (maxCounter > 0) {
            while (!backupList.isEmpty() && !stuList.isEmpty()) {
                final StorageDevice selectedSTU = stuList.get(0);
                int index = 0;
                boolean started = false;
                while (!started && index < backupList.size()) {
                    if (super.getConstraints().isValidStorageUnit(backupList.get(index).getName(), selectedSTU.getName())) {
                        backupAssignments.put(backupList.get(index).getName(), selectedSTU.getName());
                        backupList.remove(index);
                        started = true;
                    }
                    index++;
                }
                if (!started) {
                    stuList.remove(0);
                } else {
                    if (stuList.get(0).getActiveBackups().size() < maxCounter) {
                        //Move to to temp list.
                        final StorageDevice tempHold = stuList.get(0);

                        tempStorageList.add(tempHold);
                    }
                    stuList.remove(0);
                }

            }
            if (backupList.isEmpty()) {
                break;
            } else {
                stuList.addAll(tempStorageList);
                tempStorageList = new ArrayList<StorageDevice>();
                --maxCounter;
            }
        }

        return backupAssignments;
    }

    @Override
    public void incrementDay() {
        ++day;
    }

    /**
     * 
     */
    @Override
    public void notateHistoricalData(final Backup completedBackup) {
        final ArrayList<Double> historicalValues = storageAssociationThroughputs.get(completedBackup.getName()).get(completedBackup.getStorageName());
        if (historicalValues != null) {
            if (historicalValues.size() >= 5) { //TODO
                storageAssociationThroughputs.get(completedBackup.getName()).get(completedBackup.getStorageName()).remove(0);
            }
        } else {
            storageAssociationThroughputs.get(completedBackup.getName()).put(completedBackup.getStorageName(), new ArrayList<Double>());
        }

        storageAssociationThroughputs.get(completedBackup.getName()).get(completedBackup.getStorageName()).add(completedBackup.getOverallThroughput());

        if (backupHistoricalThroughputs.get(completedBackup.getName()).size() > 15) { //TODO
            backupHistoricalThroughputs.get(completedBackup.getName()).remove(0);
        }
        backupHistoricalThroughputs.get(completedBackup.getName()).add(completedBackup.getOverallThroughput());
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
                storageAssociationThroughputs.put(backupName, new HashMap<String, ArrayList<Double>>());
                backupHistoricalThroughputs.put(backupName, new ArrayList<Double>());
            }
        }
    }

    private double calculateAverage(List<Double> throughputList) {
        Double sum = (double) 0;
        if (!throughputList.isEmpty()) {
            for (final Double throughput : throughputList) {
                sum += throughput;
            }
            return sum / throughputList.size();
        }
        return -1;
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

    private Map<String, StorageDevice> removeIneligibleStorage(final Map<String, StorageDevice> storageCopy, final int maxPerSTU) {
        final Map<String, StorageDevice> toReturn = new HashMap<String, StorageDevice>();
        for (final Map.Entry<String, StorageDevice> stuEntry : storageCopy.entrySet()) {
            if (stuEntry.getValue().getActiveBackups().size() < maxPerSTU) {
                toReturn.put(stuEntry.getKey(), stuEntry.getValue());
            }
        }
        return toReturn;
    }

}
