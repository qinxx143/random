package cris.dynamic.backup.algorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.TriangularDistribution;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.infrastructure.HistoricalDataPoint;
import cris.dynamic.backup.server.StorageDevice;

public class DynamicAlgorithmV2 extends Scheduler {

    private static int                                                     day;
    private static int                                                     maxBackupPerSTU              = 5;
    private static int                                                     historicalDataExpirationTime = 25;
    private static int                                                     presentedOptions             = 5;

    private final Map<String, Map<String, ArrayList<HistoricalDataPoint>>> storageAssociationThroughputs;    //Map<BackupName, Map<StorageName, ArrayList<Throughputs>
    private final Map<String, ArrayList<HistoricalDataPoint>>              backupHistoricalThroughputs;      //<String(backupName), ArrayList<Throughputs>(throughput values for that backup)>
    private Map<String, ArrayList<Backup>>                                 addedBackupList;                  //<String(StorageDeviceName), ArrayList<Backup>(added backups during this iteration)

    public DynamicAlgorithmV2() {
        day = 0;
        storageAssociationThroughputs = new HashMap<String, Map<String, ArrayList<HistoricalDataPoint>>>();
        backupHistoricalThroughputs = new HashMap<String, ArrayList<HistoricalDataPoint>>();
    }

    @Override
    public String getName() {
        return "DynamicAlgorithmV2";
    }

    /**
     * getNewBackups
     */
    @Override
    public Map<String, String> getNewBackups(final long currentTime) {
        addedBackupList = new HashMap<String, ArrayList<Backup>>();
        for (final StorageDevice stu : super.getStorageDevices().values()) {
            addedBackupList.put(stu.getName(), new ArrayList<Backup>());
        }

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
                    return backup.getDataSize() / historicalThroughput; //longest backup first

                } else { //We have an end constraint and we use it to determine who is added first
                    return ((currentTime - backup.getConstraint().getEndConstraint()) - (backup.getDataSize() / historicalThroughput))
                            / (backup.getDataSize() / historicalThroughput);

                }

                //				double historicalThroughput = calculateAverage(backupHistoricalThroughputs.get(backup.getName()));
                //				if(backup.isActive() || backup.isCompleted() || historicalThroughput == -1){
                //					return 0;
                //				} else {
                //					return ( (currentTime - backup.getConstraint().getEndConstraint()) -  ( backup.getDataSize() / historicalThroughput) ) 
                //							/  ( backup.getDataSize() / historicalThroughput);
                //				}
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

        //Loop for deciding which backups start.
        boolean added = true;
        while (added) { //If no backup was added there are no more valid options so stop the loop.
            added = false;
            if (!backupList.isEmpty() && !stuList.isEmpty()) {
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
                            final Backup backup = DynamicAlgorithmV2.super.getBackups().get(activeBackups.get(i));
                            if (!backupHistoricalThroughputs.get(backup.getName()).isEmpty()) {
                                //If there is historical data estimate how much time is left.
                                priorityToReturn += backup.getDataLeft() / calculateExpectedThroughput(backupHistoricalThroughputs.get(backup.getName()));
                            } //else do nothing
                        }

                        //Also for backups added during this iteration of selection
                        final ArrayList<Backup> addedBackups = addedBackupList.get(stu.getName());
                        for (int i = 0; i < addedBackups.size(); i++) {
                            final Backup backup = DynamicAlgorithmV2.super.getBackups().get(addedBackups.get(i).getName());
                            if (!backupHistoricalThroughputs.get(backup.getName()).isEmpty()) {
                                //If there is historical data estimate how much time is left.
                                priorityToReturn += backup.getDataLeft() / calculateExpectedThroughput(backupHistoricalThroughputs.get(backup.getName()));
                            } //else do nothing
                        }

                        return priorityToReturn;
                    }

                });
                /**
                 * Now we have a sorted list of storage devices.
                 */
                //Take the top storage device.
                final StorageDevice prioritySTU = stuList.get(0);

                //Create a list of the top 5 valid backups.
                final ArrayList<Integer> backupOptions = new ArrayList<Integer>(); //list of the indexes of the backups.
                int optionsAdded = 0;
                int index = 0;
                while (optionsAdded < presentedOptions && index < backupList.size()) {
                    if (backupList.get(index).getConstraint().isStorageValid(prioritySTU.getName())) {
                        backupOptions.add(index);
                        ++optionsAdded;
                    }
                    ++index;
                }

                //Sort the backupOptions by affinity
                Collections.sort(backupOptions, new Comparator<Integer>() {

                    @Override
                    public int compare(Integer index0, Integer index1) {

                        //get the affinity value
                        final int value0 = getStorageRanking(backupList.get(index0).getName(), prioritySTU.getName());
                        final int value1 = getStorageRanking(backupList.get(index1).getName(), prioritySTU.getName());

                        return Integer.compare(value0, value1);
                    }

                });

                //We now have a list of 5 (or less) options
                //TODO change to be a probability function that is day dependent
                if (optionsAdded != 0) {
                    final TriangularDistribution selectionDistribution = new TriangularDistribution(0, 0, optionsAdded);
                    final int randomChoice = (int) Math.floor(selectionDistribution.sample());
                    final Backup choosenBackup = backupList.get(backupOptions.get(randomChoice));
                    backupAssignments.put(choosenBackup.getName(), prioritySTU.getName()); //add assignment to return
                    addedBackupList.get(prioritySTU.getName()).add(choosenBackup); //add to added backup list for multiple backups on the same iteration
                    final int toRemove = backupOptions.get(randomChoice);
                    backupList.remove(toRemove); //remove the assigned backup.
                    if (prioritySTU.getActiveBackups().size() + addedBackupList.get(prioritySTU.getName()).size() >= maxBackupPerSTU) {
                        //remove storage from list
                        stuList.remove(0);
                    }
                    added = true;
                } else {
                    //Remove that storage device from list because it no longer has valid options.
                    stuList.remove(0);
                }

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
        final ArrayList<HistoricalDataPoint> historicalValues = storageAssociationThroughputs.get(completedBackup.getName()).get(completedBackup.getStorageName());
        if (historicalValues == null) {
            storageAssociationThroughputs.get(completedBackup.getName()).put(completedBackup.getStorageName(), new ArrayList<HistoricalDataPoint>());
        }

        storageAssociationThroughputs.get(completedBackup.getName()).get(completedBackup.getStorageName())
        .add(new HistoricalDataPoint(day, day + historicalDataExpirationTime, completedBackup.getOverallThroughput(), completedBackup.getDataSize()));

        backupHistoricalThroughputs.get(completedBackup.getName())
        .add(new HistoricalDataPoint(day, day + historicalDataExpirationTime, completedBackup.getOverallThroughput(), completedBackup.getDataSize()));

    }

    /**
     * 
     */
    @Override
    public void removeOldHistoricalData(final int day) {
        //expire storage affinity data points
        for (final Map<String, ArrayList<HistoricalDataPoint>> map : storageAssociationThroughputs.values()) {
            for (final ArrayList<HistoricalDataPoint> list : map.values()) {
                for (final Iterator<HistoricalDataPoint> iter = list.iterator(); iter.hasNext();) {
                    final HistoricalDataPoint point = iter.next();
                    if (point.isExpired(day)) {
                        iter.remove();
                    }
                }
            }
        }

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
                storageAssociationThroughputs.put(backupName, new HashMap<String, ArrayList<HistoricalDataPoint>>());
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

    private int getStorageRanking(final String backupName, final String storageName) {
        int higher = 0;
        final double expectedThroughput = calculateExpectedThroughput(storageAssociationThroughputs.get(backupName).get(storageName));
        for (final Map.Entry<String, ArrayList<HistoricalDataPoint>> mapEntry : storageAssociationThroughputs.get(backupName).entrySet()) {
            if (!mapEntry.getKey().equals(storageName)) {
                if (calculateExpectedThroughput(mapEntry.getValue()) > expectedThroughput) {
                    higher++; //count how many storage units are above it to get the ranking.
                }
            }
        }
        return higher;
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

}
