package cris.dynamic.backup.algorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.infrastructure.HistoricalDataPoint;
import cris.dynamic.backup.server.StorageDevice;

public class DynamicAlgorithmNoAffinity extends Scheduler {

    private static int                                                     day;
    private static int                                                     maxBackupPerSTU              = 5;
    private static int                                                     historicalDataExpirationTime = 25;

    private final Map<String, ArrayList<HistoricalDataPoint>>              backupHistoricalThroughputs;      //<String(backupName), ArrayList<Throughputs>(throughput values for that backup)>

    public DynamicAlgorithmNoAffinity() {
        day = 0;
        backupHistoricalThroughputs = new HashMap<String, ArrayList<HistoricalDataPoint>>();
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

    @Override
    public void incrementDay() {
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

}
