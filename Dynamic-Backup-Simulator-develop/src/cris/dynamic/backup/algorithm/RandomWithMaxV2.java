package cris.dynamic.backup.algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.server.StorageDevice;

public class RandomWithMaxV2 extends Scheduler {

    private int activeBackups;
    private int backupsRemaining;

    private int maxActive;

    public RandomWithMaxV2(int maxActive) {
        this.maxActive = maxActive;
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

}
