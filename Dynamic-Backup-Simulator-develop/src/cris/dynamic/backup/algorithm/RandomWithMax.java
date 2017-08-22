package cris.dynamic.backup.algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import cris.dynamic.backup.Backup;

public class RandomWithMax extends Scheduler {

    private int activeBackups;
    private int backupsRemaining;

    private int maxActive;

    public RandomWithMax(int maxActive) {
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
        return "RandomWithMax(" + maxActive + ")";
    }

    /**
     * Returns a list of backup and storage device pairs.
     */
    @Override
    public Map<String, String> getNewBackups(final long currentTime) {
        updateActiveAndRemaining();

        final Map<String, Backup> backupsCopy = new HashMap<String, Backup>();
        backupsCopy.putAll(super.getBackups());

        final Map<String, String> backupAssignments = new HashMap<String, String>();
        while (activeBackups < maxActive && backupsRemaining > 0) {
            boolean started = false;
            for (final Iterator<Map.Entry<String, Backup>> iter = backupsCopy.entrySet().iterator(); iter.hasNext();) {
                final Map.Entry<String, Backup> backupEntry = iter.next();
                if (started) {
                    break;
                }
                if (!backupEntry.getValue().isActive()) {
                    //set active

                    //storage selection
                    final Random random = new Random();
                    final ArrayList<String> keys = new ArrayList<String>(super.getStorageDevices().keySet());

                    boolean isValidSTU = false;
                    String storageName = "";
                    while (!isValidSTU) {
                        storageName = keys.get(random.nextInt(keys.size()));
                        if (super.getConstraints().isValidStorageUnit(backupEntry.getKey(), storageName)) {
                            isValidSTU = true;
                        }
                    }
                    backupAssignments.put(backupEntry.getKey(), storageName);
                    iter.remove();
                    activeBackups++;
                    backupsRemaining--;
                    started = true;
                }
            }
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
