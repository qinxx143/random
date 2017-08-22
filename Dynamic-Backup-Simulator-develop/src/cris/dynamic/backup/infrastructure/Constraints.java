package cris.dynamic.backup.infrastructure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.client.Client;

public class Constraints {

    //TODO This class should be a helper class and constraints should be moved as private fields inside a backup.

    private final Map<String, Constraint> constraints;
    private int                           windowSize        = -1; //
    private final long                    overallWindowSize = -1; //overall backup window size used for randomly generated backup windows.

    public Constraints() {
        constraints = new HashMap<String, Constraint>();
    }

    public Constraints(final int windowSize, final long overallWindowSize) {
        constraints = new HashMap<String, Constraint>();
        this.windowSize = windowSize;

    }

    /**
     * Adds a constraint to a backup.
     * 
     * @param backupName
     * @param constraint
     */
    public void addConstraint(final String backupName, final Constraint constraint) {
        constraints.put(backupName, constraint);
    }

    /**
     * Returns whether the backup can started based on the backup window
     * 
     * @param backup
     * @param startTime
     * @return
     */
    public boolean canBackupStart(final String backup, final Long startTime) {
        return constraints.get(backup).canStart(startTime);
    }

    public void fillWindowAndConstraints(final ArrayList<String> storageList, final Map<String, Client> clientMap,
            final Map<String, Backup> backupMap) {
        //		if(randomizedConstraints > 0){
        //			giveRandomStorageConstraints(storageList, randomizedConstraints);
        //		}
        if (windowSize > 0) {
            generateMultiplicativeWindowSizes(clientMap, backupMap);
        }

    }

    public void generateMultiplicativeWindowSizes(final Map<String, Client> clientMap, final Map<String, Backup> backupMap) {
        for (final Map.Entry<String, Constraint> constraintEntry : constraints.entrySet()) {
            constraintEntry.getClass();
            final double associatedClientThroughput = clientMap.get(backupMap.get(constraintEntry.getKey()).getClientName()).getThroughput();
            final double backupSize = backupMap.get(constraintEntry.getKey()).getDataSize();
            constraintEntry.getValue().setMultiplicativeBackupWindow(backupSize, windowSize, associatedClientThroughput, overallWindowSize);
        }
    }

    public Constraint getConstraint(final String backupName) {
        return constraints.get(backupName);
    }

    public ArrayList<Constraint> getConstraints() {
        return new ArrayList<Constraint>(constraints.values());
    }


    /**
     * @return the windowSize
     */
    public long getWindowSize() {
        return windowSize;
    }

    /**
     * Returns whether the given server is a valid option.
     * 
     * @param backup
     * @param server
     * @return
     */
    public boolean isValidServer(final String backup, final String server) {
        return constraints.get(backup).isServerValid(server);
    }

    /**
     * Returns whether a given storage unit is a valid option.
     * 
     * @param backup
     * @param storageUnit
     * @return
     */
    public boolean isValidStorageUnit(final String backup, final String storageUnit) {
        return constraints.get(backup).isStorageValid(storageUnit);
    }

    /**
     * Returns whether the backup window was missed
     * 
     * @param backup
     * @param endTime
     * @return
     */
    public boolean missedWindow(final String backup, final Long endTime) {
        return constraints.get(backup).missedWindow(endTime);
    }

    /**
     * @param windowSize
     *            the windowSize to set
     */
    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

}
