package cris.dynamic.backup.infrastructure;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Constraint {

    private final String       backupName;
    private final List<String> validServers;
    private final List<String> validStorageUnits;
    private double             startConstraint;
    private double             endConstraint;

    public Constraint(final String backupName) {
        //Constructor for empty constraints
        this.backupName = backupName;
        validServers = new ArrayList<String>(); //empty = any allowed
        validStorageUnits = new ArrayList<String>(); //empty = any allowed
        startConstraint = 0;
        endConstraint = -1;
    }

    public Constraint(final String backupName, String storageUnits, String servers,
            final String startConstraint, final String endConstraint) {

        this.backupName = backupName;
        validServers = new ArrayList<String>(); //empty indicates no constraints, any allowed
        validStorageUnits = new ArrayList<String>(); //empty indicates no constraints, any allowed

        //Parse storage constraints
        if (!"*".equals(storageUnits) && !"".equals(storageUnits)) {
            storageUnits = storageUnits.replace("{", "");
            storageUnits = storageUnits.replace("}", "");
            final String[] splitSTUS = storageUnits.split("\\|");
            for (int i = 0; i < splitSTUS.length; i++) {
                validStorageUnits.add(splitSTUS[i].replaceAll("\\s+", ""));
            }
        }

        //Parse server constraints
        if (!"*".equals(servers) && !"".equals(servers)) {
            servers = servers.replace("{", "");
            servers = servers.replace("}", "");
            final String[] splitServers = servers.split("\\|");
            for (int i = 0; i < splitServers.length; i++) {
                validServers.add(splitServers[i].replaceAll("\\s+", ""));
            }
        }

        //Parse start Constraint
        if (!"*".equals(startConstraint) && !"".equals(startConstraint)) {
            this.startConstraint = Long.parseLong(startConstraint);
        } else {
            this.startConstraint = 0;
        }

        //Parse end Constraint
        if (!"*".equals(endConstraint) && !"".equals(endConstraint)) {
            this.endConstraint = Long.parseLong(endConstraint);
        } else {
            this.endConstraint = -1;
        }

    }

    /**
     * Add a server option to the backup.
     * 
     * @param server
     */
    public void addServerConstraint(final String server) {
        validServers.add(server);
    }

    /**
     * Adds a backup option to the backup.
     * 
     * @param stu
     */
    public void addStorageConstraint(final String stu) {
        validStorageUnits.add(stu);
    }

    /**
     * Returns whether the backup can started based on start time constraints.
     * 
     * @param startTime
     * @return boolean
     */
    public boolean canStart(final Long startTime) {
        if (startTime < startConstraint) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * @return the endConstraint
     */
    public double getEndConstraint() {
        return endConstraint;
    }

    /**
     * @return the startConstraint
     */
    public double getStartConstraint() {
        return startConstraint;
    }

    /**
     * Assigns random storage constraints.
     * 
     * @param storageList
     * @param numConstraints
     */
    public void giveRandomStorageConstraint(final ArrayList<String> storageList, final int numConstraints) {
        final Random randomGen = new Random();
        int numAdded = 0;
        while (numAdded != numConstraints) {
            if (storageList.size() == 0) {

                throw new RuntimeException("Unable to add the requested number of valid storage units for backup: " + backupName + ".");
            }
            final int randomIndex = randomGen.nextInt(storageList.size());
            if (!validStorageUnits.contains(storageList.get(randomIndex))) {
                //Add it and increment number added
                validStorageUnits.add(storageList.get(randomIndex));
                ++numAdded;
            } //else: This storage unit is already a constraint. Choose another and remove this one.

            storageList.remove(randomIndex);
        }
    }

    /**
     * Returns whether the server is valid.
     * 
     * @param serverName
     * @return boolean
     */
    public boolean isServerValid(final String serverName) {
        if (validServers.size() == 0) {
            return true;
        } else {
            return validServers.contains(serverName);
        }
    }

    /**
     * Returns whether the storage unit is valid.
     * 
     * @param storageName
     * @return boolean
     */
    public boolean isStorageValid(final String storageName) {
        if (validStorageUnits.size() == 0) {
            return true;
        } else {
            return validStorageUnits.contains(storageName);
        }
    }

    /**
     * Returns whether the backup missed the backup windo.
     * 
     * @param endTime
     * @return
     */
    public boolean missedWindow(final Long endTime) {
        if (endConstraint == -1) {
            return false;
        }
        if (endTime > endConstraint) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * @param endConstraint
     *            the endConstraint to set
     */
    public void setEndConstraint(double endConstraint) {
        this.endConstraint = endConstraint;
    }

    /**
     * This function creates a backup window that is a multiplicative size of
     * the expected backup window. This needs to be revisited before it is used.
     */
    public void setMultiplicativeBackupWindow(final double backupSize, final int windowSize,
            final double associatedClientThroughput, final long overallWindowSize) {
        if (overallWindowSize == -1) {
            startConstraint = 0;
        } else {
            final double latestStartTime = (overallWindowSize - (backupSize / associatedClientThroughput) * windowSize);
            startConstraint = (Math.random() * latestStartTime);
        }
        endConstraint = (startConstraint + (backupSize / associatedClientThroughput * windowSize)) * 1000;
    }

    /**
     * @param startConstraint
     *            the startConstraint to set
     */
    public void setStartConstraint(double startConstraint) {
        this.startConstraint = startConstraint;
    }

    @Override
    public String toString() {

        //storage units
        String storageString = "Valid Storage Units: ";
        for (final String stu : validStorageUnits) {
            storageString += stu + "  ";
        }

        //servers
        String serverString = "Valid Servers: ";
        for (final String stu : validServers) {
            serverString += stu + "  ";
        }

        final String toReturn = "Backup Constraints: " + backupName + "\r\n"
                + storageString + "\r\n"
                + serverString + "\r\n"
                + "Start Constraint: " + startConstraint + "\r\n"
                + "End Constraint: " + endConstraint;

        return toReturn;
    }
}
