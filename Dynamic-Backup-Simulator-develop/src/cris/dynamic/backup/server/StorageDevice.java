package cris.dynamic.backup.server;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.Restore;

public class StorageDevice implements Comparable<StorageDevice> {

    private final String            name;
    private String                  serverName;
    private double                  throughput;
    private double                  throughputVariance;
    private double                  maxData;
    private double                  currentDataSize;
    private double                  totalActiveBackupData    = 0;
    private double                  smallestActiveBackupData = -1;
    private final ArrayList<String> activeBackups;
    
    private double  totalActiveRestoreData    = 0;
    private double  smallestActiveRestoreData = -1;
    private final ArrayList<String> activeRestores;

    public StorageDevice(final String name, final String serverName, final double throughput, final double throughputVariance,
            final double maxData, final double currentDataSize) {
        this.name = name;
        this.serverName = serverName;
        this.throughput = throughput;
        if (throughputVariance >= 1) {
            throw new RuntimeException("Throughput Variance must be less than 100%");
        }
        this.throughputVariance = throughputVariance;
        this.maxData = maxData;
        this.currentDataSize = currentDataSize;
        activeBackups = new ArrayList<String>();
        
        activeRestores = new ArrayList<String>();

    }

    /**
     * 
     * @param backup
     */
    public void addActiveBackup(final Backup backup) {
        activeBackups.add(backup.getName());
        if (backup.getDataSize() < smallestActiveBackupData) {
            smallestActiveBackupData = backup.getDataSize();
        }
    }

    //TODO move comparables to algorithms
    @Override
    public int compareTo(final StorageDevice otherStorageDevice) {
        final int compareValue = Integer.compare(getTimeRemainingThreshold(getExpectedTimeRemaining()),
                getTimeRemainingThreshold(otherStorageDevice.getExpectedTimeRemaining()));
        switch (compareValue) {
        case 1:
        case -1:
            return compareValue;
        default:
            if (activeBackups.size() > otherStorageDevice.getActiveBackups().size()) {
                return 1;
            }
            if (activeBackups.size() < otherStorageDevice.getActiveBackups().size()) {
                return -1;
            }
            return 0;
        }

    }

    public double computeThroughput() {
        final Random random = new Random();
        final double toReturn = throughput - (throughputVariance * throughput) +
                (random.nextDouble() * 2 * throughputVariance * throughput);
        return toReturn;
    }

    /**
     * @return the activeBackups
     */
    public ArrayList<String> getActiveBackups() {
        return activeBackups;
    }

    /**
     * @return the currentDataSize
     */
    public double getCurrentDataSize() {
        return currentDataSize;
    }

    public double getExpectedTimeRemaining() {
        return totalActiveBackupData / throughput;
    }

    /**
     * @return the maxData
     */
    public double getMaxData() {
        return maxData;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the serverName
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * @return the smallestActiveBackupData
     */
    public double getSmallestActiveBackupData() {
        return smallestActiveBackupData;
    }

    /**
     * @return the throughput
     */
    public double getThroughput() {
        return throughput;
    }

    /**
     * @return the throughputVariance
     */
    public double getThroughputVariance() {
        return throughputVariance;
    }

    /**
     * @return the totalActiveBackupData
     */
    public double getTotalActiveBackupData() {
        return totalActiveBackupData;
    }

    /**
     * 
     * @param backupName
     * @return
     */
    public boolean removeActiveBackup(final String backupName) {
        if (activeBackups.contains(backupName)) {
            activeBackups.remove(activeBackups.indexOf(backupName));
            return true;
        } else {
            return false;
        }
    }

    /**
     * @param currentDataSize
     *            the currentDataSize to set
     */
    public void setCurrentDataSize(double currentDataSize) {
        this.currentDataSize = currentDataSize;
    }

    /**
     * @param maxData
     *            the maxData to set
     */
    public void setMaxData(double maxData) {
        this.maxData = maxData;
    }

    /**
     * @param serverName
     *            the serverName to set
     */
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    /**
     * @param smallestActiveBackupData
     *            the smallestActiveBackupData to set
     */
    public void setSmallestActiveBackupData(double smallestActiveBackupData) {
        this.smallestActiveBackupData = smallestActiveBackupData;
    }

    /**
     * @param throughput
     *            the throughput to set
     */
    public void setThroughput(double throughput) {
        this.throughput = throughput;
    }

    /**
     * @param throughputVariance
     *            the throughputVariance to set
     */
    public void setThroughputVariance(double throughputVariance) {
        this.throughputVariance = throughputVariance;
    }

    /**
     * @param totalActiveBackupData
     *            the totalActiveBackupData to set
     */
    public void setTotalActiveBackupData(double totalActiveBackupData) {
        this.totalActiveBackupData = totalActiveBackupData;
    }

    public void updateSmallestBackup(final Map<String, Backup> backups) {
        if (activeBackups.size() == 0) {
            smallestActiveBackupData = -1;
        } else {
            for (final String activeBackup : activeBackups) {
                if (backups.get(activeBackup).getDataLeft() < smallestActiveBackupData || smallestActiveBackupData == -1) {
                    smallestActiveBackupData = backups.get(activeBackup).getDataLeft();
                }
            }
        }
    }

    public void updateTotalActiveBackup(final Map<String, Backup> backups) {
        double sum = 0;
        for (final String activeBackup : activeBackups) {
            sum += backups.get(activeBackup).getDataLeft();
        }
        totalActiveBackupData = sum;
    }

    private int getTimeRemainingThreshold(final double timeRemaining) {
        if (isBetween(timeRemaining, 0, 30)) {
            return 4;
        } else if (isBetween(timeRemaining, 30, 180)) {
            return 3;
        } else if (isBetween(timeRemaining, 180, 300)) {
            return 2;
        } else if (isBetween(timeRemaining, 300, 600)) {
            return 1;
        } else {
            return 0;
        }
    }
    
	public void addActiveRestore(Restore restore) {
		activeRestores.add(restore.getRestoreName());
        if (restore.getDataSize() < smallestActiveRestoreData) {
        	smallestActiveRestoreData = restore.getDataSize();
        }
	}

    public ArrayList<String> getActiveRestores() {
		return activeRestores;
	}

	public double getTotalActiveRestoreData() {
		return totalActiveRestoreData;
	}

	public void setTotalActiveRestoreData(double totalActiveRestoreData) {
		this.totalActiveRestoreData = totalActiveRestoreData;
	}

	public double getSmallestActiveRestoreData() {
		return smallestActiveRestoreData;
	}

	public void setSmallestActiveRestoreData(double smallestActiveRestoreData) {
		this.smallestActiveRestoreData = smallestActiveRestoreData;
	}

	private boolean isBetween(double x, double lower, double upper) {
        return lower <= x && x <= upper;
    }
	
	public boolean removeRestores(String restoreName) {
		return activeRestores.remove(restoreName);
	}
	
	public void updateTotalActiveRestore(Map<String, Restore> restoreMap) {
		double sum = 0;
        for (final String activeRestore : activeRestores) {
        	Restore restore = restoreMap.get(activeRestore);
            sum += restore.getDataLeft();
        }
        totalActiveRestoreData = sum;
	}
	public void updateSmallestRestore(Map<String, Restore> restoreMap) {
		if (restoreMap.isEmpty()) {
            smallestActiveRestoreData = -1;
        } else {
            for (final String activeRestore : activeRestores) {
            	Restore restore = restoreMap.get(activeRestore);
                if (restore.getDataLeft() < smallestActiveRestoreData || smallestActiveRestoreData == -1) {
                	smallestActiveRestoreData = restoreMap.get(activeRestore).getDataLeft();
                }
            }
        }
	}

}
