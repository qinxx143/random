package cris.dynamic.backup;

import java.util.Random;

import cris.dynamic.backup.infrastructure.Constraint;

public class Backup {

    private final String name;
    private double       fullSize;
    private double       dataSize;
    private double       dataLeft;
    private double       criticality;
    private String       clientName;
    private String       storageName;
    private String       serverName;
    private String       RTO;
    private String       backupType;
    private String       dailyBackupType;
    private boolean      active    = false;
    private boolean      completed = false;
    private long         startTime = 0;
    private long         endTime   = 0;
    private int          RPO;
    private int          backupFrequency;
    private int          fullBackupFrequency;

    private Constraint   constraint;

    //TODO	private double priorityValue = 0;

    public Backup(final String name, final double dataSize, final String RTO, final int RPO) {
        this.name = name;
        fullSize = dataSize;
        this.dataSize = dataSize;
        dataLeft = dataSize; 
        this.RTO = RTO;
        this.RPO = RPO; 
    }

    public double getCriticality() {
    		return criticality;
    }
    
    public String getRTO() {
    		return RTO;
    }
    
    public int getRPO() {
    		return RPO;
    }
    /**
     * @return the clientName
     */
    public String getClientName() {
        return clientName;
    }

    /**
     * @return the constraint
     */
    public Constraint getConstraint() {
        return constraint;
    }

    /**
     * @return the dataLeft
     */
    public double getDataLeft() {
        return dataLeft;
    }

    /**
     * @return the dataSize
     */
    public double getDataSize() {
        return dataSize;
    }

    public long getDuration() {
        return endTime - startTime;
    }

    /**
     * @return the endTime
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Calculated and return the throughput of the backup.
     * 
     * @return throughput
     */
    public double getOverallThroughput() {
        if (endTime == 0) {
            //throw an error if the backup completed without time occuring. This should not happen obviously.
            throw new RuntimeException("getOverallThroughput called when endTime == 0.");
        } else {
            return dataSize / (endTime - startTime) * 1000;
        }
    }

    /**
     * @return the serverName
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * @return the startTime
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @return the storageName
     */
    public String getStorageName() {
        return storageName;
    }
    
    public int getBackupFrequency() {
		return backupFrequency;
	}

    public String getBackupType() {
		return backupType;
	}
    
    public int getFullBackupFrequency() {
		return fullBackupFrequency;
	}
    
	public String getDailyBackupType() {
		return dailyBackupType;
	}
    
    /**
     * @return whether the backup is active
     */
    public boolean isActive() {
        return active;
    }

    public boolean isCompleted() {
        return completed;
    }

    /**
     * This method sets the data size based on an incremental distribution.
     */
    public void makeIncrementalBackup() {
        final Random rand = new Random();
        double percentageFull = 15 + rand.nextGaussian() * 5;
        if (percentageFull < 5) {
            percentageFull = 5;
        }
        dataSize = fullSize * 0.01 * percentageFull;
        dataLeft = dataSize;
    }

    /**
     * 
     */
    public boolean missedBackupWindow(final long completedTime) {
        return constraint.missedWindow(completedTime);
    }

    /**
     * Resets the backup for a new day.
     */
    public void resetBackup(final double dataSizeChange, final boolean incremental) {
        //TODO fix this.
        final Random random = new Random();
        fullSize = fullSize - (dataSizeChange * fullSize) + (random.nextDouble() * 2 * dataSizeChange * fullSize);
        dataSize = fullSize;
        dataLeft = fullSize;
        if (incremental) {
            makeIncrementalBackup();
        }
        storageName = "";
        serverName = "";
        completed = false;
        active = false;
    }

    /**
     * @param active
     *            the active to set
     */
    
    public void setCriticality(double criticality) {
    		this.criticality = criticality;
    }
    
    public void setRTO(String RTO) {
    		this.RTO = RTO;
    }
    
    public void setRPO(int RPO) {
    		this.RPO = RPO;
    }
    
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @param clientName
     *            the clientName to set	
     */
    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    /**
     * @param completed
     *            the completed to set
     */
    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    /**
     * @param constraint
     *            the constraint to set
     */
    public void setConstraint(final Constraint constraint) {
        this.constraint = constraint;
    }

    /**
     * @param dataLeft
     *            the dataLeft to set
     */
    public void setDataLeft(double dataLeft) {
        this.dataLeft = dataLeft;
    }

    /**
     * @param dataSize
     *            the dataSize to set
     */
    public void setDataSize(double dataSize) {
        fullSize = dataSize;
    }

    /**
     * @param endTime
     *            the endTime to set
     */
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    /**
     * @param serverName
     *            the serverName to set
     */
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    /**
     * @param startTime
     *            the startTime to set
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * @param storageName
     *            the storageName to set
     */
    public void setStorageName(String storageName) {
        this.storageName = storageName;
    }
    
    public void setBackupFrequency(int backupFrequency) {
		this.backupFrequency = backupFrequency;
	}
    
    public void setBackupType(String backupType) {
		this.backupType = backupType;
	}
    
    public void setFullBackupFrequency(int fullBackupFrequency) {
		this.fullBackupFrequency = fullBackupFrequency;
	}
    
	public void setDailyBackupType(String dailyBackupType) {
		this.dailyBackupType = dailyBackupType;
	}

    /**
     * 
     * @param time
     *            amount of time to make pass
     * @param throughput
     *            allocated throughput
     * @return boolean true if this step completes the backup, else false
     */
    public boolean step(final int time, final double throughput) {
        if (active && !completed) {
            //calculate bandwidth
            dataLeft = dataLeft - throughput * .001 * time;
            if (dataLeft <= 0) {
                return true;
            }
        }
        return false;

    }




	

}
