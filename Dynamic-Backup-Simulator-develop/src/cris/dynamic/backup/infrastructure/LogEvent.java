package cris.dynamic.backup.infrastructure;

import java.text.DecimalFormat;

import cris.dynamic.backup.system.Helper;

public class LogEvent {

    public static class LogBuilder {
        private final Events event;
        private String       backupName  = "";
        private String       storageName = "";
        private String       serverName  = "";
        private String       clientName  = "";
        private String       backupType  = "";
        private String       dailyBackupType = "";
        
        private long         duration;
        private double       throughput;
        private double       dataSize;
        private final long   time;
        private int          iterationNumber;
        
        
        public LogBuilder iterationNumber(final int iterationNumber) {
    		this.iterationNumber = iterationNumber;
    		return this;
        }
        
        public LogBuilder backupType(final String backupType) {
        		this.backupType = backupType;
        		return this;
        }
        
        public LogBuilder dailyBackupType(final String dailyBackupType) {
        		this.dailyBackupType = dailyBackupType;
        		return this;
        }
        
        public LogBuilder(final Events event, final long time) {
            this.event = event;
            this.time = time;
        }

        public LogBuilder backup(final String backupName) {
            this.backupName = backupName;
            return this;
        }

        public LogEvent build() {
            return new LogEvent(this);
        }

        public LogBuilder client(final String clientName) {
            this.clientName = clientName;
            return this;
        }

        public LogBuilder dataSize(final double dataSize) {
            this.dataSize = dataSize;
            return this;
        }

        public LogBuilder duration(final long duration) {
            this.duration = duration;
            return this;
        }

        public LogBuilder server(final String serverName) {
            this.serverName = serverName;
            return this;
        }

        public LogBuilder storage(final String storageName) {
            this.storageName = storageName;
            return this;
        }

        public LogBuilder throughput(final double throughput) {
            this.throughput = throughput;
            return this;
        }
    }

    private static final String BACKUP_SUB     = "**backup**";
    private static final String STORAGE_SUB    = "**storageDevice**";
    private static final String SERVER_SUB     = "**server**";
    private static final String CLIENT_SUB     = "**client**";
    private static final String DURATION_SUB   = "**duration**";
    private static final String DATA_SIZE_SUB  = "**dataSize**";
    private static final String THROUGHPUT_SUB = "**throughput**";
    private static final String BACKUPTYPE_SUB = "**backupType**";
    private static final String DAILYBACKUPTYPE_SUB = "**dailyBackupType**";
    private static final String ITERATION_NUMBER_SUB ="**iterationNumber**";

    private final String        eventMessage;
    private final String        backupName;
    private final String        storageName;
    private final String        serverName;
    private final String        clientName;
    private final String        throughput;
    private final String        dataSize;
    private final String        backupType;
    private final String        dailyBackupType;
    private final long          duration;
    private final long          time; 
    private final int           iterationNumber;

    private LogEvent(LogBuilder builder) {
        eventMessage = builder.event.getEventText();
        backupName = builder.backupName;
        storageName = builder.storageName;
        serverName = builder.serverName;
        clientName = builder.clientName;
        final DecimalFormat format = new DecimalFormat("#.##");
        throughput = format.format(builder.throughput);
        dataSize = format.format(builder.dataSize);
        backupType = builder.backupType;
        dailyBackupType = builder.dailyBackupType;
        duration = builder.duration;
        time = builder.time;
        iterationNumber = builder.iterationNumber;
    }

    public String getBackupType() {
		return backupType;
	}

	public String getDailyBackupType() {
		return dailyBackupType;
	}

	/**
     * @return the backupName
     */
    public String getBackupName() {
        return backupName;
    }

    /**
     * @return the clientName
     */
    public String getClientName() {
        return clientName;
    }

    /**
     * @return the dataSize
     */
    public String getDataSize() {
        return dataSize;
    }

    /**
     * @return the duration
     */
    public long getDuration() {
        return duration;
    }

    /**
     * @return the eventMessage
     */
    public String getEventMessage() {
        return eventMessage;
    }

    /**
     * @return the serverName
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * @return the storageName
     */
    public String getStorageName() {
        return storageName;
    }

    /**
     * @return the throughput
     */
    public String getThroughput() {
        return throughput;
    }

    /**
     * @return the time
     */
    public long getTime() {
        return time;
    }

    public int getIterationNumber() {
		return iterationNumber;
	}

	@Override
    public String toString() {
        String toReturn = Helper.convertToTimestamp(time);
        toReturn += " " + eventMessage;

        if (!"".equals(backupName)) {
            toReturn = toReturn.replace(BACKUP_SUB, backupName);
        }

        if (!"".equals(storageName)) {
            toReturn = toReturn.replace(STORAGE_SUB, storageName);
        }

        if (!"".equals(serverName)) {
            toReturn = toReturn.replace(SERVER_SUB, serverName);
        }

        if (!"".equals(clientName)) {
            toReturn = toReturn.replace(CLIENT_SUB, clientName);
        }

        if (!"".equals(throughput)) {
            toReturn = toReturn.replace(THROUGHPUT_SUB, throughput);
        }

        if (duration != 0) {
            toReturn = toReturn.replace(DURATION_SUB, Helper.convertToTimestamp(duration));
        }

        if (!"".equals(dataSize)) {
            toReturn = toReturn.replace(DATA_SIZE_SUB, dataSize);
        }
        
        if (!"".equals(backupType)) {
            toReturn = toReturn.replace(BACKUPTYPE_SUB, backupType);
        }
        
        if (!"".equals(dailyBackupType)) {
            toReturn = toReturn.replace(DAILYBACKUPTYPE_SUB, dailyBackupType);
        } 
        
        if (!"".equals(String.valueOf(iterationNumber))) {
            toReturn = toReturn.replace(ITERATION_NUMBER_SUB, String.valueOf(iterationNumber));
        }

        return toReturn;
    }


}
