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
        private long         duration;
        private double       throughput;
        private double       dataSize;
        private final long   time;

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

    private final String        eventMessage;
    private final String        backupName;
    private final String        storageName;
    private final String        serverName;
    private final String        clientName;
    private final String        throughput;
    private final String        dataSize;
    private final long          duration;

    private final long          time;

    private LogEvent(LogBuilder builder) {
        eventMessage = builder.event.getEventText();
        backupName = builder.backupName;
        storageName = builder.storageName;
        serverName = builder.serverName;
        clientName = builder.clientName;
        final DecimalFormat format = new DecimalFormat("#.##");
        throughput = format.format(builder.throughput);
        dataSize = format.format(builder.dataSize);
        duration = builder.duration;
        time = builder.time;
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

        return toReturn;
    }


}
