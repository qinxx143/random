package cris.dynamic.backup.infrastructure;

public enum Events {

    BACKUP_START("Backup: **backup** of size: **dataSize** started to **storageDevice**"),
    BACKUP_COMPLETED("Backup: **backup** to **storageDevice** completed. Duration: **duration**. Throughput: **throughput**"),
    ALL_BACKUPS_COMPLETED("All backups completed. Total data size: **dataSize**");

    private final String eventText;

    Events(final String eventText) {
        this.eventText = eventText;
    }

    public String getEventText() {
        return eventText;
    }
}
