package cris.dynamic.backup.infrastructure;

public enum Events {

    //BACKUP_START("Backup: **backup** of size: **dataSize** started to **storageDevice**"),
    BACKUP_START("**backupType**(**dailyBackupType**): **backup** of size: **dataSize** started to **storageDevice**"),
    BACKUP_COMPLETED("**backupType**(**dailyBackupType**): **backup** to **storageDevice** completed. Duration: **duration**. Throughput: **throughput**"),
    ALL_BACKUPS_COMPLETED("All backups completed. Total data size: **dataSize**"),
    SNAPSHOTCHAINS("Day **iterationNumber**: **backupType**(**dailyBackupType**) , **dataSize** --> **storageDevice**");
	
	

    private final String eventText;

    Events(final String eventText) {
        this.eventText = eventText;
    }

    public String getEventText() {
        return eventText;
    }
}
