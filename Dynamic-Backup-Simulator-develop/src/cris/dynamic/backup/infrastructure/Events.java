package cris.dynamic.backup.infrastructure;

public enum Events {
//backup
    BACKUP_START("**backupType**(**dailyBackupType**): **backup** of size: **dataSize** started to **storageDevice**"),
    BACKUP_COMPLETED("**backupType**(**dailyBackupType**): **backup** to **storageDevice** completed. Duration: **duration**. Throughput: **throughput**"),
    ALL_BACKUPS_COMPLETED("All backups completed. Total data size: **dataSize**"),
    SNAPSHOTCHAINS("Day **iterationNumber**: **backup** **backupType**(**dailyBackupType**) , **dataSize** --> **storageDevice**"),
//restore
	RESTORE_START("**restore**: day **backupDay** (**dataSize**) started from **storageDevice**"),
	RESTORE_COMPLETED("**restore**: day **backupDay** completed. Duration: **duration**. Throughput: **throughput**"),
	ALL_RESTORE_COMPLETED("All restores completed. Total data size: **dataSize**. Duration: **duration**");
	
	

    private final String eventText;

    Events(final String eventText) {
        this.eventText = eventText;
    }

    public String getEventText() {
        return eventText;
    }
}
