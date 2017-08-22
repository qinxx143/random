package cris.dynamic.backup.server;

import java.util.ArrayList;

public class MediaServer {

    private final String            name;
    private final ArrayList<String> storageDevices;
    private final ArrayList<String> backups;

    public MediaServer(final String name) {
        this.name = name;
        storageDevices = new ArrayList<String>();
        backups = new ArrayList<String>();
    }

    public void addBackup(final String backupName) {
        backups.add(backupName);
    }

    public void addStorageDevice(final String stu) {
        storageDevices.add(stu);
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    public boolean removeBackup(final String backupName) {
        final int index = backups.indexOf(backupName);
        if (index == -1) {
            return false;
        } else {
            backups.remove(index);
            return true;
        }

    }

}
