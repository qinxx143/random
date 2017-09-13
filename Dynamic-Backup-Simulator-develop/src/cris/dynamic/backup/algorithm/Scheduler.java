package cris.dynamic.backup.algorithm;

import java.util.Map;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.Restore;
import cris.dynamic.backup.client.Client;
import cris.dynamic.backup.infrastructure.Constraints;
import cris.dynamic.backup.server.MediaServer;
import cris.dynamic.backup.server.StorageDevice;

public abstract class Scheduler {

    private Map<String, Backup>        backups;
    private Map<String, StorageDevice> storageDevices;
    private Map<String, MediaServer>   servers;
    private Map<String, Client>        clients;
    private Map<String, Restore>       restores;

    private Constraints                constraints;
    private int                        fullBackupFrequency; 
    private String 					  backupType;
    

    /**
     * @return the backups
     */
    public Map<String, Backup> getBackups() {
        return backups;
    }

    /**
     * @return the clients
     */
    public Map<String, Client> getClients() {
        return clients;
    }

    /**
     * @return the constraints
     */
    public Constraints getConstraints() {
        return constraints;
    }

    public abstract String getName();

    public abstract Map<String, String> getNewBackups(final long currentTime);

    /**
     * @return the servers
     */
    public Map<String, MediaServer> getServers() {
        return servers;
    }

    /**
     * @return the storageDevices
     */
    public Map<String, StorageDevice> getStorageDevices() {
        return storageDevices;
    }

    public void incrementDay() {
        //By default do nothing. Algorithms that need to keep track of day must override this.
    }

    public void notateHistoricalData(final Backup completedBackup) {
        //By default do nothing. Algorithms that keep track of historical data must override this.
    }

    public void removeOldHistoricalData(final int day) {
        //By default do nothing. Algorithms that keep track of historical data must override this.
    }

    /**
     * @param backups
     *            the backups to set
     */
    public void setBackups(Map<String, Backup> backups) {
        this.backups = backups;
    }

    /**
     * @param clients
     *            the clients to set
     */
    public void setClients(Map<String, Client> clients) {
        this.clients = clients;
    }

    /**
     * @param constraints
     *            the constraints to set
     */
    public void setConstraints(Constraints constraints) {
        this.constraints = constraints;
    }

    /**
     * @param servers
     *            the servers to set
     */
    public void setServers(Map<String, MediaServer> servers) {
        this.servers = servers;
    }

    /**
     * @param storageDevices
     *            the storageDevices to set
     */
    public void setStorageDevices(Map<String, StorageDevice> storageDevices) {
        this.storageDevices = storageDevices;
    }
    
    public Map<String, Restore> getRestores() {
		return restores;
	}

	public void setRestores(Map<String, Restore> restores) {
		this.restores = restores;
	}

	//TODO using mathematical model
    public int computeFullBackupFrequency(int fullBackupFrequency) {
    		return fullBackupFrequency;
    }
    
    //TODO using mathematical model
    public String computeBackupTpye(String backupType) {
    		return backupType;
    }
    

}
