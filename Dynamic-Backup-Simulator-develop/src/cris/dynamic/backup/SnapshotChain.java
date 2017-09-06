package cris.dynamic.backup;

public class SnapshotChain {
	
	private String backupName;
	private String backupType;
	private String dailyBackupType;
	private String clientName;
	private String serverName;
	private String storageName;
	private double dataSize;
	private int iterationNumber;
	
	public String getBackupName() {
		return backupName;
	}
	
	public void setBackupName(String backupName) {
		this.backupName = backupName;
	}
	
	public String getBackupType() {
		return backupType;
	}
	
	public void setBackupType(String backupType) {
		this.backupType = backupType;
	}
	
	public String getDailyBackupType() {
		return dailyBackupType;
	}
	
	public void setDailyBackupType(String dailyBackupType) {
		this.dailyBackupType = dailyBackupType;
	}
	
	public String getClientName() {
		return clientName;
	}
	
	public void setClientName(String clientName) {
		this.clientName = clientName;
	}
	
	public String getServerName() {
		return serverName;
	}
	
	public void setServerName(String serverName) {
		this.serverName = serverName;
	}
	
	public double getDataSize() {
		return dataSize;
	}
	
	public void setDataSize(double dataSize) {
		this.dataSize = dataSize;
	}

	public int getIterationNumber() {
		return iterationNumber;
	}

	public void setIterationNumber(int iterationNumber) {
		this.iterationNumber = iterationNumber;
	}

	public String getStorageName() {
		return storageName;
	}

	public void setStorageName(String storageName) {
		this.storageName = storageName;
	} 
	
	

}
