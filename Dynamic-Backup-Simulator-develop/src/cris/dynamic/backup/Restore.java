package cris.dynamic.backup;

import javax.management.loading.PrivateClassLoader;

import cris.dynamic.backup.system.Helper;

public class Restore {
	
	private String 	restoreName;
	private String  requestTime; 
	
	private int     requestDay;
	private double  fullSize;
    private double  dataSize;
    private double  dataLeft;
	private String	clientName;//客户端
	private String  storageName;//存储器
    private String  serverName;//服务器
    private String  snapshotChainName;//快照的名字
    private String 	DFOAName;
    private String 	DFOBName;
    
    private boolean      active    = false;
    private boolean      completed = false;
    private long         startTime = 0;
    private long         endTime   = 0;
    private long 		 penalty = 0;
    
    
	public Restore() {
	}
	public Restore(String name, double dataSize, String storageName) {
		this.restoreName = name;
        fullSize = dataSize;
        this.dataSize = dataSize;
        dataLeft = dataSize;
        this.storageName = storageName;
	}
	public String getRestoreName() {
		return restoreName;
	}
	public void setRestoreName(String restoreName) {
		this.restoreName = restoreName;
	}
	public double getDataSize() {
		return dataSize;
	}
	public void setDataSize(double dataSize) {
		this.dataSize = dataSize;
	}
	public String getClientName() {
		return clientName;
	}
	public void setClientName(String clientName) {
		this.clientName = clientName;
	}
	public String getStorageName() {
		return storageName;
	}
	public void setStorageName(String storageName) {
		this.storageName = storageName;
	}
	public String getServerName() {
		return serverName;
	}
	public void setServerName(String serverName) {
		this.serverName = serverName;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}
	public boolean isCompleted() {
		return completed;
	}
	public void setCompleted(boolean completed) {
		this.completed = completed;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public long getEndTime() {
		return endTime;
	}
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	public double getFullSize() {
		return fullSize;
	}
	public void setFullSize(double fullSize) {
		this.fullSize = fullSize;
	}
	public double getDataLeft() {
		return dataLeft;
	}
	public void setDataLeft(double dataLeft) {
		this.dataLeft = dataLeft;
	}
	public String getSnapshotChainName() {
		return snapshotChainName;
	}
	public void setSnapshotChainName(String snapshotChainName) {
		this.snapshotChainName = snapshotChainName;
	}
	
	public boolean step(int time, Double throughput) {
		if (active && !completed) {
            //calculate bandwidth
            dataLeft = dataLeft - throughput * .001 * time;
            if (dataLeft <= 0) {
                return true;
            }
        }
        return false;
	}
	 public long getDuration() {
        return (endTime - startTime) + this.penalty;
    }
	public double getOverallThroughput() {
		if (endTime == 0) {
            //throw an error if the backup completed without time occuring. This should not happen obviously.
            throw new RuntimeException("getOverallThroughput called when endTime == 0.");
        } else {
            return dataSize / (endTime - startTime) * 1000;
        }
	}
	public String getRequestTime() {
		return requestTime;
	}
	public void setRequestTime(int requestTime) {
		this.requestTime = Helper.convertToTimestamp(requestTime * 1000);
	}
	public int getRequestDay() {
		return requestDay;
	}
	public void setRequestDay(int requestDay) {
		this.requestDay = requestDay;
	}
	public String getDFOAName() {
		return DFOAName;
	}
	public void setDFOAName(String dFOAName) {
		DFOAName = dFOAName;
	}
	public String getDFOBName() {
		return DFOBName;
	}
	public void setDFOBName(String dFOBName) {
		DFOBName = dFOBName;
	}
	public void addPenalty(int penalty) {
		this.penalty = penalty * 1000;
	}
	public long getPenalty(){
		return this.penalty;
	}

}
