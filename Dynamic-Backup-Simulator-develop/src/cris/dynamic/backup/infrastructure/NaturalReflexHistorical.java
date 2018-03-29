package cris.dynamic.backup.infrastructure;

import java.util.ArrayList;

import cris.dynamic.backup.Backup;
import sun.net.www.protocol.http.spnego.NegotiateCallbackHandler;

public class NaturalReflexHistorical {
	private String backupName;
	private String dailyBackupType;
	
	private double size;
	private double backupThroughput;
	
	private ArrayList waitTime;  //no data for now
	private ArrayList<Double> throughput;
	private ArrayList<Double> fullSize;
	private ArrayList<Double> increSize;
	
	
	public NaturalReflexHistorical(Backup backup) {
		this.backupName = backup.getName();
		this.dailyBackupType = backup.getDailyBackupType();
		this.backupThroughput = backup.getOverallThroughput();
		this.size = backup.getDataSize();
	}
	
	public void notateNaturalReflexHistorical() {
		if(waitTime == null) {
			waitTime = new ArrayList<>();
		}else {
			
		}
		
		if(throughput == null) {
			throughput = new ArrayList<Double>();
			throughput.add(backupThroughput);
		}else {
			throughput.add(backupThroughput);
		}
		
		if(dailyBackupType=="full") {
			if(fullSize == null) {
				fullSize = new ArrayList<Double>();
				fullSize.add(size);
			}else {				 
				fullSize.add(size);				
			}
			
//			if (increSize != null) {
//				increSize.clear();
//			}
		}
		
		if(dailyBackupType=="incre") {
			if(increSize == null) {
				increSize = new ArrayList<Double>();
				increSize.add(size);
			}else {
				increSize.add(size);						
			}
		}
		

	}
	
	public void updateNRHistorical(Backup backup) {
		this.backupName = backup.getName();
		this.dailyBackupType = backup.getDailyBackupType();
		this.backupThroughput = backup.getOverallThroughput();
		this.size = backup.getDataSize();
	}
	
	public ArrayList<Double> getfullSize(){
		return fullSize;
	}
	
	public ArrayList<Double> getIncreSize(){
		return increSize;
	}

}
