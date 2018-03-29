package cris.dynamic.backup.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.xml.crypto.dsig.spec.C14NMethodParameterSpec;

import org.apache.commons.math3.distribution.NormalDistribution;

import com.sun.jndi.url.dns.dnsURLContext;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.client.Client;
import cris.dynamic.backup.infrastructure.HistoricalDataPoint;
import cris.dynamic.backup.server.StorageDevice;
import cris.dynamic.backup.system.BackupSystem;
import cris.dynamic.backup.system.Helper;

public class NaturalReflex {
	private String backupName;
	private int backupFrequency; 
	private int fullBackupFrequency;
	private int estimatedWaitingTime;
	private int estimatedNumOfActiveRestore;
	private int RTO;
	private int estimatedNumOfIncremental;
	private int estimiatedReatoreTime;
	private double fullBackupSize;
	private double estimatedIncrementalSize;
	private double estimatedThroughputAssigned; //needs allocation rule to make it predictable 
	private double clientThroughput;
	private double stuThroughput;
			
	public NaturalReflex(Backup backup, Client client, StorageDevice stu) {
		backupName = backup.getName();
		//String clientName = backup.getClientName();
		fullBackupSize   = backup.getFullSize();
		clientThroughput = client.computeThroughput();
		stuThroughput    = stu.computeThroughput();
		RTO              = Helper.converToTimeSeconds(backup.getRTO());
	}
	
	
	public int computeFrequency(int RPO) {
		 backupFrequency = RPO;
		 //backupFrequency = 2;
		 return backupFrequency; 
	}
	
	public int computeFullBackupFrequency() {
		waitingTimeEstimator();
		throughputAssigedEstimator();
		incrementalSizeEstimator();
		
		double fullBackupTime = fullBackupSize/estimatedThroughputAssigned;
		double unitIncreBackupTime = estimatedIncrementalSize/estimatedThroughputAssigned;
		double estimatedNumOfIncrementalDouble = (RTO - estimatedWaitingTime - fullBackupTime) / unitIncreBackupTime ;
		estimatedNumOfIncremental = (new Double(estimatedNumOfIncrementalDouble)).intValue(); 
		fullBackupFrequency =(int) estimatedNumOfIncremental+1;
		
		estimiatedReatoreTime = estimatedWaitingTime + (new Double(fullBackupSize)).intValue() / (new Double(estimatedThroughputAssigned)).intValue() + (new Double(estimatedIncrementalSize)).intValue() * estimatedNumOfIncremental / (new Double(estimatedThroughputAssigned)).intValue();		
		//estimiatedReatoreTime = estimatedWaitingTime + (new Double(fullBackupSize)).intValue() / (new Double(estimatedThroughputAssigned)).intValue();
		if (fullBackupFrequency<=0) {
			fullBackupFrequency=1;
		}
		return fullBackupFrequency; 
	}
	
	public int dynamicFullBackupFequency() {
		waitingTimeEstimator();
		throughputAssigedEstimator();
		//incrementalSizeEstimatorAvg();
		incrementalSizeEstimatorMax();
		double fullBackupTime = fullBackupSize/estimatedThroughputAssigned;
		double unitIncreBackupTime = estimatedIncrementalSize/estimatedThroughputAssigned;
		double estimatedNumOfIncrementalDouble = (RTO - estimatedWaitingTime - fullBackupTime) / unitIncreBackupTime ;
		estimatedNumOfIncremental = (new Double(estimatedNumOfIncrementalDouble)).intValue(); 
		fullBackupFrequency = (int) estimatedNumOfIncremental+1;
		if (fullBackupFrequency<=0) {
			fullBackupFrequency=1;
		}
		estimiatedReatoreTime = estimatedWaitingTime + (new Double(fullBackupSize)).intValue() / (new Double(estimatedThroughputAssigned)).intValue() + (new Double(estimatedIncrementalSize)).intValue() * estimatedNumOfIncremental / (new Double(estimatedThroughputAssigned)).intValue();		
		return fullBackupFrequency; 
	}
	
	public int dynamicOptimalFullBackupFequency(double c1) {
		waitingTimeEstimator();
		throughputAssigedEstimator();
		double T1 = estimatedThroughputAssigned;
		double T2 = estimatedThroughputAssigned;
		double full = fullBackupSize;
		double c2 = 1-c1;
		//use DFBF model to compute the FBF
		ArrayList<Double> increList = BackupSystem.getNaturalMap().get(backupName).getIncreSize();
		if(increList==null || increList.isEmpty() || increList.size()<3) {	
			dynamicFullBackupFequency();			
		}else {
			double mean = average(increList);
			double var  = variance(increList);
			double stdvar = Math.sqrt(var);
			NormalDistribution distribution = new NormalDistribution(mean, stdvar);
	        double qStrongApproximation = (RTO * T1 * T2 - full * T2)/(T1 * distribution.inverseCumulativeProbability(c2/(c1+c2)))+1;        		
	        //System.out.println(qWeak);		
	        //System.out.println(qStrongApproximation);
			fullBackupFrequency = (int)qStrongApproximation;
			if (fullBackupFrequency<=0) {
				fullBackupFrequency=1;
			}
		}
		return fullBackupFrequency; 
	}
	
	public double throughputAssigedEstimator() {
		if(clientThroughput < stuThroughput/estimatedNumOfActiveRestore) {
			estimatedThroughputAssigned = clientThroughput;
		}else {
			numOfAactiveRestoreEstimator();
			estimatedThroughputAssigned = stuThroughput/estimatedNumOfActiveRestore;
		}
		return estimatedThroughputAssigned;
	}
	
	public void waitingTimeEstimator() {
		estimatedWaitingTime = 0;
	}
	
	public void numOfAactiveRestoreEstimator() {
		estimatedNumOfActiveRestore = 1;
	}
	
	public void incrementalSizeEstimator() {
		final Random rand = new Random();
		double percentageFull = 15 + rand.nextGaussian() * 5;
        if (percentageFull < 5) {
            percentageFull = 5;
        }
        estimatedIncrementalSize = fullBackupSize * 0.01 * percentageFull;
	}
	
	public void incrementalSizeEstimatorAvg() {		
		ArrayList<Double> increList = BackupSystem.getNaturalMap().get(backupName).getIncreSize();
		
		if(increList == null) {
			incrementalSizeEstimator();
		}else {
			if(increList.isEmpty()) {
				incrementalSizeEstimator();	
			}else
			{
				double total = 0;
				for(int i=0 ; i< increList.size(); i++) {
					total = total + increList.get(i);
				}
				double avg = total / increList.size();
				estimatedIncrementalSize =avg;
			}
			
			
		}

	}
	
	
	public void incrementalSizeEstimatorMax() {		
		ArrayList<Double> increList = BackupSystem.getNaturalMap().get(backupName).getIncreSize();
		
		if(increList == null) {
			incrementalSizeEstimator();
		}else {
			if(increList.isEmpty()) {
				incrementalSizeEstimator();	
			}else
			{
				double max = 0;
				for(int i=0 ; i< increList.size(); i++) {
					if (increList.get(i)>max){
						max = increList.get(i);
					}
				}
				estimatedIncrementalSize =max;
			}
			
			
		}

	}
	
	public int getEstimiatedRestoreTime() {		
		return estimiatedReatoreTime;
	}
	
	public static double sum(ArrayList<Double> list) {
	    double sum = 0;        
	    for(int i=0; i<list.size(); i++ ){
	        sum = sum + list.get(i) ;
	    }
	    return sum;
	}

	public static double average(ArrayList<Double> list) {  
	    double average = sum(list)/list.size();
	    return average;
	}
	
	public static double variance(ArrayList<Double> list) {
		   double sumDiffsSquared = 0.0;
		   double avg = average(list);
		   for (Double value : list)
		   {
		       double diff = value - avg;
		       diff *= diff;
		       sumDiffsSquared += diff;
		   }
		   return sumDiffsSquared  / (list.size()-1);
		}
	

}
