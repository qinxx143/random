package cris.dynamic.backup.system;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.DynamicBackupSimulator;
import cris.dynamic.backup.Restore;
import cris.dynamic.backup.SnapshotChain;
import cris.dynamic.backup.algorithm.Scheduler;
import cris.dynamic.backup.client.Client;
import cris.dynamic.backup.server.MediaServer;
import cris.dynamic.backup.server.StorageDevice;
import cris.dynamic.backup.infrastructure.Events;
import cris.dynamic.backup.infrastructure.LogEvent;
import cris.dynamic.backup.infrastructure.LogEvent.LogBuilder;

public class RestoreSystem {
	

	private Map<String, Restore> restoreMap;
	
	private final PrintWriter writer;
	private final Scheduler scheduler;
	
	//统计
	private int			numActiveRestores    = 0;//当前活跃的
    private int 		numCompletedRestores = 0;//已完成的
    private int         numTotalRestores = 0;//总数量
    private int 		totalNumberOfNonLastestRevcovering = 0;
    private int 		numberOfSwithching = 0;
    
    private HashMap<String, Long>		unutilizedStorageMap;
    private Map<String, Restore> 		completedRestoresMap;
    private Map<String,Boolean>			unCompletedRequestNumberMap;//未完成的请求数量
    private Map<String, String> 		    requestStoragesMap;//请求的storage
    private Map<String,Integer>			requestRestoreDayMap;//请求还原的天数
    private Map<String, Restore>         restores;
    private static Map<String, Map<String, Restore>> dayTorestores;
    private HashMap<String, String>      durationMap;
    private HashMap<String, Long>        initialStartTime;
    private HashMap<String, ArrayList<String>>   MoSMap;
    
    //Metrics
    private double	dailyDataRestoreUp = 0;
    private long	    dailyTotalTime = 0;
    private double  totalDataRestoreUp = 0;
    private long    totalRestoreTime  = 0;
    private long    totalUnutilizedTime = 0;

	private int iterationNumber = 1;
	private long	time = 0;
	
	//parse input file 
	private static int restoreRequestDay=0;
	private static String restoreName="";
	private static String restoreRequestTime="";
	private static Restore restore;
	

	
	public RestoreSystem(PrintWriter writer,String systemRestoreFile,Scheduler scheduler) throws Exception {
		this.writer = writer;
		this.scheduler = scheduler;
		new HashMap<String,SnapshotChain>();
		this.restoreMap = new HashMap<String, Restore>();
		this.unCompletedRequestNumberMap = new HashMap<String, Boolean>();
		this.requestStoragesMap = new HashMap<String, String>();
		this.requestRestoreDayMap = new HashMap<String,Integer>();		
		
		this.unutilizedStorageMap = new HashMap<String, Long>();
		this.completedRestoresMap = new HashMap<String, Restore>();
		this.dayTorestores = new HashMap<String,Map<String, Restore>>();
		this.durationMap = new HashMap<String,String>();
		this.initialStartTime = new HashMap<String,Long>();
		this.MoSMap = new HashMap<String,ArrayList<String>>();
		parseInputFiles(systemRestoreFile);
		
	}
	
	
	private void parseRestoreSystemLine(String line, int lineCount) {
		restore = new Restore();
			
		if((line.indexOf("restore")==-1) && (line.indexOf("Day")== -1))return;
		
		if (line.contains("Day")) {
			if(restoreRequestDay != Integer.parseInt(line.substring(5, line.length()))) {
				restores = new HashMap<String, Restore>();
			}
			
			restoreRequestDay = Integer.parseInt(line.substring(5, line.length()));
		}
		if (line.contains("restore")) {
			int index = line.indexOf(":");
			restoreName = line.substring(0, index );
			restoreRequestTime = line.substring(index + 2, line.length());	
			restores.put(restoreName, restore);
			restores.get(restoreName).setRestoreName(restoreName);
			restores.get(restoreName).setRequestDay(restoreRequestDay);
			restores.get(restoreName).setRequestTime(Helper.converToTimeSeconds(restoreRequestTime));
		}
		dayTorestores.put(String.valueOf(restoreRequestDay), restores);
		
	}
	
	private void parseInputFiles(String systemRestoreFile) throws Exception {
		BufferedReader reader = null;
        String line = "";
        int lineCount = 0;
        try {
            reader = new BufferedReader(new FileReader(systemRestoreFile));

            while ((line = reader.readLine()) != null) {
                //do the thing
                line = line.trim();
                if (!"".equals(line) && line.charAt(0) != '#') { //ignore this line
                		parseRestoreSystemLine(line, lineCount);
                }
                lineCount++; //increment line count for error message
            }

        } finally {
            if (reader != null) {
                reader.close();
            }
        }
	}
	
	public static Map<String, Map<String, Restore>> getDayToRestores(){
		return dayTorestores;
	}
	
	//recover system 
	public void step(int timeStep) {
        //Only start backups every second (1000 milliseconds).
        if (time % 1000 == 0) {
        	Map<String, Restore> restoresToStart = scheduler.getNewRestores(time);
        	if (null != restoresToStart) {
        		for (final Map.Entry<String, Restore> restoresAssignment : restoresToStart.entrySet()) {
        			String name =restoresAssignment.getKey();
//            		System.out.print(iterationNumber+": " +"get new "+ name + "_" + restoresAssignment.getValue().getDataBeBackupDay()+" from " + restoresAssignment.getValue().getStorageName());
        		}    		
        	}
        	for (final Map.Entry<String, Restore> restoresAssignment : restoresToStart.entrySet()) {
        		String requestName = restoresAssignment.getKey();
        		Restore restore = restoresAssignment.getValue();
        		if(null == restore){
        			//没有备份完成，所以没有读取到数据
        			unCompletedRequestNumberMap.put(requestName, true);
        			continue;
        		}
        		    unCompletedRequestNumberMap.put(requestName, false);
        		    restore.setActive(true);
				restore.setStartTime(time);
				
				//record the actual start time for the restore
				if(null == initialStartTime.get(restore.getRestoreName())) {
					initialStartTime.put(restore.getRestoreName(), restore.getStartTime());
				}
						
				String restoreName = restore.getRestoreName();
				MediaServer mediaServer = scheduler.getServers().get(restore.getServerName());
				mediaServer.addActiveRestore(restoreName);				
				
				StorageDevice storageDevice = scheduler.getStorageDevices().get(restore.getStorageName());
				storageDevice.addActiveRestore(restore);
//				System.out.println(" "+ storageDevice.getActiveRestores());
				
				Client client = scheduler.getClients().get(restore.getClientName());
				client.addRestore(restoreName);
				
				this.restoreMap.put(restoreName, restore);				
				numActiveRestores++;
				
				printLog(writer, new LogBuilder(Events.RESTORE_START, time)
		         .restore(restore.getRestoreName())
		         .backupDay(restore.getDataBeBackupDay())
		         .dataSize(restore.getDataSize())
		         .storage(restore.getStorageName())
		         .build());
								
			}
        }
        
        //calculate the throughput for each restore
        final HashMap<String, Double> throughputMap = new HashMap<String, Double>(); //<String (backupName), Double (backupThroughput)>
        Iterator<Entry<String, StorageDevice>> iterator = scheduler.getStorageDevices().entrySet().iterator();            
        while(iterator.hasNext()){
        	Entry<String, StorageDevice> next = iterator.next();
        	String storageDeviceName = next.getKey();
        	StorageDevice storageDevice = next.getValue();
            if (storageDevice.getActiveRestores().size() == 0) {
                /**
                 * Unutilized Storage
                 */
                if (unutilizedStorageMap.containsKey(storageDeviceName)) { //If it is not already in the unutilizedMap
                    unutilizedStorageMap.put(storageDeviceName, Long.valueOf((unutilizedStorageMap.get(storageDeviceName).longValue() + timeStep)));
                } else {
                    unutilizedStorageMap.put(storageDeviceName, Long.valueOf(timeStep));
                }
                totalUnutilizedTime += timeStep;
            } else {
            	
                List<Restore> activeRestores = new ArrayList<Restore>();
                for(String restoreName : storageDevice.getActiveRestores()){
                	Restore restore = restoreMap.get(restoreName);
                	activeRestores.add(restore);
                }
                final ArrayList<Double> maxAllowed = new ArrayList<Double>();
                for (int i = 0; i < activeRestores.size(); i++) {
                	Restore restore = activeRestores.get(i);
                    //get the client associated with the backup associated with the name at index i in backupList
                	Client client = scheduler.getClients().get(restore.getClientName());
                    final double throughput = client.computeThroughput();
                    //add throughput to ArrayList, note order is important and used
                    maxAllowed.add(Double.valueOf(throughput));
                }
                //For each storage device, allocate the availableThroughput to the backups
                //smallest value gets first choice, with max available being 1/N the throughput
                //after each backup takes its bandwidth, available and max to be taken is recalculated
                //therefore if there is leftovers after even distribution it gets distributed fairly
                double availableThroughput = storageDevice.computeThroughput();
                while (!maxAllowed.isEmpty()) {
                    final double realThrought= availableThroughput / activeRestores.size();          
                    final int index = Helper.findMinIndex(maxAllowed);
                    Restore restore = activeRestores.get(index);
                    if (realThrought>=maxAllowed.get(index)) {
                        //allocate max allowed
                        throughputMap.put(restore.getRestoreName(), maxAllowed.get(index));
                        availableThroughput = availableThroughput - maxAllowed.get(index);
                    } else {
                        //allocate requested
                        throughputMap.put(restore.getRestoreName(), realThrought);
                   
                        
                        availableThroughput = availableThroughput - realThrought;
                    }
                    //remove backup from contention
                    activeRestores.remove(index);
                    maxAllowed.remove(index);
                }
            }
        }
        
        //time step progress backups
        Iterator<Entry<String, Restore>> iter = restoreMap.entrySet().iterator();
        while(iter.hasNext()){
            Entry<String, Restore> restoreEntry = iter.next();
            String restoreName = restoreEntry.getKey();
            Restore restore = restoreEntry.getValue();
            if (restore.isActive()) {
                //It is active
                final boolean completed = restore.step(timeStep, throughputMap.get(restoreName));
                if (completed || time == (23*3600 + 59*60 +59)*1000) {   //TODO force complete restore avoid the restore left for another day        
   //             	System.out.println(iterationNumber+": "+ restoreName+"_"+ restore.getDataBeBackupDay()+ " completed");
                	String requestName = restore.getRestoreName();
                    //if that step completes the backup
                restore.setRequestTime(Helper.converToTimeSeconds(dayTorestores.get(String.valueOf(iterationNumber)).get(requestName).getRequestTime()));               		             	                	
                	restore.setActive(false);
                	restore.setEndTime(time + timeStep);
                	restore.setCompleted(true);
                    numActiveRestores--;
                    totalDataRestoreUp += (long) restoreEntry.getValue().getDataSize();
                    dailyDataRestoreUp += restoreEntry.getValue().getDataSize();
                    
					this.requestStoragesMap.put(requestName, restore.getStorageName());
					//计算non-last...记录已还原的天数
					int restoreDay = restore.getRequestDay();
					this.requestRestoreDayMap.put(requestName, restoreDay);
					
                     if(null == durationMap.get(restore.getRestoreName())) {
                    	 	durationMap.put(restore.getRestoreName(), Helper.convertToTimestamp(restore.getDuration()));
                    	 	//System.out.println(restore.getRestoreName() + " " + Helper.convertToTimestamp(restore.getDuration()));
                     }else {
                    	 	long duration1 = restore.getDuration();
                    	 	long duration2 = Helper.converToTimeSeconds(durationMap.get(restore.getRestoreName()))*1000;
                    	 	String duration2_2 = durationMap.get(restore.getRestoreName());
                    	 	long duration  = duration1 + duration2;
                    	 	durationMap.replace(restore.getRestoreName(), Helper.convertToTimestamp(duration));
                    	 	//System.out.println(iterationNumber);
                    	 	//System.out.println(restore.getRestoreName() + " " + duration1 + " + " + duration2 +" = "+ duration);
                     }
				   				    
                    printLog(writer, new LogBuilder(Events.RESTORE_COMPLETED, time + timeStep)
                    .backupDay(restore.getDataBeBackupDay())
                    .restore(restore.getRestoreName())
                    .storage(restore.getStorageName())
                    .duration(restore.getDuration())
                    .throughput(restore.getOverallThroughput())
                    .build());
                    
                    dailyTotalTime += restore.getDuration();
                    totalRestoreTime += restore.getDuration();
                    
                    //remove from storage device
                    StorageDevice storageDevice = scheduler.getStorageDevices().get(restore.getStorageName());
                    storageDevice.removeRestores(restoreName);

                    //remove from server
                    MediaServer mediaServer = scheduler.getServers().get(restore.getServerName());
                    mediaServer.removeRestore(restoreName);

                    //TODO remove if I change my mind
                    completedRestoresMap.put(restoreName, restore);
                    
                    scheduler.updatePieceRestoresMap(restore);

                    iter.remove();
                }
            }
        }
        
        // force the restore not span to another day
        if (time == (23*3600 + 59*60 +59)*1000){
        		restoreMap = new HashMap<>();
        }
        
        if(!restoreMap.isEmpty()) {
            //update smallest remaining and total remaining backups for storage devices
            for (final StorageDevice stu : scheduler.getStorageDevices().values()) {
                stu.updateSmallestRestore(restoreMap);
                stu.updateTotalActiveRestore(restoreMap);
            }
        	
        }

        numCompletedRestores = 0;
        Iterator<Entry<String, Boolean>> iterator2 = unCompletedRequestNumberMap.entrySet().iterator();
        while(iterator2.hasNext()){
        	Entry<String, Boolean> next = iterator2.next();
        	if(next.getValue().equals(true)){
        		numCompletedRestores ++;
        	}
        }

        //increment time
		time += timeStep;
	}
	
	private void printLog(final PrintWriter writer, final LogEvent event) {
        writer.println(event.toString());
//        System.out.println(event.toString());
    }
	public void nextIteration() {
        //未执行完的
        for(Integer day : this.requestRestoreDayMap.values()){
        	if(day < iterationNumber){
        		totalNumberOfNonLastestRevcovering ++;
        	}
        }
		this.iterationNumber++;
		
		//TODO logging?
        writer.println("\r\nIteration " + iterationNumber + "\r\n");
//        DynamicBackupSimulator.allWriter.println("\r\nIteration " + iterationNumber + "\r\n");
        
        this.requestRestoreDayMap.clear();
        
        numActiveRestores = 0;
        unCompletedRequestNumberMap.clear();
        this.numCompletedRestores = 0;
        this.numTotalRestores = 0;
        initialStartTime = new HashMap<>();
        durationMap      = new HashMap<>();
        if(null != dayTorestores.get(String.valueOf(iterationNumber))) {
            for(Map.Entry<String, Restore> requestEntry : dayTorestores.get(String.valueOf(iterationNumber)).entrySet()){
    			{
    				this.numTotalRestores++;
    			}
    		}
        	
        }

        
		this.time = 0;
		dailyDataRestoreUp = 0;
		dailyTotalTime = 0;

		
	}
	public void writeCompletionStatistics() {
		writer.println("***** Statistics for day" + " " + iterationNumber+  "*****");
		System.out.println(iterationNumber+":");
        for (Map.Entry<String, String> durationEntry : durationMap.entrySet()) {
        	
        		long duration1 = Helper.converToTimeSeconds(durationEntry.getValue());
        		long duration2 =  initialStartTime.get(durationEntry.getKey()) - Helper.converToTimeSeconds(completedRestoresMap.get(durationEntry.getKey()).getRequestTime())*1000;   
        		long duration3 = duration1*1000 + duration2;
        		String RTO = completedRestoresMap.get(durationEntry.getKey()).getRTO();
        		long RTO_seconds = Helper.converToTimeSeconds(RTO);
        		String duration = Helper.convertToTimestamp(duration3);
        		
        		long MoS = RTO_seconds-Helper.converToTimeSeconds(duration);
        		DecimalFormat df = new DecimalFormat("0.00");
        		String MoSPecentage = df.format((float)MoS / RTO_seconds);
        		
        		String restoreNameID = durationEntry.getKey().substring(7, durationEntry.getKey().length());
        		String associatedBackupName = "backup" + restoreNameID;
        		Map<String, Integer> estimatedRtoreTime = BackupSystem.getEstimatedReatoreTime();      	
        		int predictedReatoreTime =estimatedRtoreTime.get(associatedBackupName);
        		float estimatedPerformance = (float) (RTO_seconds - predictedReatoreTime) / RTO_seconds;
        		String predictPerformance = df.format(estimatedPerformance);
        		System.out.println(durationEntry.getKey() + " estimate:" +  predictedReatoreTime + " RestoreTime:" + duration3/1000 +" RTO:"+RTO_seconds +" MoS%:"+MoSPecentage+" predict:"+predictPerformance);
        		writer.println("Total Restore Time for " + durationEntry.getKey() + ": " + durationEntry.getValue());
        		
        		
        		//final statistics
        		ArrayList<String> MoSList = new ArrayList<String>();
        		if (null == MoSMap.get(durationEntry.getKey())) {
        			MoSList.add(MoSPecentage);
        			MoSMap.put(durationEntry.getKey(), MoSList);
        		}else {
        			MoSList = MoSMap.get(durationEntry.getKey());
        			MoSList.add(MoSPecentage);
        		}
        		        		
        }
		if(iterationNumber == DynamicBackupSimulator.iterations) {
			double MoSNegative = 0;
			double MoSLessTen = 0;
			double MoSMoreTen = 0;
			double totalRequest = 0;
			double MoSMoreFifty =0;
			
			System.out.println("*****restore summary*****");
			for (Map.Entry<String, ArrayList<String>> MoSEntry: MoSMap.entrySet()) {
				String restoreName = MoSEntry.getKey();
				for (int i=0; i< MoSEntry.getValue().size();i++) {
					totalRequest ++;
					if (Double.valueOf(MoSEntry.getValue().get(i)) < -0.1) {
						MoSNegative ++;
					}
					
					if (Double.valueOf(MoSEntry.getValue().get(i)) >=-0.1 && Double.valueOf(MoSEntry.getValue().get(i)) <= 0.1){
						MoSLessTen ++;
					}
					
					if (Double.valueOf(MoSEntry.getValue().get(i)) >0.1){
						MoSMoreTen ++;
					}
					
					if (Double.valueOf(MoSEntry.getValue().get(i)) >0.5){
						MoSMoreFifty ++;
					}
				}
				System.out.print(restoreName + ": ");
				System.out.print(MoSEntry.getValue());       					
				System.out.println();
			}
			System.out.println("****Statistics Summary*****");
			System.out.println("total request: " + totalRequest);
			System.out.println("MoS<0: " + MoSNegative / totalRequest);
			System.out.println("MoS<10%: " + MoSLessTen / totalRequest);
			System.out.println("MoS>10%: " +MoSMoreTen / totalRequest);
			System.out.println("MoS>50%: " +MoSMoreFifty / totalRequest);
		}
	}
	public int getCompletedBackups() {
		return numCompletedRestores;
	}
	public int getTotalBackups() {
		return numTotalRestores;
	}
	public String getTotalRecoverTime() {
		return Helper.convertToTimestamp(this.totalRestoreTime);
	}
	public int getTotalNumberOfNonLastestRevcovering() {
		return totalNumberOfNonLastestRevcovering;
	}
	public double getTotalDataRecoverUp() {
		return totalDataRestoreUp;
	}
	public long getTotalRestoreTime() {
		return totalRestoreTime;
	}
	public int getNumberOfSwithching() {
		return numberOfSwithching;
	}

}
