package cris.dynamic.backup.system;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import com.sun.corba.se.impl.orb.ParserTable.TestAcceptor1;

import cris.dynamic.backup.Backup;
import cris.dynamic.backup.Restore;
import cris.dynamic.backup.SnapshotChain;
import cris.dynamic.backup.algorithm.Scheduler;

public class RestoreSystem {
	
	private static final int penalty = 10;
	/**
	 * 所有的快照
	 */
	private Map<String,SnapshotChain> snapshotChains;
	
	/**
	 * 请求列表
	 */
	//private Map<String, RestoreRequest> requests;
	
	/**
	 * 所有还原的数据
	 */
	private Map<String, Restore> restoreMap;
	
//	private final PrintWriter writer;
//	private final PrintWriter writer2;
//	private final Scheduler scheduler;
	
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
    private Map<String, Map<String, Restore>> dayTorestores;
    
    //Metrics
    private double	dailyDataRestoreUp = 0;
    private long	dailyTotalTime = 0;
    private double  totalDataRestoreUp = 0;
    private long    totalRestoreTime  = 0;
    private long    totalUnutilizedTime = 0;

	private int iterationNumber = 1;
	private long	time = 0;
	
	//parse input file me
	private static int restoreRequestDay=0;
	private static String restoreName="";
	private static String restoreRequestTime="";
	private static Restore restore;
	

	
	public RestoreSystem(String systemRestoreFile) throws Exception {
		
		this.dayTorestores = new HashMap<String,Map<String, Restore>>();
		parseInputFiles(systemRestoreFile);
		
	}
	
	private void parseConfigLine(String line, int lineCount) {
		//Day  1
		//restore0: 02:00:00
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
		
		//00:09:00 DFO_B_r0_n0_c1_d4
//		if(line.indexOf("DFO_B_")==-1)return;
//		String restoreTime = line.substring(0,8);
//		String requestName = line.substring(9,line.length());
//		int restoreDay = Integer.parseInt(line.substring(line.lastIndexOf("d")+1,line.length()));
//		RestoreRequest request = new RestoreRequest();
//		request.setRestoreDay(restoreDay);
//		request.setRestoreTime(restoreTime);
//		request.setRequestName(requestName);
//		requests.put(request.getRequestName(), request);
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
                    parseConfigLine(line, lineCount);
                }
                lineCount++; //increment line count for error message
            }
            //test the reading of the restore system file
            for (final Map.Entry<String, Restore> Entry : restores.entrySet()) {
            		String x = Entry.getValue().getRestoreName();
            		String y = Entry.getValue().getRequestTime();
            		System.out.println(restoreRequestDay + " "+ x + " " + y);
            	
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
	}

}
