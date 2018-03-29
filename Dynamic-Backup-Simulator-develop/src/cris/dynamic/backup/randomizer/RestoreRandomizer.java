package cris.dynamic.backup.randomizer;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Random;

import cris.dynamic.backup.DynamicBackupSimulator;
import cris.dynamic.backup.Restore;
import sun.management.VMOptionCompositeData;

public class RestoreRandomizer {
	
	private static final String	outputFileName = "restore2018";
	
	//private static final  int    restoreAtomic = 3600; 
	
	private static final double  faultRate= 0.05;
	
	private static PrintWriter restoreWriter;
	
	private static ArrayList<Restore> restoreList;
	
	private static Restore restore;
	
	public static void main(String[] args) throws Exception {
		final File f = new File(outputFileName + ".system");
        if (f.exists()) {
            throw new RuntimeException("A file of that name already exists. Delete file or change outputFile name in randomizer.");
        }
        
        restoreWriter = new PrintWriter(outputFileName + ".system", "UTF-8");
        
        restoreRquestGenerate();
        
        restoreWriter.close();

	}
	
	public static void restoreRquestGenerate() {
		restoreWriter.println("*****Restore Request*****");
		for (int i = 1; i <= DynamicBackupSimulator.iterations ; i++) {
			restoreWriter.println("Day "+ " " + i);			
			restoreList = new ArrayList<Restore>();
	        Random random = new Random();
	        for (int j = 0; j < SystemRandomizer.numClients; j++) {
	        	    restore = new Restore();
	        		restore.setRestoreName("restore"+j);
	        		int hour = random.nextInt(12);
	        		int min  = random.nextInt(60);
                int sec  =  random.nextInt(60);
	        		restore.setRequestTime(hour * 3600 + min * 60 + sec);	        		
	        		double rand2 = random.nextDouble();
	        		restoreList.add(restore);
	        		if (rand2 <= faultRate) {
	        			restoreWriter.println(restoreList.get(j).getRestoreName() + ": " + restoreList.get(j).getRequestTime());
	        		} 
	        }
	        restoreWriter.println();
		}
		
		
	}

}
