package cris.dynamic.backup.system;

import java.util.HashMap;
import java.util.Map;

import cris.dynamic.backup.algorithm.DynamicAlgorithmV3;

public class test {
	
	public static void main(String[] args) throws Exception {
		
		//DynamicAlgorithmV3 scheduler= new DynamicAlgorithmV3();
		//RestoreSystem restoreSystem = new RestoreSystem("restore.system");	//TODO map cannot put the same restore for different days
		//scheduler.getNewRestores(7200*1000);
		Map<String, String> map_1 = new HashMap<String,String>();
		Map<String, String> map_2 = new HashMap<String,String>();
		map_2.put("1", "Hello");
		if (null == map_1) {
			System.out.print("has somethong");
		}

	}

}
