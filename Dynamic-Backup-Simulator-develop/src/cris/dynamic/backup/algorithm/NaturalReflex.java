package cris.dynamic.backup.algorithm;

public class NaturalReflex {
	private int backupFrequency; 
	
	
	public int computeFrequency(int RPO) {
		 backupFrequency = RPO;
		 return backupFrequency; 
	}

}
