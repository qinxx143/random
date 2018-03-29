package test;

import java.util.ArrayList;
import java.util.Random;

import javax.swing.JOptionPane;

import org.apache.commons.math3.distribution.NormalDistribution;

public class testDFBF {
	
	public static void main (String[] args) {
		// client information
		double RTO = 3600 * 1.5;
		double T1  = 150;
		double T2  = 150;
		double full= 1024 * 150;
		double c1  = 0.1;
		double c2  = 1 - c1; 
		
		// incremental data 
		int numberOfIncr = 10;
		ArrayList<Double> increList;
		increList = new ArrayList<Double>();
		for(int i=0 ; i< numberOfIncr; i++) {
			double increSize = makeIncrementalBackup(full);
			increList.add(increSize);			
		}
	
		for(int i=0; i< increList.size(); i++) {
			//System.out.println(increList.get(i));
		}
		
		// learn the Gaussian distribution from the historical data
		double mean = average(increList);
		double var  = variance(increList);
		double stdvar = Math.sqrt(var);
		//System.out.println(mean);
		//System.out.println(var);
		
		// weak model
		NormalDistribution distribution = new NormalDistribution(mean, stdvar);
		double qWeak = (T2 * RTO)/mean - (T2 * full)/(T1 * mean);
        double qStrongApproximation = (RTO * T1 * T2 - full * T2)/(T1 * distribution.inverseCumulativeProbability(c2/(c1+c2)));        		
        System.out.println(qWeak);		
        System.out.println(qStrongApproximation);
        
		// strong model 
        
        double x1,x2,x12,y1,y12;
        x1=1;   x2=60;
        int qStrongOptimal = 0;
        do {    
        	x12=(x1+x2)/2;
        	double A = (RTO * T1 * T2 - full * T2)/(x1 * T1);
        	double A12 = (RTO * T1 * T2 - full * T2)/(x12 * T1);
        	y1 = distribution.cumulativeProbability(A) * mean - var * distribution.density(A) - (c2 * mean)/(c1 + c2);
        	y12 = distribution.cumulativeProbability(A12) * mean - var * distribution.density(A12) - (c2 * mean)/(c1 + c2);		
        	//y1=Math.pow(x1, 3)-Math.sqrt(x1)-5;
        //y12=Math.pow(x12, 3)-Math.sqrt(x12)-5;    
        	if(y1*y12<0) {
        		x2 = x12;       		
        	}else {
        		x1 = x12;
        	}
        }while(Math.abs(y12)>1e-1);
        qStrongOptimal = (int)x12;
        System.out.println(x12);
        System.out.println(qStrongOptimal);
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
		       diff = diff * diff;
		       sumDiffsSquared = sumDiffsSquared + diff;
		   }
		   return sumDiffsSquared  / (list.size()-1);
		}

    public static double makeIncrementalBackup(double full) {
        final Random rand = new Random(); 
        double increSize;
        double percentageFull = 15 + rand.nextGaussian() * 5;
        //double percentageFull =1;
        if (percentageFull < 5) {
            percentageFull = 5;
        }
        increSize = full * 0.01 * percentageFull;
        return increSize; 
    }

}
