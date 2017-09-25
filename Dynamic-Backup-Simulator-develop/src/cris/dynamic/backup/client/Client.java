package cris.dynamic.backup.client;

import java.util.ArrayList;
import java.util.Random;

public class Client {

    private final String            name;
    private double                  throughput;
    private double                  throughputVariance;
    private final ArrayList<String> backups;
    private final ArrayList<String> restores;

    public Client(final String name, final double throughput, final double throughputVariance) {
        this.name = name;
        this.throughput = throughput;
        if (throughputVariance >= 1) {
            throw new RuntimeException("Throughput Variance must be less than 100%");
        }
        this.throughputVariance = throughputVariance;
        backups = new ArrayList<String>();
        restores = new ArrayList<String>();
    }

    /**
     * Adds an associated backup to the client.
     * 
     * @param backupName
     */
    public void addBackup(final String backupName) {
        backups.add(backupName);
    }

    /**
     * Calculate the daily throughput of the client.
     * 
     * @return
     */
    public double computeThroughput() {
        final Random random = new Random();
        final double toReturn = throughput - (throughputVariance * throughput) +
                (random.nextDouble() * 2 * throughputVariance * throughput);
        return toReturn;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the throughput
     */
    public double getThroughput() {
        return throughput;
    }

    /**
     * @return the throughputVariance
     */
    public double getThroughputVariance() {
        return throughputVariance;
    }

    /**
     * @param throughput
     *            the throughput to set
     */
    public void setThroughput(double throughput) {
        this.throughput = throughput;
    }

    /**
     * @param throughputVariance
     *            the throughputVariance to set
     */
    public void setThroughputVariance(double throughputVariance) {
        this.throughputVariance = throughputVariance;
    }
    
	public void addRestore(String restoreName) {
		this.restores.add(restoreName);
	}

	public ArrayList<String> getActiveRestores() {
		return this.restores;
	}

}
