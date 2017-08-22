package cris.dynamic.backup.infrastructure;

public class HistoricalDataPoint {

    private int    dayCreated;
    private int    expirationDay;
    private double throughput;
    private final double size;

    public HistoricalDataPoint(final int dayCreated, final int expirationDay, final double throughput, final double size) {
        this.dayCreated = dayCreated;
        this.expirationDay = expirationDay;
        this.throughput = throughput;
        this.size = size;
    }


    /**
     * @return the dayCreated
     */
    public int getDayCreated() {
        return dayCreated;
    }

    /**
     * @return the expirationDay
     */
    public int getExpirationDay() {
        return expirationDay;
    }

    /**
     * @return the size
     */
    public double getSize() {
        return size;
    }

    /**
     * @return the throughput
     */
    public double getThroughput() {
        return throughput;
    }

    public double getWeight(final int currentDay) {
        final int days = currentDay - dayCreated;
        return Math.exp(-(days - 1) / 10);
    }

    public double getWeightedSize(final int currentDay) {
        return getWeight(currentDay) * size;
    }

    public double getWeightedThroughput(final int currentDay) {
        return getWeight(currentDay) * throughput;
    }

    public boolean isExpired(final int currentDay) {
        return currentDay > expirationDay;
    }

    /**
     * @param dayCreated
     *            the dayCreated to set
     */
    public void setDayCreated(int dayCreated) {
        this.dayCreated = dayCreated;
    }

    /**
     * @param expirationDay
     *            the expirationDay to set
     */
    public void setExpirationDay(int expirationDay) {
        this.expirationDay = expirationDay;
    }

    /**
     * @param throughput
     *            the throughput to set
     */
    public void setThroughput(double throughput) {
        this.throughput = throughput;
    }

}
