package cern.accsoft.cals.kafka_perf;

class Throttler {
    public static final long THROTTLING_DISABLED = -1;

    private final long messagesPerSecond;
    private long startTime = System.currentTimeMillis();
    private long sentMessages = 0;

    public Throttler(long messagesPerSecond) {
        this.messagesPerSecond = messagesPerSecond;
    }

    public void reset() {
        startTime = System.currentTimeMillis();
        sentMessages = 0;
    }

    public void messageSent() {
        sentMessages++;
    }

    public void pauseIfNeeded() {
        if(this.messagesPerSecond == THROTTLING_DISABLED) return;

        long currentTime = System.currentTimeMillis();
        boolean shouldPause = ((double) sentMessages)*1000/(currentTime - startTime) > messagesPerSecond;

        if(shouldPause) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
