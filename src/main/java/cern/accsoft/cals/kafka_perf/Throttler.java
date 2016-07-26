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

        while(shouldPause()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean shouldPause() {
        return ((double) sentMessages)*1000/(System.currentTimeMillis() - startTime) > messagesPerSecond;
    }
}
