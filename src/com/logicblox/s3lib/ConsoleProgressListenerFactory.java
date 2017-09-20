package com.logicblox.s3lib;

public class ConsoleProgressListenerFactory
    implements OverallProgressListenerFactory {

    private long intervalInBytes = -1;

    private long getDefaultIntervalInBytes(long totalSizeInBytes) {
        long mb = 1024 * 1024;
        return (totalSizeInBytes >= 50 * mb) ? 10 * mb : mb;
    }

    public ConsoleProgressListenerFactory setIntervalInBytes(long intervalInBytes) {
        this.intervalInBytes = intervalInBytes;
        return this;
    }

    public OverallProgressListener create(ProgressOptions progressOptions) {
        if (intervalInBytes <= 0)
            intervalInBytes = getDefaultIntervalInBytes(
              progressOptions.getFileSizeInBytes());
        return new ConsoleProgressListener(progressOptions, intervalInBytes);
    }
}
