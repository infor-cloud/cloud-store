package com.logicblox.s3lib;

import com.google.common.base.Optional;

public class ConsoleProgressListenerFactory
    implements OverallProgressListenerFactory {

    private Optional<Long> intervalInBytes = Optional.absent();

    private long getDefaultIntervalInBytes(long totalSizeInBytes) {
        long mb = 1024 * 1024;
        return (totalSizeInBytes >= 50 * mb) ? 10 * mb : mb;
    }

    public ConsoleProgressListenerFactory setIntervalInBytes(long intervalInBytes) {
        this.intervalInBytes.of(intervalInBytes);
        return this;
    }

    public OverallProgressListener create(ProgressOptions progressOptions) {
        if (!intervalInBytes.isPresent())
            intervalInBytes = Optional.of(
                getDefaultIntervalInBytes(
                    progressOptions.getFileSizeInBytes()));
        return new ConsoleProgressListener(progressOptions,
            intervalInBytes.get());
    }
}
