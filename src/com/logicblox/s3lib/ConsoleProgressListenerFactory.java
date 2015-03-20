package com.logicblox.s3lib;

public class ConsoleProgressListenerFactory
    implements OverallProgressListenerFactory {
    public OverallProgressListener create(String name,
                                   String operation,
                                   long intervalInBytes,
                                   long totalSizeInBytes) {
        return new ConsoleProgressListener(name, operation,
            intervalInBytes, totalSizeInBytes);
    }

    public OverallProgressListener create(String name,
                                   String operation,
                                   long totalSizeInBytes) {
        long mb = 1024 * 1024;
        long intervalInBytes = mb;
        if (totalSizeInBytes >= 50 * mb)
            intervalInBytes = 10 * mb;
        return create(name, operation, intervalInBytes, totalSizeInBytes);
    }
}
