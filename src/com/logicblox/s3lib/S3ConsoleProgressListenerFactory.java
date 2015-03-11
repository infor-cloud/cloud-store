package com.logicblox.s3lib;


import com.amazonaws.event.ProgressListener;

public class S3ConsoleProgressListenerFactory
    implements S3ProgressListenerFactory {
    public ProgressListener create(String name,
                                   String operation,
                                   long intervalInBytes,
                                   long totalSizeInBytes) {
        return new ConsoleProgressListener.S3ConsoleProgressListener(name,
            operation, intervalInBytes, totalSizeInBytes);
    }
}
