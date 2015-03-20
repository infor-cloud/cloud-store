package com.logicblox.s3lib;

public interface OverallProgressListenerFactory {
    OverallProgressListener create(String name,
                                   String operation,
                                   long intervalInBytes,
                                   long totalSizeInBytes);

    OverallProgressListener create(String name,
                                   String operation,
                                   long totalSizeInBytes);
}
