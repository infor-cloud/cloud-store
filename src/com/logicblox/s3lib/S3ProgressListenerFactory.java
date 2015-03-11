package com.logicblox.s3lib;


import com.amazonaws.event.ProgressListener;

public interface S3ProgressListenerFactory {
    ProgressListener create(String name,
                            String operation,
                            long intervalInBytes,
                            long totalSizeInBytes);
}
