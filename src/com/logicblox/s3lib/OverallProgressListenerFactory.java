package com.logicblox.s3lib;

public interface OverallProgressListenerFactory {
    OverallProgressListener create(ProgressOptions progressOptions);
}
