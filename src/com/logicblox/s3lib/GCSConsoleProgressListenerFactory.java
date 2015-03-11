package com.logicblox.s3lib;


import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;

public class GCSConsoleProgressListenerFactory
    implements GCSProgressListenerFactory {
    public MediaHttpUploaderProgressListener create(String name,
                                                    String operation,
                                                    long intervalInBytes,
                                                    long totalSizeInBytes) {
        return new ConsoleProgressListener.GCSConsoleProgressListener(name,
            operation, intervalInBytes, totalSizeInBytes);
    }
}
