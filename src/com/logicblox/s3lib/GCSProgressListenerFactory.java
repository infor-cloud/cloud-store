package com.logicblox.s3lib;


import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;

public interface GCSProgressListenerFactory {
    MediaHttpUploaderProgressListener create(String name,
                                             String operation,
                                             long intervalInBytes,
                                             long totalSizeInBytes);
}
