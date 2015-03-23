package com.logicblox.s3lib;

public class ProgressOptions {
    private final String objectUri;
    private final String operation;
    private final long fileSizeInBytes;

    ProgressOptions(String objectUri, String operation, long fileSizeInBytes) {
        this.objectUri = objectUri;
        this.operation = operation;
        this.fileSizeInBytes = fileSizeInBytes;
    }

    public String getObjectUri() {
        return objectUri;
    }

    public String getOperation() {
        return operation;
    }

    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }
}
