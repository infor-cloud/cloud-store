package com.logicblox.s3lib;

public class ProgressOptionsBuilder {
    private String objectUri;
    private String operation;
    private long fileSizeInBytes;

    public ProgressOptionsBuilder setObjectUri(String objectUri) {
        this.objectUri = objectUri;
        return this;
    }

    public ProgressOptionsBuilder setOperation(String operation) {
        this.operation = operation;
        return this;
    }

    public ProgressOptionsBuilder setFileSizeInBytes(long fileSizeInBytes) {
        this.fileSizeInBytes = fileSizeInBytes;
        return this;
    }

    public ProgressOptions createProgressOptions() {
        return new ProgressOptions(objectUri, operation, fileSizeInBytes);
    }
}