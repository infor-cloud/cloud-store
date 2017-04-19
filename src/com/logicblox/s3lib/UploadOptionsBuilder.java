package com.logicblox.s3lib;

import com.google.common.base.Optional;

import java.io.File;
import java.net.URI;


/**
 * {@code UploadOptionsBuilder} is a builder for {@code UploadOptions} objects.
 * <p>
 * Setting fields {@code file}, {@code bucket} and {@code objectKey} is
 * mandatory. All the others are optional.
 */
public class UploadOptionsBuilder {
    private File file;
    private String bucket;
    private String objectKey;
    private long chunkSize = -1;
    private Optional<String> encKey = Optional.absent();
    private Optional<String> acl = Optional.absent();
    private Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory = Optional.absent();
    private boolean dryRun = false;

    public UploadOptionsBuilder setFile(File file) {
        this.file = file;
        return this;
    }

    public UploadOptionsBuilder setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public UploadOptionsBuilder setObjectKey(String objectKey) {
        this.objectKey = objectKey;
        return this;
    }

    public UploadOptionsBuilder setChunkSize(long chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public UploadOptionsBuilder setUri(URI uri) {
        setBucket(Utils.getBucket(uri)).setObjectKey(Utils.getObjectKey(uri));
        return this;
    }

    public UploadOptionsBuilder setEncKey(String encKey) {
        this.encKey = Optional.fromNullable(encKey);
        return this;
    }

    public UploadOptionsBuilder setAcl(String acl) {
        this.acl = Optional.fromNullable(acl);
        return this;
    }

    public UploadOptionsBuilder setOverallProgressListenerFactory
        (OverallProgressListenerFactory overallProgressListenerFactory) {
        this.overallProgressListenerFactory = Optional.fromNullable
            (overallProgressListenerFactory);
        return this;
    }

    public UploadOptionsBuilder setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    public UploadOptions createUploadOptions() {
        return new UploadOptions(file, bucket, objectKey, chunkSize, encKey,
            acl, dryRun, overallProgressListenerFactory);
    }
}
