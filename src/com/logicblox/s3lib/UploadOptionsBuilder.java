package com.logicblox.s3lib;

import com.google.common.base.Optional;

import java.io.File;


/**
 * {@code UploadOptionsBuilder} is a builder for {@code UploadOptions} objects.
 * <p>
 * Setting fields {@code file}, {@code bucket} and {@code objectKey} is
 * mandatory. All the others are optional.
 */
public class UploadOptionsBuilder extends CommandOptionsBuilder {
    private File file;
    private String bucket;
    private String objectKey;
    private long chunkSize = -1;
    private Optional<String> encKey = Optional.absent();
    private Optional<String> acl = Optional.absent();
    private Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory = Optional.absent();
    private boolean dryRun = false;
    private boolean ignoreAbortInjection = false;

    UploadOptionsBuilder(CloudStoreClient client) {
        _cloudStoreClient = client;
     }

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

    public UploadOptionsBuilder setIgnoreAbortInjection(boolean ignore) {
        this.ignoreAbortInjection = ignore;
        return this;
    }

  @Override
    public UploadOptions createOptions() {
        // TODO: Check that all mandatory fields have been set. All the rest
        // should return Optional in the corresponding Options method.
        return new UploadOptions(_cloudStoreClient, file, bucket, objectKey,
          chunkSize, encKey, acl, dryRun, ignoreAbortInjection,
          overallProgressListenerFactory);
    }
}
