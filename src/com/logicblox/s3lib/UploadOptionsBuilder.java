package com.logicblox.s3lib;

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
    private String encKey;
    private String acl;
    private OverallProgressListenerFactory overallProgressListenerFactory;
    private boolean dryRun = false;
    private boolean ignoreAbortInjection = false;

    UploadOptionsBuilder(CloudStoreClient client) {
        _cloudStoreClient = client;
     }

    public UploadOptionsBuilder setFile(File file) {
        this.file = file;
        return this;
    }

    public UploadOptionsBuilder setBucketName(String bucket) {
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
        this.encKey = encKey;
        return this;
    }

    public UploadOptionsBuilder setAcl(String acl) {
        this.acl = acl;
        return this;
    }

    public UploadOptionsBuilder setOverallProgressListenerFactory
        (OverallProgressListenerFactory overallProgressListenerFactory) {
        this.overallProgressListenerFactory = overallProgressListenerFactory;
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

    private void validateOptions() {
        if (_cloudStoreClient == null) {
            throw new UsageException("CloudStoreClient has to be set");
        }
        else if (file == null) {
            throw new UsageException("File has to be set");
        }
        else if (bucket == null) {
            throw new UsageException("Bucket has to be set");
        }
        else if (objectKey == null) {
            throw new UsageException("Object key has to be set");
        }
    }

    @Override
    public UploadOptions createOptions() {
        validateOptions();

        return new UploadOptions(_cloudStoreClient, file, bucket, objectKey,
          chunkSize, encKey, acl, dryRun, ignoreAbortInjection,
          overallProgressListenerFactory);
    }
}
