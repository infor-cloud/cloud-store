package com.logicblox.s3lib;

import java.io.File;

/**
 * {@code DownloadOptionsBuilder} is a builder for {@code DownloadOptions}
 * objects.
 * <p>
 * Setting fields {@code file}, {@code bucket} and {@code objectKey} is
 * mandatory. All the others are optional.
 */
public class DownloadOptionsBuilder {
    private CloudStoreClient cloudStoreClient;
    private File file;
    private String bucket;
    private String objectKey;
    private String version;
    private boolean recursive = false;
    private boolean overwrite = false;
    private boolean dryRun = false;
    private OverallProgressListenerFactory overallProgressListenerFactory;

    public DownloadOptionsBuilder setCloudStoreClient(CloudStoreClient client) {
        this.cloudStoreClient = client;
        return this;
    }

    public DownloadOptionsBuilder setFile(File file) {
        this.file = file;
        return this;
    }

    public DownloadOptionsBuilder setBucketName(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public DownloadOptionsBuilder setObjectKey(String objectKey) {
        this.objectKey = objectKey;
        return this;
    }

    public DownloadOptionsBuilder setRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }
    
    public DownloadOptionsBuilder setVersion(String version) {
        this.version = version;
        return this;
    }

    public DownloadOptionsBuilder setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public DownloadOptionsBuilder setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    public DownloadOptionsBuilder setOverallProgressListenerFactory
        (OverallProgressListenerFactory overallProgressListenerFactory) {
        this.overallProgressListenerFactory = overallProgressListenerFactory;
        return this;
    }

    private void validateOptions()
    {
        if (cloudStoreClient == null) {
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

    public DownloadOptions createDownloadOptions() {
        validateOptions();

        return new DownloadOptions(cloudStoreClient, file, bucket, objectKey,
          version, recursive, overwrite, dryRun, overallProgressListenerFactory);
    }
}
