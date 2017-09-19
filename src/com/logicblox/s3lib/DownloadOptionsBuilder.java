package com.logicblox.s3lib;

import com.google.common.base.Optional;

import java.io.File;

/**
 * {@code DownloadOptionsBuilder} is a builder for {@code DownloadOptions}
 * objects.
 * <p>
 * Setting fields {@code file}, {@code bucket} and {@code objectKey} is
 * mandatory. All the others are optional.
 */
public class DownloadOptionsBuilder extends CommandOptionsBuilder {
    private File file;
    private String bucket;
    private String objectKey;
    private String version;
    private boolean recursive = false;
    private boolean overwrite = false;
    private boolean dryRun = false;
    private Optional<OverallProgressListenerFactory> overallProgressListenerFactory =
        Optional.absent();

    DownloadOptionsBuilder(CloudStoreClient client) {
        _cloudStoreClient = client;
    }

    public DownloadOptionsBuilder setFile(File file) {
        this.file = file;
        return this;
    }

    public DownloadOptionsBuilder setBucket(String bucket) {
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
        this.overallProgressListenerFactory = Optional.fromNullable
            (overallProgressListenerFactory);
        return this;
    }

    @Override
    public DownloadOptions createOptions() {
        return new DownloadOptions(_cloudStoreClient, file, bucket, objectKey,
          version, recursive, overwrite, dryRun, overallProgressListenerFactory);
    }
}
