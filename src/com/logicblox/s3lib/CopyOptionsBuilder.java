package com.logicblox.s3lib;

import com.google.common.base.Optional;

/**
 * {@code CopyOptionsBuilder} is a builder for {@code CopyOptions} objects.
 * <p>
 * Setting {@code sourceBucketName}, {@code sourceKey}, {@code
 * destinationBucketName} and {@code destinationKey} is mandatory. All the
 * others are optional.
 */
public class CopyOptionsBuilder {
    private String sourceBucketName;
    private String sourceKey;
    private String destinationBucketName;
    private String destinationKey;
    private Optional<String> cannedAcl = Optional.absent();
    private boolean recursive = false;
    private boolean dryRun = false;
    private Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory = Optional.absent();

    public CopyOptionsBuilder setSourceBucketName(String sourceBucketName) {
        this.sourceBucketName = sourceBucketName;
        return this;
    }

    public CopyOptionsBuilder setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
        return this;
    }

    public CopyOptionsBuilder setDestinationBucketName(String
                                                           destinationBucketName) {
        this.destinationBucketName = destinationBucketName;
        return this;
    }

    public CopyOptionsBuilder setDestinationKey(String destinationKey) {
        this.destinationKey = destinationKey;
        return this;
    }

    public CopyOptionsBuilder setCannedAcl(String cannedAcl) {
        this.cannedAcl = Optional.fromNullable(cannedAcl);
        return this;
    }

    public CopyOptionsBuilder setRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }

    public CopyOptionsBuilder setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

   // Disabled progress listener since AWS S3 copy progress indicator doesn't
    // notify about the copied bytes.
    //    public CopyOptionsBuilder setOverallProgressListenerFactory
    //        (OverallProgressListenerFactory overallProgressListenerFactory) {
    //        this.overallProgressListenerFactory = Optional.fromNullable
    //            (overallProgressListenerFactory);
    //        return this;
    //    }

    public CopyOptions createCopyOptions() {
        return new CopyOptions(sourceBucketName, sourceKey,
            destinationBucketName, destinationKey, cannedAcl, recursive,
            dryRun, overallProgressListenerFactory);
    }
}
