package com.logicblox.s3lib;

import com.google.common.base.Optional;

/**
 * {@code CopyOptions} contains all the details needed by the copy operation.
 * The specified {@code sourceKey}, under {@code sourceBucketName} bucket, is
 * copied
 * to {@code destinationKey}, under {@code destinationBucketName}.
 * <p>
 * If {@code cannedAcl} is specified then it's applied to the destination
 * object.
 * <p>
 * If progress listener factory has been set, then progress notifications will
 * be recorded.
 * <p>
 * {@code CopyOptions} objects are meant to be built by {@code
 * CopyOptionsBuilder}. This class provides only public getter methods.
 */
public class CopyOptions {
    private final String sourceBucketName;
    private final String sourceKey;
    private final String destinationBucketName;
    private final String destinationKey;
    private final boolean recursive;
    // TODO(geo): Revise use of Optionals. E.g. it's not a good idea to use them
    // as fields.
    private final Optional<String> cannedAcl;
    private final String storageClass;
    private final Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory;

    CopyOptions(String sourceBucketName,
                String sourceKey,
                String destinationBucketName,
                String destinationKey,
                Optional<String> cannedAcl,
                String storageClass,
                boolean recursive,
                Optional<OverallProgressListenerFactory>
                    overallProgressListenerFactory) {
        this.sourceBucketName = sourceBucketName;
        this.sourceKey = sourceKey;
        this.destinationBucketName = destinationBucketName;
        this.destinationKey = destinationKey;
        this.recursive = recursive;
        this.cannedAcl = cannedAcl;
        this.storageClass = storageClass;
        this.overallProgressListenerFactory = overallProgressListenerFactory;
    }

    public String getSourceBucketName() {
        return sourceBucketName;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public String getDestinationBucketName() {
        return destinationBucketName;
    }

    public String getDestinationKey() {
        return destinationKey;
    }

    public Optional<String> getCannedAcl() {
        return cannedAcl;
    }

    public Optional<String> getStorageClass() {
        return Optional.fromNullable(storageClass);
    }

    public boolean isRecursive() {
        return recursive;
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return overallProgressListenerFactory;
    }
}
