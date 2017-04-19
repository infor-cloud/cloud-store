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
    private final boolean dryRun;
    private final Optional<String> cannedAcl;
    private final Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory;

    CopyOptions(String sourceBucketName,
                String sourceKey,
                String destinationBucketName,
                String destinationKey,
                Optional<String> cannedAcl,
                boolean recursive,
		boolean dryRun,
                Optional<OverallProgressListenerFactory>
                    overallProgressListenerFactory) {
        this.sourceBucketName = sourceBucketName;
        this.sourceKey = sourceKey;
        this.destinationBucketName = destinationBucketName;
        this.destinationKey = destinationKey;
        this.recursive = recursive;
        this.cannedAcl = cannedAcl;
	this.dryRun = dryRun;
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

    public boolean isRecursive() {
        return recursive;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return overallProgressListenerFactory;
    }
}
