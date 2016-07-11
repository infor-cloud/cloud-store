package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.AccessControlList;
import com.google.common.base.Optional;

import java.util.Map;

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
    private final Optional<String> cannedAcl;
    private final Optional<AccessControlList> s3Acl;
    private final Optional<Map<String,String>> userMetadata;
    private final Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory;

    CopyOptions(String sourceBucketName,
                String sourceKey,
                String destinationBucketName,
                String destinationKey,
                Optional<String> cannedAcl,
                Optional<AccessControlList> s3Acl,
                boolean recursive,
                Optional<Map<String,String>> userMetadata,
                Optional<OverallProgressListenerFactory>
                    overallProgressListenerFactory) {
        this.sourceBucketName = sourceBucketName;
        this.sourceKey = sourceKey;
        this.destinationBucketName = destinationBucketName;
        this.destinationKey = destinationKey;
        this.recursive = recursive;
        this.cannedAcl = cannedAcl;
        this.s3Acl = s3Acl;
        this.userMetadata = userMetadata;
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

    public Optional<AccessControlList> getS3Acl() {
        return s3Acl;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public Optional<Map<String,String>> getUserMetadata() {
        return userMetadata;
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return overallProgressListenerFactory;
    }
}
