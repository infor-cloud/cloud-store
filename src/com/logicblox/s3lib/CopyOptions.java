package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.AccessControlList;
import java.util.Map;
import java.util.Optional;

/**
 * {@code CopyOptions} contains all the details needed by the copy operation.
 * The specified {@code sourceObjectKey}, under {@code sourceBucketName} bucket, is
 * copied
 * to {@code destinationObjectKey}, under {@code destinationBucketName}.
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
public class CopyOptions extends CommandOptions {
    private final String sourceBucketName;
    private final String sourceObjectKey;
    private final String destinationBucketName;
    private final String destinationObjectKey;
    private final boolean recursive;
    private final boolean dryRun;
    private final boolean ignoreAbortInjection;
    private String cannedAcl;
    private final AccessControlList s3Acl;
    private final String storageClass;
    private final Map<String,String> userMetadata;
    private final OverallProgressListenerFactory
        overallProgressListenerFactory;

    // for testing injection of aborts during a copy
    private static AbortCounters _abortCounters = new AbortCounters();


    CopyOptions(CloudStoreClient cloudStoreClient,
                String sourceBucketName,
                String sourceObjectKey,
                String destinationBucketName,
                String destinationObjectKey,
                String cannedAcl,
                AccessControlList s3Acl,
                String storageClass,
                boolean recursive,
                boolean dryRun,
                boolean ignoreAbortInjection,
                Map<String,String> userMetadata,
                OverallProgressListenerFactory overallProgressListenerFactory) {
        super(cloudStoreClient);
        this.sourceBucketName = sourceBucketName;
        this.sourceObjectKey = sourceObjectKey;
        this.destinationBucketName = destinationBucketName;
        this.destinationObjectKey = destinationObjectKey;
        this.recursive = recursive;
        this.cannedAcl = cannedAcl;
        this.s3Acl = s3Acl;
        this.storageClass = storageClass;
        this.dryRun = dryRun;
        this.ignoreAbortInjection = ignoreAbortInjection;
        this.userMetadata = userMetadata;
        this.overallProgressListenerFactory = overallProgressListenerFactory;
    }

    // for testing injection of aborts during a copy
    void injectAbort(String id)
    {
      if(!this.ignoreAbortInjection
           && (_abortCounters.decrementInjectionCounter(id) > 0))
      {
        throw new AbortInjection("forcing copy abort");
      }
    }

    static AbortCounters getAbortCounters()
    {
      return _abortCounters;
    }

    public String getSourceBucketName() {
        return sourceBucketName;
    }

    public String getSourceObjectKey() {
        return sourceObjectKey;
    }

    public String getDestinationBucketName() {
        return destinationBucketName;
    }

    public String getDestinationObjectKey() {
        return destinationObjectKey;
    }

    public String getCannedAcl() {
        if (cannedAcl == null) {
            if (getCloudStoreClient().getScheme().equals("s3")) {
                cannedAcl = "bucket-owner-full-control";
            }
            else if (getCloudStoreClient().getScheme().equals("gs")) {
                cannedAcl = "projectPrivate";
            }
        }
        return cannedAcl;
    }

    public Optional<String> getStorageClass() {
        return Optional.ofNullable(storageClass);
    }

    public Optional<AccessControlList> getS3Acl() {
        return Optional.ofNullable(s3Acl);
    }

    public boolean isRecursive() {
        return recursive;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Optional<Map<String,String>> getUserMetadata() {
        return Optional.ofNullable(userMetadata);
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return Optional.ofNullable(overallProgressListenerFactory);
    }
}
