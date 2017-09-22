/*
  Copyright 2017, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.cloudstore;

import java.util.Map;

/**
 * {@code CopyOptionsBuilder} is a builder for {@code CopyOptions} objects.
 * <p>
 * Setting {@code sourceBucketName}, {@code sourceObjectKey}, {@code
 * destinationBucketName} and {@code destinationObjectKey} is mandatory. All the
 * others are optional.
 */
public class CopyOptionsBuilder extends CommandOptionsBuilder {
    private String sourceBucketName;
    private String sourceObjectKey;
    private String destinationBucketName;
    private String destinationObjectKey;
    private String storageClass;
    private boolean recursive = false;
    private String cannedAcl;
    private Map<String,String> userMetadata;
    private boolean dryRun = false;
    private boolean ignoreAbortInjection = false;
    private OverallProgressListenerFactory overallProgressListenerFactory;

    CopyOptionsBuilder(CloudStoreClient client) {
        _cloudStoreClient = client;
    }

    public CopyOptionsBuilder setSourceBucketName(String sourceBucketName) {
        this.sourceBucketName = sourceBucketName;
        return this;
    }

    public CopyOptionsBuilder setSourceObjectKey(String sourceObjectKey) {
        this.sourceObjectKey = sourceObjectKey;
        return this;
    }

    public CopyOptionsBuilder setDestinationBucketName(String
                                                           destinationBucketName) {
        this.destinationBucketName = destinationBucketName;
        return this;
    }

    public CopyOptionsBuilder setDestinationObjectKey(String destinationObjectKey) {
        this.destinationObjectKey = destinationObjectKey;
        return this;
    }

    public CopyOptionsBuilder setCannedAcl(String cannedAcl) {
        this.cannedAcl = cannedAcl;
        return this;
    }

    public CopyOptionsBuilder setUserMetadata(Map<String,String> userMetadata) {
        this.userMetadata = userMetadata;
        return this;
    }

    public CopyOptionsBuilder setStorageClass(String storageClass) {
        this.storageClass = storageClass;
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

    public CopyOptionsBuilder setIgnoreAbortInjection(boolean ignore)
    {
      this.ignoreAbortInjection = ignore;
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

    private void validateOptions()
    {
        if (_cloudStoreClient == null) {
            throw new UsageException("CloudStoreClient has to be set");
        }
        else if (sourceBucketName == null) {
            throw new UsageException("Source bucket name has to be set");
        }
        else if (sourceObjectKey == null) {
            throw new UsageException("Source object key has to be set");
        }
        else if (destinationBucketName == null) {
            throw new UsageException("Destination bucket name key has to be set");
        }
        else if (destinationObjectKey == null) {
            throw new UsageException("Destination object key has to be set");
        }

        if (cannedAcl != null) {
            if (!_cloudStoreClient.getAclHandler().isCannedAclValid(cannedAcl)) {
                throw new UsageException("Invalid canned ACL '" + cannedAcl + "'");
            }
        }

        if (storageClass != null) {
            if (!_cloudStoreClient.getStorageClassHandler().isStorageClassValid(storageClass)) {
                throw new UsageException("Invalid storage class '" + storageClass + "'");
            }
        }
    }

    @Override
    public CopyOptions createOptions() {
        validateOptions();

        return new CopyOptions(_cloudStoreClient, sourceBucketName, sourceObjectKey,
            destinationBucketName, destinationObjectKey, cannedAcl, storageClass,
            recursive, dryRun, ignoreAbortInjection, userMetadata, overallProgressListenerFactory);
    }
}