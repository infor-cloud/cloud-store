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
    private OverallProgressListenerFactory overallProgressListenerFactory;

    DownloadOptionsBuilder(CloudStoreClient client) {
        _cloudStoreClient = client;
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
    public DownloadOptions createOptions() {
        validateOptions();

        return new DownloadOptions(_cloudStoreClient, file, bucket, objectKey,
          version, recursive, overwrite, dryRun, overallProgressListenerFactory);
    }
}
