/*
  Copyright 2018, Infor Inc.

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

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

class S3CopyRecursivelyCommand
  extends Command
{
  private CopyOptions _options;
  private boolean _dryRun;

  public S3CopyRecursivelyCommand(CopyOptions options)
  {
    super(options);
    _options = options;
    _dryRun = _options.isDryRun();
  }

  public ListenableFuture<List<StoreFile>> run()
    throws ExecutionException, InterruptedException, IOException
  {
    if(!_options.getDestinationObjectKey().endsWith("/") &&
      !_options.getDestinationObjectKey().equals(""))
    {
      throw new UsageException("Destination directory key should end with a '/'");
    }

    String baseDirURI = Utils.getBaseDirURI(_options.getSourceObjectKey());
    List<ListenableFuture<StoreFile>> files = new ArrayList<>();

    ListObjectsRequest req = new ListObjectsRequest().withBucketName(_options.getSourceBucketName())
      .withPrefix(_options.getSourceObjectKey());

    ObjectListing current = getS3Client().listObjects(req);
    files.addAll(copyBatch(current.getObjectSummaries(), baseDirURI));
    current = getS3Client().listNextBatchOfObjects(current);

    while(current.isTruncated())
    {
      files.addAll(copyBatch(current.getObjectSummaries(), baseDirURI));
      current = getS3Client().listNextBatchOfObjects(current);
    }
    files.addAll(copyBatch(current.getObjectSummaries(), baseDirURI));

    if(_dryRun)
    {
      return Futures.immediateFuture(null);
    }
    else
    {
      return Futures.allAsList(files);
    }
  }

  private List<ListenableFuture<StoreFile>> copyBatch(List<S3ObjectSummary> lst, String baseDirPath)
    throws IOException
  {
    List<ListenableFuture<StoreFile>> batch = new ArrayList<>();

    for(S3ObjectSummary obj : lst)
    {
      if(!obj.getKey().endsWith("/"))
      {
        String destKeyLastPart = obj.getKey().substring(baseDirPath.length());
        String destKey = _options.getDestinationObjectKey() + destKeyLastPart;
        CopyOptions options0 = _client.getOptionsBuilderFactory()
          .newCopyOptionsBuilder()
          .setSourceBucketName(_options.getSourceBucketName())
          .setSourceObjectKey(obj.getKey())
          .setDestinationBucketName(_options.getDestinationBucketName())
          .setDestinationObjectKey(destKey)
          .setCannedAcl(_options.getCannedAcl().orElse(null))
          .setStorageClass(_options.getStorageClass().orElse(null))
          .createOptions();

        if(_dryRun)
        {
          System.out.println(
            "<DRYRUN> copying '" + getUri(_options.getSourceBucketName(), obj.getKey()) + "' to '" +
              getUri(_options.getDestinationBucketName(), destKey) + "'");
        }
        else
        {
          batch.add(_client.copy(options0));
        }
      }
    }

    return batch;
  }
}
