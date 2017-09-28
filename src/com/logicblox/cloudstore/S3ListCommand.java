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

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

class S3ListCommand
  extends Command
{

  private ListOptions _options;

  public S3ListCommand(ListOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<List<StoreFile>> run()
  {
    ListenableFuture<List<StoreFile>> future = executeWithRetry(_client.getInternalExecutor(),
      new Callable<ListenableFuture<List<StoreFile>>>()
      {
        public ListenableFuture<List<StoreFile>> call()
        {
          return runActual();
        }

        public String toString()
        {
          return "listing objects and directories for " +
            getUri(_options.getBucketName(), _options.getObjectKey().orElse(""));
        }
      });

    return future;
  }

  private ListenableFuture<List<StoreFile>> runActual()
  {
    return _client.getApiExecutor().submit(new Callable<List<StoreFile>>()
    {

      public List<StoreFile> call()
      {
        ListObjectsRequest req = new ListObjectsRequest().withBucketName(_options.getBucketName())
          .withPrefix(_options.getObjectKey().orElse(null));
        if(!_options.isRecursive())
        {
          req.setDelimiter("/");
        }

        List<StoreFile> all = new ArrayList<StoreFile>();
        ObjectListing current = getS3Client().listObjects(req);
        appendS3ObjectSummaryList(all, current.getObjectSummaries());
        if(!_options.dirsExcluded())
        {
          appendS3DirStringList(all, current.getCommonPrefixes(), _options.getBucketName());
        }
        current = getS3Client().listNextBatchOfObjects(current);

        while(current.isTruncated())
        {
          appendS3ObjectSummaryList(all, current.getObjectSummaries());
          if(!_options.dirsExcluded())
          {
            appendS3DirStringList(all, current.getCommonPrefixes(), _options.getBucketName());
          }
          current = getS3Client().listNextBatchOfObjects(current);
        }
        appendS3ObjectSummaryList(all, current.getObjectSummaries());
        if(!_options.dirsExcluded())
        {
          appendS3DirStringList(all, current.getCommonPrefixes(), _options.getBucketName());
        }

        return all;
      }
    });
  }

  private List<StoreFile> appendS3ObjectSummaryList(
    List<StoreFile> all, List<S3ObjectSummary> appendList)
  {
    for(S3ObjectSummary o : appendList)
    {
      all.add(S3ObjectSummaryToStoreFile(o));
    }

    return all;
  }

  private List<StoreFile> appendS3DirStringList(
    List<StoreFile> all, List<String> appendList, String bucket)
  {
    for(String o : appendList)
    {
      all.add(S3DirStringToStoreFile(o, bucket));
    }

    return all;
  }

  private StoreFile S3ObjectSummaryToStoreFile(S3ObjectSummary o)
  {
    StoreFile of = new StoreFile();
    of.setKey(o.getKey());
    of.setETag(o.getETag());
    of.setBucketName(o.getBucketName());
    of.setSize(o.getSize());
    return of;
  }

  private StoreFile S3DirStringToStoreFile(String dir, String bucket)
  {
    StoreFile df = new StoreFile();
    df.setKey(dir);
    df.setBucketName(bucket);
    df.setSize(new Long(0));

    return df;
  }
}
