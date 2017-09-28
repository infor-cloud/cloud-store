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

import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

class S3ListPendingUploadsCommand
  extends Command
{
  private PendingUploadsOptions _options;

  public S3ListPendingUploadsCommand(PendingUploadsOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<List<Upload>> run()
  {
    ListenableFuture<List<Upload>> future = executeWithRetry(_client.getInternalExecutor(),
      new Callable<ListenableFuture<List<Upload>>>()
      {
        public ListenableFuture<List<Upload>> call()
        {
          return runActual();
        }

        public String toString()
        {
          return "list pending uploads of " +
            getUri(_options.getBucketName(), _options.getObjectKey());
        }
      });

    return future;
  }

  private ListenableFuture<List<Upload>> runActual()
  {
    return _client.getApiExecutor().submit(new Callable<List<Upload>>()
    {
      public List<Upload> call()
      {
        ListMultipartUploadsRequest listMultipartUploadsRequest = new ListMultipartUploadsRequest(
          _options.getBucketName());

        listMultipartUploadsRequest.setPrefix(_options.getObjectKey());

        MultipartUploadListing multipartUploadListing = getS3Client().listMultipartUploads(
          listMultipartUploadsRequest);
        List<Upload> uploadsList = new ArrayList<Upload>();

        while(multipartUploadListing.isTruncated())
        {
          appendMultipartUploadList(uploadsList, multipartUploadListing.getMultipartUploads(),
            _options.getBucketName());
          listMultipartUploadsRequest.setKeyMarker(multipartUploadListing.getNextKeyMarker());
          multipartUploadListing = getS3Client().listMultipartUploads(listMultipartUploadsRequest);
        }
        appendMultipartUploadList(uploadsList, multipartUploadListing.getMultipartUploads(),
          _options.getBucketName());

        return uploadsList;
      }
    });
  }

  private List<Upload> appendMultipartUploadList(
    List<Upload> mailList, List<MultipartUpload> appendList, String bucket)
  {
    for(MultipartUpload u : appendList)
      mailList.add(S3MultipartUploadToUpload(u, bucket));

    return mailList;
  }

  private Upload S3MultipartUploadToUpload(MultipartUpload multipartUpload, String bucket)
  {
    Upload u = new S3MultipartUpload(getS3Client(), bucket, multipartUpload.getKey(),
      multipartUpload.getUploadId(), multipartUpload.getInitiated(), _client.getApiExecutor(),
      _client.getOptionsBuilderFactory().newUploadOptionsBuilder().createOptions());

    return u;
  }
}
