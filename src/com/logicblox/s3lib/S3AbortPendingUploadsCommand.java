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

package com.logicblox.s3lib;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


public class S3AbortPendingUploadsCommand extends Command
{
  private PendingUploadsOptions _options;

  public S3AbortPendingUploadsCommand(PendingUploadsOptions options)
  {
    super(options);
    _options = options;
  }

  public ListenableFuture<List<Void>> run()
  {
    ListenableFuture<List<Void>> future;
    if (_options.getUploadId().isPresent() && _options.getDate().isPresent())
    {
      ListenableFuture<Void> f = executeWithRetry(_client.getInternalExecutor(),
        new AbortByIdDate(_options.getUploadId().get(), _options.getDate().get()));

      List<ListenableFuture<Void>> aborts = new ArrayList<>(Arrays.asList(f));
      future = Futures.allAsList(aborts);
    }
    else if (_options.getUploadId().isPresent())
    {
      ListenableFuture<Void> f = executeWithRetry(_client.getInternalExecutor(),
        new AbortById(_options.getUploadId().get()));

      List<ListenableFuture<Void>> aborts = new ArrayList<>(Arrays.asList(f));
      future = Futures.allAsList(aborts);
    }
    else if (_options.getDate().isPresent())
    {
      future = executeWithRetry(_client.getInternalExecutor(), new AbortByDate(_options.getDate().get()));
    }
    else
    {
      throw new UsageException("At least one of upload id or date should be " +
                               "defined");
    }

    return future;
  }

  private class AbortByIdDate implements Callable<ListenableFuture<Void>>
  {
    private String _uploadId;
    private Date _date;

    public AbortByIdDate(String uploadId, Date date)
    {
      _uploadId = uploadId;
      _date = date;
    }

    public ListenableFuture<Void> call()
    throws ExecutionException, InterruptedException
    {
      Upload u = new S3MultipartUpload(
        getS3Client(),
        _options.getBucketName(),
        _options.getObjectKey(),
        _uploadId,
        null,
        _client.getApiExecutor(),
        _client.getOptionsBuilderFactory().newUploadOptionsBuilder().createOptions());

      if (u.getInitiationDate().before(_date)) {
        return executeWithRetry(_client.getInternalExecutor(), new AbortById(u.getId()));
      }

      return Futures.immediateFuture(null);
    }

    public String toString()
    {
      return "aborting pending uploads of " + getUri(_options.getBucketName(),
        _options.getObjectKey()) + ", initiated before " + _date;
    }
  }

  private class AbortById implements Callable<ListenableFuture<Void>>
  {
    private String _uploadId;

    public AbortById(String uploadId)
    {
      _uploadId = uploadId;
    }

    public ListenableFuture<Void> call()
    {
      Upload u = new S3MultipartUpload(
        getS3Client(),
        _options.getBucketName(),
        _options.getObjectKey(),
        _uploadId,
        null,
        _client.getApiExecutor(),
        _client.getOptionsBuilderFactory().newUploadOptionsBuilder().createOptions());

      return u.abort();
    }

    public String toString()
    {
      return "aborting pending upload of " + getUri(_options.getBucketName(),
        _options.getObjectKey()) + ", with id " + _uploadId;
    }
  }

  private class AbortByDate implements Callable<ListenableFuture<List<Void>>>
  {
    private Date _date;

    public AbortByDate(Date date)
    {
      _date = date;
    }

    public ListenableFuture<List<Void>> call()
      throws ExecutionException, InterruptedException
    {
      // TODO(geokollias): It's a blocking call (similar case with DownloadDirectoryCommand)
      List<Upload> pendingUploads = _client.listPendingUploads(_options).get();

      List<ListenableFuture<Void>> aborts = new ArrayList<>();
      for (Upload obj : pendingUploads) {
        if (obj.getInitiationDate().before(_date)) {
          ListenableFuture<Void> abort = executeWithRetry(_client.getInternalExecutor(),
            new AbortById(obj.getId()));
          aborts.add(abort);
        }
      }

      return Futures.allAsList(aborts);
    }

    public String toString()
    {
      return "aborting pending uploads of " + getUri(_options.getBucketName(),
        _options.getObjectKey()) + ", initiated before " + _date;
    }
  }
}
