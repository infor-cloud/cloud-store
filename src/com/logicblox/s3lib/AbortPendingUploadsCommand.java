package com.logicblox.s3lib;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


public class AbortPendingUploadsCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private CloudStoreClient _csClient;
  private PendingUploadsOptions _options;

  public AbortPendingUploadsCommand(PendingUploadsOptions options)
  {
    _csClient = options.getCloudStoreClient();
    _httpExecutor = _csClient.getApiExecutor();
    _executor = _csClient.getInternalExecutor();
    _options = options;
  }

  public ListenableFuture<List<Void>> run()
  {
    ListenableFuture<List<Void>> future;
    if (_options.getUploadId().isPresent() && _options.getDate().isPresent())
    {
      ListenableFuture<Void> f = executeWithRetry(_executor,
        new AbortByIdDate(_options.getUploadId().get(), _options.getDate().get()));

      List<ListenableFuture<Void>> aborts = new ArrayList<>(Arrays.asList(f));
      future = Futures.allAsList(aborts);
    }
    else if (_options.getUploadId().isPresent())
    {
      ListenableFuture<Void> f = executeWithRetry(_executor,
        new AbortById(_options.getUploadId().get()));

      List<ListenableFuture<Void>> aborts = new ArrayList<>(Arrays.asList(f));
      future = Futures.allAsList(aborts);
    }
    else if (_options.getDate().isPresent())
    {
      future = executeWithRetry(_executor, new AbortByDate(_options.getDate().get()));
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
      Upload u = new MultipartAmazonUpload(
        getAmazonS3Client(),
        _options.getBucketName(),
        _options.getObjectKey(),
        _uploadId,
        null,
        _httpExecutor,
        (new UploadOptionsBuilder()).createUploadOptions());

      if (u.getInitiationDate().before(_date)) {
        return executeWithRetry(_executor, new AbortById(u.getId()));
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
      Upload u = new MultipartAmazonUpload(
        getAmazonS3Client(),
        _options.getBucketName(),
        _options.getObjectKey(),
        _uploadId,
        null,
        _httpExecutor,
        (new UploadOptionsBuilder()).createUploadOptions());

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
      List<Upload> pendingUploads = _csClient.listPendingUploads(_options).get();

      List<ListenableFuture<Void>> aborts = new ArrayList<>();
      for (Upload obj : pendingUploads) {
        if (obj.getInitiationDate().before(_date)) {
          ListenableFuture<Void> abort = executeWithRetry(_executor,
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
