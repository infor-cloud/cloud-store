package com.logicblox.s3lib;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.concurrent.Callable;


public class AbortPendingUploadCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;

  public AbortPendingUploadCommand(
      ListeningExecutorService httpExecutor,
      ListeningScheduledExecutorService internalExecutor)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }

  /**
   * Aborts the pending upload to {@code bucket/key}, with the given {@code
   * uploadId}.
   *
   * @param bucket   The name of the bucket
   * @param key      The object key that the pending upload targets to
   * @param uploadId The id of the pending upload. Such ids can be found via
   *                 {@link ListPendingUploadsCommand}.
   * @see ListPendingUploadsCommand
   * @see AbortOldPendingUploadsCommand
   */
  public ListenableFuture<Void> run(final String bucket,
                                    final String key,
                                    final String uploadId)
  {
    ListenableFuture<Void> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<Void>>()
        {
          public ListenableFuture<Void> call()
          {
            Upload u = new MultipartAmazonUpload(
                getAmazonS3Client(),
                bucket,
                key,
                uploadId,
                null,
                _httpExecutor);
            return u.abort();
          }

          public String toString()
          {
            return "abort pending upload of " + getUri(bucket, key) + ", with" +
                " id " + uploadId;
          }
        });

    return future;
  }
}
