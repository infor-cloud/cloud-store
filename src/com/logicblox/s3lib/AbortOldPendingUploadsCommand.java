package com.logicblox.s3lib;

import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.Date;
import java.util.concurrent.Callable;


public class AbortOldPendingUploadsCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;

  public AbortOldPendingUploadsCommand(
      ListeningExecutorService httpExecutor,
      ListeningScheduledExecutorService internalExecutor)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }

  /**
   * Aborts {@code bucket}'s pending uploads that were initiated before {@code
   * date}.
   *
   * @param bucket The name of the bucket
   * @param date   The date indicating which multipart uploads should be
   *               aborted. All pending uploads initiated before this date
   *               will be aborted.
   * @see ListPendingUploadsCommand
   * @see AbortPendingUploadCommand
   */
  public ListenableFuture<Void> run(final String bucket,
                                    final Date date)
  {
    ListenableFuture<Void> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<Void>>()
        {
          public ListenableFuture<Void> call()
          {
            return _httpExecutor.submit(new AbortCallable(bucket, date));
          }

          public String toString()
          {
            return "abort old pending uploads of " + bucket;
          }
        });

    return future;
  }

  private class AbortCallable implements Callable<Void>
  {
    private String bucket;
    private Date date;

    public AbortCallable(String bucket, Date date)
    {
      this.bucket = bucket;
      this.date = date;
    }

    public Void call() throws Exception {
      TransferManager tm = new TransferManager(getAmazonS3Client());
      tm.abortMultipartUploads(bucket, date);

      return null;
    }
  }
}
