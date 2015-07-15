package com.logicblox.s3lib;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;


public class AbortOldPendingUploadsCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private CloudStoreClient _csClient;

  public AbortOldPendingUploadsCommand(
      ListeningExecutorService httpExecutor,
      ListeningScheduledExecutorService internalExecutor,
      CloudStoreClient csClient)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _csClient = csClient;
  }

  /**
   * Aborts pending uploads under {@code bucket/prefix} that were initiated
   * before {@code date}.
   *
   * @param bucket The name of the bucket
   * @param prefix The prefix of the keys we want to check
   * @param date   The date indicating which multipart uploads should be
   *               aborted. All pending uploads initiated before this date
   *               will be aborted.
   * @see ListPendingUploadsCommand
   * @see AbortPendingUploadCommand
   */
  public ListenableFuture<List<Void>> run(final String bucket,
                                          final String prefix,
                                          final Date date)
      throws ExecutionException, InterruptedException, URISyntaxException {
    // TODO(geokollias): It's a blocking call (similar case with DownloadDirectoryCommand)
    List<Upload> pendingUploads = _csClient.listPendingUploads(bucket, prefix).get();

    List<ListenableFuture<Void>> aborts = new ArrayList<ListenableFuture<Void>>();
    for (Upload obj : pendingUploads) {
      if (obj.getInitiationDate().before(date)) {
        ListenableFuture<Void> abort = _csClient.abortPendingUpload(
            obj.getBucket(), obj.getKey(), obj.getId());
        aborts.add(abort);
      }
    }

    return Futures.allAsList(aborts);
  }
}
