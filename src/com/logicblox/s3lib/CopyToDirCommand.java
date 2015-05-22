package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CopyToDirCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private CloudStoreClient _client;

  public CopyToDirCommand(ListeningExecutorService httpExecutor,
                          ListeningScheduledExecutorService internalExecutor,
                          CloudStoreClient client)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _client = client;
  }

  public ListenableFuture<List<S3File>> run(final CopyOptions options)
      throws ExecutionException, InterruptedException, IOException, URISyntaxException {
   if (!options.getDestinationKey().endsWith("/") && !options.getDestinationKey().equals(""))
     throw new UsageException("Destination directory key should end with a '/'");

   List<S3ObjectSummary> lst = _client.listObjects(
       options.getSourceBucketName(),
       options.getSourceKey(),
       options.isRecursive()).get();

   String baseDirPath = "";
   if (options.getSourceKey().length() > 0)
   {
     int endIndex = options.getSourceKey().lastIndexOf("/");
     if (endIndex != -1)
       baseDirPath = options.getSourceKey().substring(0, endIndex+1);
   }

   List<ListenableFuture<S3File>> files = new ArrayList<>();
   for (S3ObjectSummary obj : lst)
     if (!obj.getKey().endsWith("/"))
     {
       // TODO(geokollias): Handle empty files appropriately.
       String destKeyLastPart = obj.getKey().substring(baseDirPath.length());
       String destKey = options.getDestinationKey() + destKeyLastPart;
       CopyOptions options0 = new CopyOptionsBuilder()
           .setSourceBucketName(options.getSourceBucketName())
           .setSourceKey(obj.getKey())
           .setDestinationBucketName(options.getDestinationBucketName())
           .setDestinationKey(destKey)
           .setCannedAcl(options.getCannedAcl().orNull())
           .createCopyOptions();

       files.add(_client.copy(options0));
     }

   return Futures.allAsList(files);
  }
}