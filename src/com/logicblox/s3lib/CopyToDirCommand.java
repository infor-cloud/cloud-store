package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CopyToDirCommand extends Command
{
  private CopyOptions _options;
  private CloudStoreClient _client;
  private boolean _dryRun;

  public CopyToDirCommand(CopyOptions options)
  {
    _options = options;
    _client = _options.getCloudStoreClient();
    _dryRun = _options.isDryRun();
  }

  public ListenableFuture<List<S3File>> run()
      throws ExecutionException, InterruptedException, IOException {
    if (!_options.getDestinationKey().endsWith("/") && !_options.getDestinationKey().equals(""))
      throw new UsageException("Destination directory key should end with a '/'");

    String baseDirPath = "";
    if (_options.getSourceKey().length() > 0)
    {
      int endIndex = _options.getSourceKey().lastIndexOf("/");
      if (endIndex != -1)
        baseDirPath = _options.getSourceKey().substring(0, endIndex+1);
    }

    List<ListenableFuture<S3File>> files = new ArrayList<>();

    ListObjectsRequest req = new ListObjectsRequest()
      .withBucketName(_options.getSourceBucketName())
      .withPrefix(_options.getSourceKey());
    if (!_options.isRecursive()) req.setDelimiter("/");

    ObjectListing current = getAmazonS3Client().listObjects(req);
    files.addAll(copyBatch(current.getObjectSummaries(), baseDirPath));
    current = getAmazonS3Client().listNextBatchOfObjects(current);

    while (current.isTruncated())
    {
      files.addAll(copyBatch(current.getObjectSummaries(), baseDirPath));
      current = getAmazonS3Client().listNextBatchOfObjects(current);
    }
    files.addAll(copyBatch(current.getObjectSummaries(), baseDirPath));

    if(_dryRun)
    {
      return Futures.immediateFuture(null);
    }
    else
    {
      return Futures.allAsList(files);
    }
  }

  private List<ListenableFuture<S3File>> copyBatch(List<S3ObjectSummary> lst,
                                                   String baseDirPath)
    throws IOException
  {
    List<ListenableFuture<S3File>> batch = new ArrayList<>();

    for (S3ObjectSummary obj : lst)
    {
      if (!obj.getKey().endsWith("/"))
      {
        String destKeyLastPart = obj.getKey().substring(baseDirPath.length());
        String destKey = _options.getDestinationKey() + destKeyLastPart;
        CopyOptions options0 = new CopyOptionsBuilder()
            .setCloudStoreClient(_options.getCloudStoreClient())
            .setSourceBucketName(_options.getSourceBucketName())
            .setSourceKey(obj.getKey())
            .setDestinationBucketName(_options.getDestinationBucketName())
            .setDestinationKey(destKey)
            .setCannedAcl(_options.getCannedAcl().orNull())
            .setStorageClass(_options.getStorageClass().orNull())
            .createCopyOptions();

        if(_dryRun)
        {
          System.out.println("<DRYRUN> copying '" + getUri(_options.getSourceBucketName(), obj.getKey())
            + "' to '" + getUri(_options.getDestinationBucketName(), destKey) + "'");
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
