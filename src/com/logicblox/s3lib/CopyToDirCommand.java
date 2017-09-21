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
  private boolean _dryRun;

  public CopyToDirCommand(CopyOptions options)
  {
    super(options);
    _options = options;
    _dryRun = _options.isDryRun();
  }

  public ListenableFuture<List<StoreFile>> run()
      throws ExecutionException, InterruptedException, IOException {
    if (!_options.getDestinationObjectKey().endsWith("/") && !_options.getDestinationObjectKey().equals(""))
      throw new UsageException("Destination directory key should end with a '/'");

    String baseDirPath = "";
    if (_options.getSourceObjectKey().length() > 0)
    {
      int endIndex = _options.getSourceObjectKey().lastIndexOf("/");
      if (endIndex != -1)
        baseDirPath = _options.getSourceObjectKey().substring(0, endIndex + 1);
    }

    List<ListenableFuture<StoreFile>> files = new ArrayList<>();

    ListObjectsRequest req = new ListObjectsRequest()
      .withBucketName(_options.getSourceBucketName())
      .withPrefix(_options.getSourceObjectKey());
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

  private List<ListenableFuture<StoreFile>> copyBatch(List<S3ObjectSummary> lst,
                                                   String baseDirPath)
    throws IOException
  {
    List<ListenableFuture<StoreFile>> batch = new ArrayList<>();

    for (S3ObjectSummary obj : lst)
    {
      if (!obj.getKey().endsWith("/"))
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
