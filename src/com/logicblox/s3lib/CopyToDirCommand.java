package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
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
  private boolean _dryRun;

  public CopyToDirCommand(ListeningExecutorService httpExecutor,
                          ListeningScheduledExecutorService internalExecutor,
			  boolean dryRun,
                          CloudStoreClient client)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
    _client = client;
    _dryRun = dryRun;
  }

  public ListenableFuture<List<S3File>> run(final CopyOptions options)
      throws ExecutionException, InterruptedException, IOException, URISyntaxException {
    if (!options.getDestinationKey().endsWith("/") && !options.getDestinationKey().equals(""))
      throw new UsageException("Destination directory key should end with a '/'");

    String baseDirPath = "";
    if (options.getSourceKey().length() > 0)
    {
      int endIndex = options.getSourceKey().lastIndexOf("/");
      if (endIndex != -1)
        baseDirPath = options.getSourceKey().substring(0, endIndex+1);
    }

    List<ListenableFuture<S3File>> files = new ArrayList<>();

    ListObjectsRequest req = new ListObjectsRequest()
      .withBucketName(options.getSourceBucketName())
      .withPrefix(options.getSourceKey());
    if (!options.isRecursive()) req.setDelimiter("/");

    ObjectListing current = getAmazonS3Client().listObjects(req);
    files.addAll(copyBatch(current.getObjectSummaries(), options, baseDirPath));
    current = getAmazonS3Client().listNextBatchOfObjects(current);

    while (current.isTruncated())
    {
      files.addAll(copyBatch(current.getObjectSummaries(), options, baseDirPath));
      current = getAmazonS3Client().listNextBatchOfObjects(current);
    }
    files.addAll(copyBatch(current.getObjectSummaries(), options, baseDirPath));

    if(_dryRun)
    {
      List<S3File> dummy = new ArrayList<S3File>();
      return Futures.immediateFuture(dummy);
    }
    else
    {
      return Futures.allAsList(files);
    }
  }

  private List<ListenableFuture<S3File>> copyBatch(List<S3ObjectSummary> lst,
                                                   CopyOptions options,
                                                   String baseDirPath)
    throws IOException
  {
    List<ListenableFuture<S3File>> batch = new ArrayList<>();

    for (S3ObjectSummary obj : lst)
    {
      if (!obj.getKey().endsWith("/"))
      {
        String destKeyLastPart = obj.getKey().substring(baseDirPath.length());
        String destKey = options.getDestinationKey() + destKeyLastPart;
        CopyOptions options0 = new CopyOptionsBuilder()
            .setSourceBucketName(options.getSourceBucketName())
            .setSourceKey(obj.getKey())
            .setDestinationBucketName(options.getDestinationBucketName())
            .setDestinationKey(destKey)
            .setCannedAcl(options.getCannedAcl().orNull())
            .createCopyOptions();

	if(_dryRun)
	{
          System.out.println("<DRYRUN> copying '" + getUri(options.getSourceBucketName(), obj.getKey())
            + "' to '" + getUri(options.getDestinationBucketName(), destKey) + "'");
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
