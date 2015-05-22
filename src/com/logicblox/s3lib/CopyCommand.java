package com.logicblox.s3lib;


import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class CopyCommand extends Command
{
  private ListeningExecutorService _copyExecutor;
  private ListeningScheduledExecutorService _executor;
  private String acl;
  private Optional<OverallProgressListenerFactory> progressListenerFactory;

  public CopyCommand(
      ListeningExecutorService copyExecutor,
      ListeningScheduledExecutorService internalExecutor,
      String acl,
      OverallProgressListenerFactory progressListenerFactory)
  throws IOException
  {
    _copyExecutor = copyExecutor;
    _executor = internalExecutor;

    this.acl = acl;
    this.progressListenerFactory = Optional.fromNullable
        (progressListenerFactory);
  }

  public ListenableFuture<S3File> run(final String sourceBucketName,
                                      final String sourceKey,
                                      final String destinationBucketName,
                                      final String destinationKey)
  {
    ListenableFuture<Copy> copy = startCopy(sourceBucketName, sourceKey,
        destinationBucketName, destinationKey);
    copy = Futures.transform(copy, startPartsAsyncFunction());
    ListenableFuture<String> result = Futures.transform(copy,
        completeAsyncFunction());
    return Futures.transform(
        result,
        new Function<String, S3File>() {
          public S3File apply(String etag) {
            S3File f = new S3File();
            f.setLocalFile(null);
            f.setETag(etag);
            f.setBucketName(destinationBucketName);
            f.setKey(destinationKey);
            return f;
          }
        }
    );
  }

  /**
   * Step 1: Start copy and fetch metadata.
   */
  private ListenableFuture<Copy> startCopy(final String sourceBucketName,
                                           final String sourceKey,
                                           final String destinationBucketName,
                                           final String destinationKey)
  {
    return executeWithRetry(
      _executor,
      new Callable<ListenableFuture<Copy>>()
      {
        public ListenableFuture<Copy> call()
        {
          return startCopyActual(sourceBucketName, sourceKey,
              destinationBucketName, destinationKey);
        }

        public String toString()
        {
          return "starting copy of " + getUri(sourceBucketName, sourceKey) +
              " to " + getUri(destinationBucketName, destinationKey);
        }
      });
  }

  private ListenableFuture<Copy> startCopyActual(final String sourceBucketName,
                                                 final String sourceKey,
                                                 final String destinationBucketName,
                                                 final String destinationKey)
  {
    MultipartAmazonCopyFactory factory = new MultipartAmazonCopyFactory
        (getAmazonS3Client(), _copyExecutor);
    return factory.startCopy(sourceBucketName, sourceKey,
        destinationBucketName, destinationKey, acl);
  }

  /**
   * Step 2: Start copying parts
   */
  private AsyncFunction<Copy, Copy> startPartsAsyncFunction()
  {
    return new AsyncFunction<Copy, Copy>()
    {
      public ListenableFuture<Copy> apply(Copy copy) throws Exception
      {
        return startParts(copy);
      }
    };
  }

  private ListenableFuture<Copy> startParts(Copy copy)
  throws IOException, UsageException
  {
    Map<String,String> meta = copy.getMeta();

    String errPrefix = "Copy of " + getUri(copy.getSourceBucket(),
        copy.getSourceKey()) + " to " + getUri(copy.getDestinationBucket(),
        copy.getDestinationKey()) + ": ";
    if (meta.containsKey("s3tool-version"))
    {
      String objectVersion = meta.get("s3tool-version");

      if (!String.valueOf(Version.CURRENT).equals(objectVersion))
        throw new UsageException(
          errPrefix + "unsupported version: " +  objectVersion +
              ", should be " + Version.CURRENT);

      setChunkSize(Long.valueOf(meta.get("s3tool-chunk-size")));
      setFileLength(Long.valueOf(meta.get("s3tool-file-length")));
    }
    else
    {
      setFileLength(copy.getObjectSize());
      if (chunkSize == 0)
      {
        setChunkSize(Math.min(fileLength, Utils.getDefaultChunkSize()));
      }
    }

    OverallProgressListener opl = null;
    if (progressListenerFactory.isPresent()) {
      opl = progressListenerFactory.get().create(
          new ProgressOptionsBuilder()
              .setObjectUri(getUri(copy.getDestinationBucket(),
                  copy.getDestinationKey()))
              .setOperation("copy")
              .setFileSizeInBytes(fileLength)
              .createProgressOptions());
    }

    List<ListenableFuture<Void>> parts = new
        ArrayList<ListenableFuture<Void>>();
    for (long position = 0; position < fileLength; position += chunkSize)
    {
      parts.add(startPartCopy(copy, position, opl));
    }

    return Futures.transform(Futures.allAsList(parts), Functions.constant(copy));
  }

  private ListenableFuture<Void> startPartCopy(final Copy copy,
                                               final long position,
                                               final OverallProgressListener opl)
  {
    final int partNumber = (int) (position / chunkSize);

    return executeWithRetry(
        _executor,
        new Callable<ListenableFuture<Void>>() {
          public ListenableFuture<Void> call() {
            return startPartCopyActual(copy, position, opl);
          }

          public String toString() {
            return "copying part " + partNumber;
          }
        });
  }

  private ListenableFuture<Void> startPartCopyActual(final Copy copy,
                                                     final long position,
                                                     OverallProgressListener opl)
  {
    final int partNumber = (int) (position / chunkSize);
    long start;
    long partSize;

    if (copy.getMeta().containsKey("s3tool-key-name"))
    {
      long blockSize;
      try
      {
        blockSize = Cipher.getInstance("AES/CBC/PKCS5Padding").getBlockSize();
      } catch (NoSuchAlgorithmException e)
      {
        throw new RuntimeException(e);
      } catch (NoSuchPaddingException e)
      {
        throw new RuntimeException(e);
      }

      long postCryptSize = Math.min(fileLength - position, chunkSize);
      start = partNumber * blockSize * (chunkSize/blockSize + 2);
      partSize = blockSize * (postCryptSize/blockSize + 2);
    }
    else
    {
      start = position;
      partSize = Math.min(fileLength - position, chunkSize);
    }

    ListenableFuture<Void> copyPartFuture = copy.copyPart(partNumber, start,
        start + partSize - 1, Optional.fromNullable(opl));

    return copyPartFuture;
  }

  /**
   * Step 3: Complete parts
   */
  private AsyncFunction<Copy, String> completeAsyncFunction()
  {
    return new AsyncFunction<Copy, String>()
    {
      public ListenableFuture<String> apply(Copy copy)
      {
        return complete(copy, 0);
      }
    };
  }

  /**
   * Execute completeActual with retry
   */
  private ListenableFuture<String> complete(final Copy copy,
                                            final int retryCount)
  {
    return executeWithRetry(_executor,
        new Callable<ListenableFuture<String>>()
        {
          public ListenableFuture<String> call()
          {
            return completeActual(copy, retryCount);
          }

          public String toString()
          {
            return "completing copy";
          }
        });
  }

  private ListenableFuture<String> completeActual(final Copy copy,
                                                  final int retryCount)
  {
    return copy.completeCopy();
  }
}
