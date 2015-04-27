package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class ListPendingUploadsCommand extends Command
{
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;

  public ListPendingUploadsCommand(
      ListeningExecutorService httpExecutor,
      ListeningScheduledExecutorService internalExecutor)
  {
    _httpExecutor = httpExecutor;
    _executor = internalExecutor;
  }

  /**
   * Returns a list of the pending uploads to keys that start with {@code
   * prefix}, inside {@code bucket}.
   *
   * @param bucket The name of the bucket
   * @param prefix Prefix to limit the returned uploads to those for keys that
   *               match this prefix
   */
  public ListenableFuture<List<Upload>> run(final String bucket,
                                            final String prefix)
  {
    ListenableFuture<List<Upload>> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<List<Upload>>>()
        {
          public ListenableFuture<List<Upload>> call()
          {
            return runActual(bucket, prefix);
          }

          public String toString()
          {
            return "list pending uploads of " + getScheme() + bucket + "/" +
                prefix;
          }
        });

    return future;
  }

  private ListenableFuture<List<Upload>> runActual(final String bucket,
                                                   final String prefix)
  {
    return _httpExecutor.submit(
      new Callable<List<Upload>>()
      {
        public List<Upload> call()
        {
          ListMultipartUploadsRequest listMultipartUploadsRequest =
              new ListMultipartUploadsRequest(bucket);

          listMultipartUploadsRequest.setPrefix(prefix);

          MultipartUploadListing multipartUploadListing = getAmazonS3Client()
              .listMultipartUploads(listMultipartUploadsRequest);
          List<Upload> uploadsList = new ArrayList<Upload>();

          while (multipartUploadListing.isTruncated())
          {
            appendMultipartUploadList(uploadsList,
                multipartUploadListing.getMultipartUploads(),
                bucket);
            listMultipartUploadsRequest.setKeyMarker(
                multipartUploadListing.getNextKeyMarker());
            multipartUploadListing = getAmazonS3Client()
                .listMultipartUploads(listMultipartUploadsRequest);
          }
          appendMultipartUploadList(uploadsList,
              multipartUploadListing.getMultipartUploads(),
              bucket);

          return uploadsList;
        }
      });
  }

  private List<Upload> appendMultipartUploadList(List<Upload> mailList,
                                                 List<MultipartUpload>
                                                     appendList,
                                                 String bucket)
  {
    for (MultipartUpload u : appendList)
      mailList.add(S3MultipartUploadToUpload(u, bucket));

    return mailList;
  }

  private Upload S3MultipartUploadToUpload(MultipartUpload multipartUpload,
                                           String bucket)
  {
    Upload u = new MultipartAmazonUpload(
        getAmazonS3Client(),
        bucket,
        multipartUpload.getKey(),
        multipartUpload.getUploadId(),
        _httpExecutor);

    return u;
  }
}
