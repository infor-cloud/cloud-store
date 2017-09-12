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
  private PendingUploadsOptions options;
  private ListeningExecutorService _httpExecutor;
  private ListeningScheduledExecutorService _executor;
  private PendingUploadsOptions _options;

  public ListPendingUploadsCommand(PendingUploadsOptions options)
  {
    _options = options;
    _httpExecutor = _options.getCloudStoreClient().getApiExecutor();
    _executor = _options.getCloudStoreClient().getInternalExecutor();
  }

  public ListenableFuture<List<Upload>> run()
  {
    ListenableFuture<List<Upload>> future =
      executeWithRetry(
        _executor,
        new Callable<ListenableFuture<List<Upload>>>()
        {
          public ListenableFuture<List<Upload>> call()
          {
            return runActual();
          }

          public String toString()
          {
            return "list pending uploads of " + getUri(_options.getBucket(),
              _options.getObjectKey());
          }
        });

    return future;
  }

  private ListenableFuture<List<Upload>> runActual()
  {
    return _httpExecutor.submit(
      new Callable<List<Upload>>()
      {
        public List<Upload> call()
        {
          ListMultipartUploadsRequest listMultipartUploadsRequest =
              new ListMultipartUploadsRequest(_options.getBucket());

          listMultipartUploadsRequest.setPrefix(_options.getObjectKey());

          MultipartUploadListing multipartUploadListing = getAmazonS3Client()
              .listMultipartUploads(listMultipartUploadsRequest);
          List<Upload> uploadsList = new ArrayList<Upload>();

          while (multipartUploadListing.isTruncated())
          {
            appendMultipartUploadList(uploadsList,
                multipartUploadListing.getMultipartUploads(),
                _options.getBucket());
            listMultipartUploadsRequest.setKeyMarker(
                multipartUploadListing.getNextKeyMarker());
            multipartUploadListing = getAmazonS3Client()
                .listMultipartUploads(listMultipartUploadsRequest);
          }
          appendMultipartUploadList(uploadsList,
              multipartUploadListing.getMultipartUploads(),
              _options.getBucket());

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
        multipartUpload.getInitiated(),
        _httpExecutor,
        (new UploadOptionsBuilder()).createUploadOptions());

    return u;
  }
}
