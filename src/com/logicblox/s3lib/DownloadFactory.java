package com.logicblox.s3lib;

import com.google.common.util.concurrent.ListenableFuture;

interface DownloadFactory
{
  ListenableFuture<Download> startDownload(String bucketName, String key);
}
