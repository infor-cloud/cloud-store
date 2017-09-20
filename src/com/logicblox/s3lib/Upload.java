package com.logicblox.s3lib;

import java.io.InputStream;
import java.util.Date;
import java.util.concurrent.Callable;

import com.google.common.util.concurrent.ListenableFuture;

interface Upload
{
  ListenableFuture<Void> uploadPart(int partNumber,
                                    long partSize,
                                    Callable<InputStream> streamCallable,
                                    OverallProgressListener opl);

  ListenableFuture<String> completeUpload();

  ListenableFuture<Void> abort();

  String getBucket();

  String getKey();

  String getId();

  Date getInitiationDate();
}
