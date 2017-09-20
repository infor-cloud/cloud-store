package com.logicblox.s3lib;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

interface Copy
{
  ListenableFuture<Void> copyPart(int partNumber, Long startByte, Long
      endByte, OverallProgressListener opl);

  ListenableFuture<String> completeCopy();

  String getSourceBucketName();

  String getSourceObjectKey();

  String getDestinationBucketName();

  String getDestinationObjectKey();

  Long getObjectSize();

  Map<String,String> getMeta();
}
