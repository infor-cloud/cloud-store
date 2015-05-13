package com.logicblox.s3lib;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;

interface Copy
{
  ListenableFuture<Void> copyPart(int partNumber, long startByte, long
      endByte, Optional<OverallProgressListener> opl);

  ListenableFuture<String> completeCopy();

  String getSourceBucket();

  String getSourceKey();

  String getDestinationBucket();

  String getDestinationKey();

  Long getObjectSize();

  Map<String,String> getMeta();
}
