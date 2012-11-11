package com.logicblox.s3lib;

import java.util.Map;
import java.io.InputStream;

import com.google.common.util.concurrent.ListenableFuture;

interface Download
{
  ListenableFuture<InputStream> getPart(long start, long end);
  
  Map<String,String> getMeta();

  long getLength();
}
