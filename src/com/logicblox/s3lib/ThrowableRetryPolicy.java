package com.logicblox.s3lib;


public interface ThrowableRetryPolicy
{
  long getDelay(Throwable thrown, int retryCount);
  boolean shouldRetry(Throwable thrown, int retryCount);
}
