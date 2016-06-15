package com.logicblox.s3lib;


interface ThrowableRetryPolicy
{
  long getDelay(Throwable thrown, int retryCount);
  boolean shouldRetry(Throwable thrown, int retryCount);
}
