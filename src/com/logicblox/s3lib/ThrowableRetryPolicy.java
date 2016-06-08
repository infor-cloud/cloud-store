package com.logicblox.s3lib;


interface ThrowableRetryPolicy {
  int getDelay();
  boolean shouldRetry();
  void sleep();
  void errorOccurred(Throwable thrown);
}
