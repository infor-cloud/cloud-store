package com.logicblox.s3lib;


public interface RetryListener
{
  public void retryTriggered(RetryEvent e);
}
