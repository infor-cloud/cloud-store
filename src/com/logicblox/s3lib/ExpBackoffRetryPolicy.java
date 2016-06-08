package com.logicblox.s3lib;


import com.amazonaws.AmazonServiceException;

import java.util.Random;
import java.util.concurrent.TimeUnit;

class ExpBackoffRetryPolicy implements ThrowableRetryPolicy {
  private final Command cmd;
  private final int initialDelay;
  private final int maxDelay;
  private final int maxRetryCount;
  private final TimeUnit timeUnit;
  private Throwable thrown;
  private int retryCount;

  public ExpBackoffRetryPolicy(Command cmd,
                               int initialDelay,
                               int maxDelay,
                               int maxRetryCount,
                               TimeUnit timeUnit) {
    this.cmd = cmd;
    this.initialDelay = initialDelay;
    this.maxDelay = maxDelay;
    this.maxRetryCount = maxRetryCount;
    this.timeUnit = timeUnit;
  }

  @Override
  public int getDelay()
  {
    int delay = 0;

    if(thrown != null)
    {
      if (thrown instanceof AmazonServiceException)
      {
        AmazonServiceException exc = (AmazonServiceException) thrown;
        if(exc.getErrorType() == AmazonServiceException.ErrorType.Service &&
          exc.getErrorCode().equals("SlowDown"))
        {
          // Full Jitter
          // (https://www.awsarchitectureblog.com/2015/03/backoff.html)
          int sdInitialDelay = Math.max(initialDelay * 10, 10_0000);
          int sdMaxDelay     = Math.max(maxDelay * 10, 600_0000);
          int max = Math.min(sdMaxDelay, sdInitialDelay * (int) Math.pow(2, retryCount - 1));

          Random random = new Random();
          delay = random.nextInt(max);

          return delay;
        }
      }
    }

    if(retryCount > 0)
    {
      delay = initialDelay * (int) Math.pow(2, retryCount - 1);
      delay = Math.min(delay, maxDelay);
    }

    return delay;
  }

  @Override
  public boolean shouldRetry()
  {
    return (!isClientError() && retryCount < maxRetryCount);
  }

  @Override
  public void sleep()
  {
    int delay = getDelay();
    if(delay > 0)
    {
      try
      {
        Thread.sleep(timeUnit.toMillis(delay));
      }
      catch(InterruptedException ignored)
      {}
    }
  }

  private boolean isClientError()
  {
    if(!cmd.getRetryClientException() && thrown instanceof AmazonServiceException)
    {
      AmazonServiceException exc = (AmazonServiceException) thrown;
      if(exc.getErrorType() == AmazonServiceException.ErrorType.Client)
        return true;
    }

    return false;
  }

  @Override
  public void errorOccurred(Throwable thrown)
  {
    this.thrown = thrown;
    retryCount++;
  }
}
