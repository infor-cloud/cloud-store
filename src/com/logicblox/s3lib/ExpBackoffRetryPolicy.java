/*
  Copyright 2017, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.s3lib;


import com.amazonaws.AmazonServiceException;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public abstract class ExpBackoffRetryPolicy implements ThrowableRetryPolicy
{
  private final long _initialDelay;
  private final long _maxDelay;
  private final int _maxRetryCount;

  public ExpBackoffRetryPolicy(int initialDelay,
                               int maxDelay,
                               int maxRetryCount,
                               TimeUnit timeUnit)
  {
    _initialDelay = timeUnit.toMillis(initialDelay);
    _maxDelay = timeUnit.toMillis(maxDelay);
    _maxRetryCount = maxRetryCount;
  }

  /**
   * Returns delay in milliseconds.
   */
  @Override
  public long getDelay(Throwable thrown, int retryCount)
  {
    long delay = 0;

    if(thrown != null)
    {
      if (thrown instanceof AmazonServiceException)
      {
        AmazonServiceException exc = (AmazonServiceException) thrown;
        if(exc.getErrorType() == AmazonServiceException.ErrorType.Service &&
          exc.getErrorCode().equals("SlowDown"))
        {
          long sdInitialDelay = TimeUnit.SECONDS.toMillis(10);
          long sdMaxDelay     = TimeUnit.MINUTES.toMillis(10);
          delay = expBackoffFullJitter(sdInitialDelay, sdMaxDelay, retryCount);

          return delay;
        }
      }
    }

    if(retryCount > 0)
    {
      delay = expBackoff(_initialDelay, _maxDelay, retryCount);
    }

    return delay;
  }

  @Override
  public boolean shouldRetry(Throwable thrown, int retryCount)
  {
    return (retryOnThrowable(thrown) && retryCount < _maxRetryCount);
  }

  /**
   * Full Jitter exponential backoff as described in
   * https://www.awsarchitectureblog.com/2015/03/backoff.html
   */
  private long expBackoffFullJitter(long initialDelay, long maxDelay, int
    retryCount)
  {
    long delay = expBackoff(initialDelay, maxDelay, retryCount);

    return ThreadLocalRandom.current().nextLong(delay);
  }

  private long expBackoff(long initialDelay, long maxDelay, int retryCount)
  {
    long delay = initialDelay * (int) Math.pow(2, retryCount - 1);

    return Math.min(delay, maxDelay);
  }

  abstract public boolean retryOnThrowable(Throwable thrown);
}
