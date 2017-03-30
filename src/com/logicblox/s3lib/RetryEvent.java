package com.logicblox.s3lib;


public class RetryEvent
{
  private String _callableId;
  private Throwable _throwable;
  
  RetryEvent(String callableId, Throwable t)
  {
    _callableId = callableId;
    _throwable = t;
  }

  public String getCallableId()
  {
    return _callableId;
  }

  public Throwable getThrowable()
  {
    return _throwable;
  }
}
