package com.logicblox.s3lib;

import java.util.HashMap;
import java.util.Map;


public class AbortCounters
{
  private int _abortInjectionCounter = 0;
  private boolean _globalAbortCounter = false;
  private Object _abortSync = new Object();
  private Map<String,Integer> _injectionCounters = new HashMap<String,Integer>();


  public int decrementInjectionCounter(String id)
  {
    synchronized(_abortSync)
    {
      if(_abortInjectionCounter <= 0)
        return 0;

      if(_globalAbortCounter)
        id = "";

      if(!_injectionCounters.containsKey(id))
        _injectionCounters.put(id, _abortInjectionCounter);
      int current = _injectionCounters.get(id);
      _injectionCounters.put(id, current - 1);
      return current;
    }
  }


  public void setInjectionCounter(int counter)
  {
    synchronized(_abortSync)
    {
      _abortInjectionCounter = counter;
    }
  }


  public void clearInjectionCounters()
  {
    synchronized(_abortSync)
    {
      _injectionCounters.clear();
    }
  }


  // if true, use a single abort counter for all operations in a type (copy,
  // delete, upload, etc).  otherwise (default), use a separate counter for 
  // each operation type
  public boolean useGlobalCounter(boolean b)
  {
    synchronized(_abortSync)
    {
      boolean old = _globalAbortCounter;
      _globalAbortCounter = b;
      return old;
    }
  }

}
