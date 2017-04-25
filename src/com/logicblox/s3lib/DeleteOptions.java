package com.logicblox.s3lib;

import java.util.HashMap;
import java.util.Map;

public class DeleteOptions {
  
  private String _bucket;
  private String _objectKey;
  private boolean _recursive;
  private boolean _dryRun;
  private boolean _forceDelete;
  private boolean _ignoreAbortInjection;
  
  // for testing injecion of aborts during a delete
  private static int _abortInjectionCounter = 0;
  private static boolean _globalAbortCounter = false;
  private static Object _abortSync = new Object();
  private static Map<String,Integer> _injectionCounters = new HashMap<String,Integer>();

  DeleteOptions(String bucket, String objectKey, boolean recursive, boolean dryRun,
    boolean forceDelete, boolean ignoreAbortInjection)
  {
    _bucket = bucket;
    _objectKey = objectKey;
    _recursive = recursive;
    _dryRun = dryRun;
    _forceDelete = forceDelete;
    _ignoreAbortInjection = ignoreAbortInjection;
  }
  
  // for testing injection of aborts during a delete
  static void setAbortInjectionCounter(int counter)
  {
    synchronized(_abortSync)
    {
      _abortInjectionCounter = counter;
    }
  }

  // for testing injection of aborts during a delete
  static int decrementAbortInjectionCounter(String id)
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

  // if true, use a single abort counter for all delete operations.
  // otherwise (default), use a separate counter for each delete
  static boolean useGlobalAbortCounter(boolean b)
  {
    synchronized(_abortSync)
    {
      boolean old = _globalAbortCounter;
      _globalAbortCounter = b;
      return old;
    }
  }

  public boolean ignoreAbortInjection()
  {
    return _ignoreAbortInjection;
  }

  public String getBucket()
  {
    return _bucket;
  }
  
  public String getObjectKey()
  {
    return _objectKey;
  }
  
  public boolean isRecursive()
  {
    return _recursive;
  }
  
  public boolean isDryRun()
  {
    return _dryRun;
  }

  public boolean forceDelete()
  {
    return _forceDelete;
  }
}
