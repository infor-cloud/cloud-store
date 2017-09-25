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

package com.logicblox.cloudstore;

import java.util.HashMap;
import java.util.Map;


/**
 * Internal class used to manage injection of aborts into command
 * execution, allowing testing of command abort behavior.
 */
class AbortCounters
{
  private int _abortInjectionCounter = 0;
  private boolean _globalAbortCounter = false;
  private Object _abortSync = new Object();
  private Map<String, Integer> _injectionCounters = new HashMap<String, Integer>();


  int decrementInjectionCounter(String id)
  {
    synchronized(_abortSync)
    {
      if(_abortInjectionCounter <= 0)
      {
        return 0;
      }

      if(_globalAbortCounter)
      {
        id = "";
      }

      if(!_injectionCounters.containsKey(id))
      {
        _injectionCounters.put(id, _abortInjectionCounter);
      }
      int current = _injectionCounters.get(id);
      _injectionCounters.put(id, current - 1);
      return current;
    }
  }


  void setInjectionCounter(int counter)
  {
    synchronized(_abortSync)
    {
      _abortInjectionCounter = counter;
    }
  }


  void clearInjectionCounters()
  {
    synchronized(_abortSync)
    {
      _injectionCounters.clear();
    }
  }


  // if true, use a single abort counter for all operations in a type (copy,
  // delete, upload, etc).  otherwise (default), use a separate counter for 
  // each operation type
  boolean useGlobalCounter(boolean b)
  {
    synchronized(_abortSync)
    {
      boolean old = _globalAbortCounter;
      _globalAbortCounter = b;
      return old;
    }
  }

}
