package com.logicblox.s3lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DirectoryNode {
  
  
  List<DirectoryNode> childs;
  String dirName;
  Long size;
  int depth;
  String rootPath;
  boolean file = false;
  
  public DirectoryNode(String nodeValue, String rootPath, Long nsize, int ndepth) {
    childs = new ArrayList<DirectoryNode>();
    dirName = nodeValue;
    if (size != null) {
      size += nsize;
    } else {
      size = new Long(nsize);
    }
    depth = ndepth;
    this.rootPath = rootPath;
  }
  
  public void addElement(String currentPath, String[] list, Long nsize, int ndepth) {
    
    while (list[0] == null || list[0].equals(""))
      list = Arrays.copyOfRange(list, 1, list.length);
    
    DirectoryNode currentChild =
        new DirectoryNode(list[0], currentPath + "/" + list[0], nsize, ndepth);
    int index = childs.indexOf(currentChild);
    if (list.length == 1) {
      currentChild.file = true;
      childs.add(currentChild);
      return;
    } else {
      if (index == - 1) {
        childs.add(currentChild);
        currentChild.addElement(currentChild.rootPath, Arrays.copyOfRange(list, 1, list.length),
            nsize, ndepth + 1);
      } else {
        DirectoryNode nextChild = childs.get(index);
        nextChild.size = currentChild.size + nextChild.size;
        nextChild.addElement(currentChild.rootPath, Arrays.copyOfRange(list, 1, list.length),
            currentChild.size, ndepth + 1);
      }
    }
  }
  
  @Override
  public boolean equals(Object obj) {
    DirectoryNode cmpObj = (DirectoryNode) obj;
    return rootPath.equals(cmpObj.rootPath) && dirName.equals(cmpObj.dirName);
  }
  
  @Override
  public String toString() {
    return rootPath + " " + " " + size;
  }
  
}
