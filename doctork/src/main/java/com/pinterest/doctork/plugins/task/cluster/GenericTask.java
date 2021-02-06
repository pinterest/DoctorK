package com.pinterest.doctork.plugins.task.cluster;

import java.util.Map;

import com.pinterest.doctork.plugins.task.Task;

public class GenericTask extends Task {
  public GenericTask(){}
  public GenericTask(String name, Map<String, Object> attributes){
    super.setName(name);
    if (attributes != null){
      for (Map.Entry<String, Object> entry : attributes.entrySet()){
        super.setAttribute(entry.getKey(), entry.getValue());
      }
    }
  }
}
