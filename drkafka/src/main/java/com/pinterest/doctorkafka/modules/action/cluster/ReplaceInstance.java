package com.pinterest.doctorkafka.modules.action.cluster;

import com.pinterest.doctorkafka.modules.action.Action;

public interface ReplaceInstance extends Action {
  void replace(String hostname) throws Exception;
}
