package com.pinterest.doctorkafka.modules.action.cluster.kafka;

import com.pinterest.doctorkafka.modules.action.Action;

public interface ReassignPartition extends Action {
  void reassign(String zkUrl, String jsonReassignment) throws Exception;
}
