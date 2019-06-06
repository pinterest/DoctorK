package com.pinterest.doctorkafka.modules.context;

/**
 * Should be implemented by entities that need a way to stop automated operations (e.g. manual intervention that might interfere with Doctorkafka operations)
 */
public interface Maintainable {
  Boolean isUnderMaintenance();
  void setUnderMaintenance(boolean maintenance);
}
