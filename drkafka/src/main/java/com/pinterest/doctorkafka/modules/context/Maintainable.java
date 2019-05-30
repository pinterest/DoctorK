package com.pinterest.doctorkafka.modules.context;

public interface Maintainable {
  Boolean isUnderMaintenance();
  void setUnderMaintenance(boolean maintenance);
}
