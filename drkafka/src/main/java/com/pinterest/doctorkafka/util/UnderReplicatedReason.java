package com.pinterest.doctorkafka.util;

public enum UnderReplicatedReason {
  BROKER_FAILURE,
  DEGRADED_HARDWARE,
  FOLLOWER_NETWORK_SATURATION,
  LEADER_NETWORK_SATURATION,
  UNKNOWN
}
