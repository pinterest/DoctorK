package com.pinterest.doctorkafka.util;

public enum UnderReplicatedReason {
  FOLLOWER_FAILURE,
  LEADER_FAILURE,
  NO_LEADER_FAILURE,
  DEGRADED_HARDWARE,
  FOLLOWER_NETWORK_SATURATION,
  LEADER_NETWORK_SATURATION,
  REPLICA_INCREASE,
  UNKNOWN
}
