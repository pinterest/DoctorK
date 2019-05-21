package com.pinterest.doctorkafka.security;

import javax.ws.rs.container.ContainerRequestFilter;

import com.pinterest.doctorkafka.config.DoctorKafkaConfig;

/**
 * This extends JAX-RS containter request filter for authorization. 
 * 
 * Please refer to https://docs.oracle.com/javaee/7/api/javax/ws/rs/container/ContainerRequestFilter.html
 * for more details on how {@link ContainerRequestFilter} works
 */
public interface DrKafkaAuthorizationFilter extends ContainerRequestFilter {
  
  public void configure(DoctorKafkaConfig config) throws Exception;

}