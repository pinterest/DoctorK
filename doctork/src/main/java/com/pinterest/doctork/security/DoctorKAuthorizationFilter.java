package com.pinterest.doctork.security;

import javax.ws.rs.container.ContainerRequestFilter;

import com.pinterest.doctork.config.DoctorKConfig;

/**
 * This extends JAX-RS containter request filter for authorization. 
 * 
 * Please refer to https://docs.oracle.com/javaee/7/api/javax/ws/rs/container/ContainerRequestFilter.html
 * for more details on how {@link ContainerRequestFilter} works
 */
public interface DoctorKAuthorizationFilter extends ContainerRequestFilter {
  
  public void configure(DoctorKConfig config) throws Exception;

}