package com.pinterest.doctorkafka.security;

import java.security.Principal;
import java.util.Set;
import javax.ws.rs.core.SecurityContext;

public class DrKafkaSecurityContext implements SecurityContext {
  
  private static final String DR_KAFKA_AUTH = "drkauth";
  private UserPrincipal principal;
  private Set<String> roles;

  public DrKafkaSecurityContext(UserPrincipal principal, Set<String> roles) {
    this.principal = principal;
    this.roles = roles;
  }
  
  @Override
  public Principal getUserPrincipal() {
    return principal;
  }

  @Override
  public boolean isUserInRole(String role) {
    return roles.contains(role);
  }

  @Override
  public boolean isSecure() {
    return true;
  }

  @Override
  public String getAuthenticationScheme() {
    return DR_KAFKA_AUTH;
  }

  @Override
  public String toString() {
    return "DrKafkaSecurityContext [principal=" + principal + ", roles=" + roles + "]";
  }

}