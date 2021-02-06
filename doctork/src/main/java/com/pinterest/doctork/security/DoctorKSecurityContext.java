package com.pinterest.doctork.security;

import java.security.Principal;
import java.util.Set;

import javax.ws.rs.core.SecurityContext;

public class DoctorKSecurityContext implements SecurityContext {
  
  private static final String DOCTORK_AUTH = "doctorkauth";
  private UserPrincipal principal;
  private Set<String> roles;

  public DoctorKSecurityContext(UserPrincipal principal, Set<String> roles) {
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
    return DOCTORK_AUTH;
  }

  @Override
  public String toString() {
    return "DoctorKSecurityContext [principal=" + principal + ", roles=" + roles + "]";
  }

}