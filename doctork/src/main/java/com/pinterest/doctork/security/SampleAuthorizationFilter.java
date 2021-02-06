package com.pinterest.doctork.security;

import com.pinterest.doctork.config.DoctorKConfig;

import jersey.repackaged.com.google.common.collect.Sets;
import jersey.repackaged.com.google.common.collect.Sets.SetView;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.ext.Provider;

/**
 * This is a sample implementation of {@link DoctorKAuthorizationFilter}
 */
@Provider
@Priority(1000)
public class SampleAuthorizationFilter implements DoctorKAuthorizationFilter {

  private static final Logger LOG = LogManager.getLogger(SampleAuthorizationFilter.class);
  private static final String GROUPS_HEADER = "GROUPS";
  private static final String USER_HEADER = "USER";
  private Set<String> allowedAdminGroups = new HashSet<>();
  private static final Set<String> ADMIN_ROLE_SET = new HashSet<>(
      Arrays.asList(DoctorKConfig.DOCTORK_ADMIN_ROLE));
  private static final Set<String> EMPTY_ROLE_SET = new HashSet<>();

  @Override
  public void configure(DoctorKConfig config) throws Exception {
    List<String> doctorKAdminGroups = config.getDoctorKAdminGroups();
    if (doctorKAdminGroups != null) {
      allowedAdminGroups.addAll(doctorKAdminGroups);
      LOG.info("Following groups will be allowed admin access:" + allowedAdminGroups);
    }
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    String userHeader = requestContext.getHeaderString(USER_HEADER);
    String groupsHeader = requestContext.getHeaderString(GROUPS_HEADER);
    DoctorKSecurityContext ctx = null;
    if (userHeader != null && groupsHeader != null) {
      Set<String> userGroups = new HashSet<>(Arrays.asList(groupsHeader.split(",")));
      SetView<String> intersection = Sets.intersection(allowedAdminGroups, userGroups);
      if (intersection.size() > 0) {
        ctx = new DoctorKSecurityContext(new UserPrincipal(userHeader), ADMIN_ROLE_SET);
        requestContext.setSecurityContext(ctx);
        LOG.info("Received authenticated request, created context:" + ctx);
        return;
      }
    }
    
    ctx = new DoctorKSecurityContext(new UserPrincipal(userHeader), EMPTY_ROLE_SET);
    requestContext.setSecurityContext(ctx);
    LOG.info("Received annonymous request, bypassing authorizer");
  }

}