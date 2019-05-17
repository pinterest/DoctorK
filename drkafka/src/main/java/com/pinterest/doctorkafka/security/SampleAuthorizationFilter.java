package com.pinterest.doctorkafka.security;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.ext.Provider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.pinterest.doctorkafka.config.DoctorKafkaConfig;

import jersey.repackaged.com.google.common.collect.Sets;
import jersey.repackaged.com.google.common.collect.Sets.SetView;

/**
 * This is a sample implementation of {@link DrKafkaAuthorizationFilter}
 */
@Provider
@Priority(1000)
public class SampleAuthorizationFilter implements DrKafkaAuthorizationFilter {

  private static final Logger LOG = LogManager.getLogger(SampleAuthorizationFilter.class);
  private static final String GROUPS_HEADER = "GROUPS";
  private static final String USER_HEADER = "USER";
  private Set<String> allowedAdminGroups = new HashSet<>();
  private static final Set<String> ADMIN_ROLE_SET = new HashSet<>(
      Arrays.asList(DoctorKafkaConfig.DRKAFKA_ADMIN_ROLE));
  private static final Set<String> EMPTY_ROLE_SET = new HashSet<>();

  @Override
  public void configure(DoctorKafkaConfig config) throws Exception {
    List<String> drKafkaAdminGroups = config.getDrKafkaAdminGroups();
    if (drKafkaAdminGroups != null) {
      allowedAdminGroups.addAll(drKafkaAdminGroups);
      LOG.info("Following groups will be allowed admin access:" + allowedAdminGroups);
    }
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    String userHeader = requestContext.getHeaderString(USER_HEADER);
    String groupsHeader = requestContext.getHeaderString(GROUPS_HEADER);
    DrKafkaSecurityContext ctx = null;
    if (userHeader != null && groupsHeader != null) {
      Set<String> userGroups = new HashSet<>(Arrays.asList(groupsHeader.split(",")));
      SetView<String> intersection = Sets.intersection(allowedAdminGroups, userGroups);
      if (intersection.size() > 0) {
        ctx = new DrKafkaSecurityContext(new UserPrincipal(userHeader), ADMIN_ROLE_SET);
        requestContext.setSecurityContext(ctx);
        LOG.info("Received authenticated request, created context:" + ctx);
        return;
      }
    }
    
    ctx = new DrKafkaSecurityContext(new UserPrincipal(userHeader), EMPTY_ROLE_SET);
    requestContext.setSecurityContext(ctx);
    LOG.info("Received annonymous request, bypassing authorizer");
  }

}