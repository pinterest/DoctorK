package com.pinterest.doctork.config;

import org.codehaus.jackson.annotate.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import io.dropwizard.Configuration;

public class DoctorKAppConfig extends Configuration {

  @JsonProperty
  @NotEmpty
  private String config;

  public DoctorKAppConfig() {
  }

  /**
   * @return the config
   */
  public String getConfig() {
    return config;
  }

  /**
   * @param config the config to set
   */
  public void setConfig(String config) {
    this.config = config;
  }

}
