package com.pinterest.doctorkafka.util;

import org.apache.logging.log4j.Logger;

import javax.servlet.http.HttpServletRequest;

public class ApiUtils {
  public static void logAPIAction(Logger LOG, HttpServletRequest ctx, String message) {
    LOG.info("User from:" + ctx.getRemoteUser() + " from ip:" + ctx.getRemoteHost() + " " + message);
  }
}
