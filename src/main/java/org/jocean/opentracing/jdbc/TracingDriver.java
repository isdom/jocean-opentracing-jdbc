/*
 * Copyright 2017-2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.jocean.opentracing.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Tracer;

public class TracingDriver implements Driver {

    private static final Logger LOG = LoggerFactory.getLogger(TracingDriver.class);

  private static final TracingDriver INSTANCE = new TracingDriver();

  protected static final String TRACE_WITH_ACTIVE_SPAN_ONLY = "traceWithActiveSpanOnly";

  protected static final String WITH_ACTIVE_SPAN_ONLY = TRACE_WITH_ACTIVE_SPAN_ONLY + "=true";

  public static final String IGNORE_FOR_TRACING_REGEX = "ignoreForTracing=\"((?:\\\\\"|[^\"])*)\"[;]*";

  protected static final Pattern PATTERN_FOR_IGNORING = Pattern.compile(IGNORE_FOR_TRACING_REGEX);

  static {
    try {
      DriverManager.registerDriver(INSTANCE);
    } catch (final SQLException e) {
      throw new IllegalStateException("Could not register TracingDriver with DriverManager", e);
    }
  }

  protected Tracer tracer;

  public TracingDriver() {
      LOG.info("TracingDriver {} created", this);
  }

  @Override
  public Connection connect(final String url, final Properties info) throws SQLException {
    // if there is no url, we have problems
    if (url == null) {
      throw new SQLException("url is required");
    }

    if (!acceptsURL(url)) {
      return null;
    }

    final String realUrl = extractRealUrl(url);
    final String dbType = extractDbType(realUrl);
    final String dbUser = info.getProperty("user");

    // find the real driver for the URL
    final Driver wrappedDriver = findDriver(realUrl);
    final Connection connection = wrappedDriver.connect(realUrl, info);

    LOG.info("{} invoke connect with tracer {}", this, tracer);

    return new TracingConnection(connection, dbType, dbUser, url.contains(WITH_ACTIVE_SPAN_ONLY),
        extractIgnoredStatements(url), tracer);
  }

  @Override
  public boolean acceptsURL(final String url) throws SQLException {
    return url != null && url.startsWith(getUrlPrefix());
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
    return findDriver(url).getPropertyInfo(url, info);
  }

  @Override
  public int getMajorVersion() {
    // There is no way to get it from wrapped driver
    return 1;
  }

  @Override
  public int getMinorVersion() {
    // There is no way to get it from wrapped driver
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return true;
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // There is no way to get it from wrapped driver
    return null;
  }

  public void setTracer(final Tracer tracer) {
    this.tracer = tracer;
    LOG.info("{} invoke setTracer with tracer {}", this, this.tracer);
  }

  protected String getUrlPrefix() {
    return "jdbc:tracing:";
  }

  protected Driver findDriver(final String realUrl) throws SQLException {
    if (realUrl == null || realUrl.trim().length() == 0) {
      throw new IllegalArgumentException("url is required");
    }

    for (final Driver candidate : Collections.list(DriverManager.getDrivers())) {
        LOG.info("test {} using {}", candidate, realUrl);
      try {
        if (candidate.acceptsURL(realUrl)) {
            LOG.info("test {} success", candidate);
          return candidate;
        }
      } catch (final SQLException ignored) {
        // intentionally ignore exception
          LOG.warn("test {} failed bcs {}", candidate, ignored);
      }
    }

    throw new SQLException("Unable to find a driver that accepts url: " + realUrl);
  }

  protected String extractRealUrl(final String url) {
    final String extracted = url.startsWith(getUrlPrefix()) ? url.replace(getUrlPrefix(), "jdbc:") : url;
    return extracted.replaceAll(TRACE_WITH_ACTIVE_SPAN_ONLY + "=(true|false)[;]*", "")
        .replaceAll(IGNORE_FOR_TRACING_REGEX, "")
        .replaceAll("\\?$", "");
  }

  protected String extractDbType(final String realUrl) {
    return realUrl.split(":")[1];
  }

  protected Set<String> extractIgnoredStatements(final String url) {

    final Matcher matcher = PATTERN_FOR_IGNORING.matcher(url);

    final Set<String> results = new HashSet<>(8);

    while (matcher.find()) {
      final String rawValue = matcher.group(1);
      final String finalValue = rawValue.replace("\\\"", "\"");
      results.add(finalValue);
    }

    return results;
  }
}
