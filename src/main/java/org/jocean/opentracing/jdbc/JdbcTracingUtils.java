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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopScopeManager.NoopScope;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;


class JdbcTracingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcTracingUtils.class);

  static final String COMPONENT_NAME = "java-jdbc";

  static Scope buildScope(final String operationName,
      final String sql,
      final String dbType,
      final String dbUser,
      final boolean withActiveSpanOnly,
      final Set<String> ignoredStatements,
      final Tracer tracer) {
    final Tracer currentTracer = getNullsafeTracer(tracer);
    if (withActiveSpanOnly && currentTracer.activeSpan() == null) {
      return NoopScope.INSTANCE;
    } else if (ignoredStatements != null && ignoredStatements.contains(sql)) {
      return NoopScope.INSTANCE;
    }

    final Tracer.SpanBuilder spanBuilder = currentTracer.buildSpan(operationName)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

    final Scope scope = spanBuilder.startActive(true);
    decorate(scope.span(), sql, dbType, dbUser);

    LOG.info("buildScope: tracer:{}/span:{}", currentTracer, scope.span());

    return scope;
  }

  private static Tracer getNullsafeTracer(final Tracer tracer) {
    if (tracer == null) {
      return GlobalTracer.get();
    }
    return tracer;
  }

  private static void decorate(final Span span,
      final String sql,
      final String dbType,
      final String dbUser) {
    Tags.COMPONENT.set(span, COMPONENT_NAME);
    Tags.DB_STATEMENT.set(span, sql);
    Tags.DB_TYPE.set(span, dbType);
    if (dbUser != null) {
      Tags.DB_USER.set(span, dbUser);
    }
  }

  static void onError(final Throwable throwable, final Span span) {
    Tags.ERROR.set(span, Boolean.TRUE);

    if (throwable != null) {
      span.log(errorLogs(throwable));
    }
  }

  private static Map<String, Object> errorLogs(final Throwable throwable) {
    final Map<String, Object> errorLogs = new HashMap<>(2);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.object", throwable);
    return errorLogs;
  }
}
