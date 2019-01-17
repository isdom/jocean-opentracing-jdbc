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


import static org.jocean.opentracing.jdbc.JdbcTracingUtils.buildScope;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import io.opentracing.Tracer;

public class TracingStatement implements Statement {

    private static final Logger LOG = LoggerFactory.getLogger(TracingStatement.class);

  private final Statement statement;
  private final String query;
  private final ArrayList<String> batchCommands = new ArrayList<>();
  private final String dbType;
  private final String dbUser;
  private final boolean withActiveSpanOnly;
  private final Set<String> ignoredStatements;
  private final Tracer tracer;

  TracingStatement(final Statement statement, final String dbType, final String dbUser, final boolean withActiveSpanOnly,
      final Set<String> ignoredStatements) {
    this(statement, dbType, dbUser, withActiveSpanOnly, ignoredStatements, null);
  }

  TracingStatement(final Statement statement, final String dbType, final String dbUser, final boolean withActiveSpanOnly,
      final Set<String> ignoredStatements, final Tracer tracer) {
    this(statement, null, dbType, dbUser, withActiveSpanOnly, ignoredStatements, tracer);
  }

  TracingStatement(final Statement statement, final String query, final String dbType, final String dbUser,
      final boolean withActiveSpanOnly, final Set<String> ignoredStatements) {
    this(statement, query, dbType, dbUser, withActiveSpanOnly, ignoredStatements, null);
  }

  TracingStatement(final Statement statement, final String query, final String dbType, final String dbUser,
      final boolean withActiveSpanOnly, final Set<String> ignoredStatements, final Tracer tracer) {
    this.statement = statement;
    this.query = query;
    this.dbType = dbType;
    this.dbUser = dbUser;
    this.withActiveSpanOnly = withActiveSpanOnly;
    this.ignoredStatements = ignoredStatements;
    this.tracer = tracer;
  }

  @Override
  public ResultSet executeQuery(final String sql) throws SQLException {
    final Scope scope = buildScope("Query", sql, dbType, dbUser, withActiveSpanOnly, ignoredStatements,
        tracer);
    try {
        LOG.info("executeQuery: tracer:{}/span:{}",tracer, scope.span());
      return statement.executeQuery(sql);
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
      LOG.info("executeQuery finally block: tracer:{}/span:{}",tracer, scope.span());
    }
  }

  @Override
  public int executeUpdate(final String sql) throws SQLException {
    final Scope scope = buildScope("Update", sql, dbType, dbUser, withActiveSpanOnly, ignoredStatements,
        tracer);
    try {
        LOG.info("executeUpdate: tracer:{}/span:{}",tracer, scope.span());
      return statement.executeUpdate(sql);
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
      LOG.info("executeUpdate finally block: tracer:{}/span:{}",tracer, scope.span());
    }
  }

  @Override
  public void close() throws SQLException {
    statement.close();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return statement.getMaxFieldSize();
  }

  @Override
  public void setMaxFieldSize(final int max) throws SQLException {
    statement.setMaxFieldSize(max);
  }

  @Override
  public int getMaxRows() throws SQLException {
    return statement.getMaxRows();
  }

  @Override
  public void setMaxRows(final int max) throws SQLException {
    statement.setMaxRows(max);
  }

  @Override
  public void setEscapeProcessing(final boolean enable) throws SQLException {
    statement.setEscapeProcessing(enable);
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return statement.getQueryTimeout();
  }

  @Override
  public void setQueryTimeout(final int seconds) throws SQLException {
    statement.setQueryTimeout(seconds);
  }

  @Override
  public void cancel() throws SQLException {
    statement.cancel();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return statement.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    statement.clearWarnings();
  }

  @Override
  public void setCursorName(final String name) throws SQLException {
    statement.setCursorName(name);
  }

  @Override
  public boolean execute(final String sql) throws SQLException {
    final Scope scope = buildScope("Execute", sql, dbType, dbUser, withActiveSpanOnly, ignoredStatements,
        tracer);
    try {
        LOG.info("execute: tracer:{}/span:{}",tracer, scope.span());
      return statement.execute(sql);
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
      LOG.info("execute finally block: tracer:{}/span:{}",tracer, scope.span());
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return statement.getResultSet();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return statement.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return statement.getMoreResults();
  }

  @Override
  public void setFetchDirection(final int direction) throws SQLException {
    statement.setFetchDirection(direction);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return statement.getFetchDirection();
  }

  @Override
  public void setFetchSize(final int rows) throws SQLException {
    statement.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() throws SQLException {
    return statement.getFetchSize();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return statement.getResultSetConcurrency();
  }

  @Override
  public int getResultSetType() throws SQLException {
    return statement.getResultSetType();
  }

  @Override
  public void addBatch(final String sql) throws SQLException {
    statement.addBatch(sql);
    batchCommands.add(sql);
  }

  @Override
  public void clearBatch() throws SQLException {
    statement.clearBatch();
    batchCommands.clear();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    final Scope scope = buildScopeForBatch();
    try {
        LOG.info("executeBatch: tracer:{}/span:{}",tracer, scope.span());
      return statement.executeBatch();
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    return statement.getConnection();
  }

  @Override
  public boolean getMoreResults(final int current) throws SQLException {
    return statement.getMoreResults(current);
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return statement.getGeneratedKeys();
  }

  @Override
  public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
    final Scope scope = buildScope("Update", sql, dbType, dbUser, withActiveSpanOnly, ignoredStatements,
        tracer);
    try {
        LOG.info("executeUpdate with autoGeneratedKeys({}): tracer:{}/span:{}",autoGeneratedKeys, tracer, scope.span());
      return statement.executeUpdate(sql, autoGeneratedKeys);
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
    }
  }

  @Override
  public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
    final Scope scope = buildScope("Update", sql, dbType, dbUser, withActiveSpanOnly, ignoredStatements,
        tracer);
    try {
        LOG.info("executeUpdate with columnIndexes: tracer:{}/span:{}",tracer, scope.span());
      return statement.executeUpdate(sql, columnIndexes);
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
    }
  }

  @Override
  public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
    final Scope scope = buildScope("Update", sql, dbType, dbUser, withActiveSpanOnly, ignoredStatements,
        tracer);
    try {
        LOG.info("executeUpdate with columnNames: tracer:{}/span:{}",tracer, scope.span());
      return statement.executeUpdate(sql, columnNames);
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
    }
  }

  @Override
  public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
    final Scope scope = buildScope("Execute", sql, dbType, dbUser, withActiveSpanOnly, ignoredStatements,
        tracer);
    try {
        LOG.info("execute with autoGeneratedKeys({}): tracer:{}/span:{}", autoGeneratedKeys, tracer, scope.span());
      return statement.execute(sql, autoGeneratedKeys);
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
    }
  }

  @Override
  public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
    final Scope scope = buildScope("Execute", sql, dbType, dbUser, withActiveSpanOnly, ignoredStatements,
        tracer);
    try {
        LOG.info("execute with columnIndexes: tracer:{}/span:{}", tracer, scope.span());
      return statement.execute(sql, columnIndexes);
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
    }
  }

  @Override
  public boolean execute(final String sql, final String[] columnNames) throws SQLException {
    final Scope scope = buildScope("Execute", sql, dbType, dbUser, withActiveSpanOnly, ignoredStatements,
        tracer);
    try {
        LOG.info("execute with columnNames: tracer:{}/span:{}", tracer, scope.span());
      return statement.execute(sql, columnNames);
    } catch (final Exception e) {
      JdbcTracingUtils.onError(e, scope.span());
      throw e;
    } finally {
      scope.close();
    }
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return statement.getResultSetHoldability();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return statement.isClosed();
  }

  @Override
  public void setPoolable(final boolean poolable) throws SQLException {
    statement.setPoolable(poolable);
  }

  @Override
  public boolean isPoolable() throws SQLException {
    return statement.isPoolable();
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    statement.closeOnCompletion();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    return statement.isCloseOnCompletion();
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    return statement.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return statement.isWrapperFor(iface);
  }

  private Scope buildScopeForBatch() {
    final StringBuilder sqlBuilder = new StringBuilder();
    if (query != null) {
      sqlBuilder.append(query);
    }

    for (final String batchCommand : batchCommands) {
      sqlBuilder.append(batchCommand);
    }

    return buildScope("Batch", sqlBuilder.toString(), dbType, dbUser, withActiveSpanOnly,
        ignoredStatements, tracer);
  }
}
