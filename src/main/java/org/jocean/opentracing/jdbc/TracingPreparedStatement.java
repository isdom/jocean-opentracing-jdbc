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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import io.opentracing.Tracer;

public class TracingPreparedStatement extends TracingStatement implements PreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(TracingPreparedStatement.class);

  private final PreparedStatement preparedStatement;
  private final String query;
  private final String dbType;
  private final String dbUser;
  private final boolean withActiveSpanOnly;
  private final Set<String> ignoredQueries;
  private final Tracer tracer;
  private final Map<String, Object> params = new HashMap<>();

  public TracingPreparedStatement(final PreparedStatement preparedStatement, final String query, final String dbType,
      final String dbUser, final boolean withActiveSpanOnly, final Set<String> ignoredStatements) {
    this(preparedStatement, query, dbType, dbUser, withActiveSpanOnly, ignoredStatements, null);
  }

  public TracingPreparedStatement(final PreparedStatement preparedStatement, final String query, final String dbType,
      final String dbUser, final boolean withActiveSpanOnly, final Set<String> ignoredStatements, final Tracer tracer) {
    super(preparedStatement, query, dbType, dbUser, withActiveSpanOnly, ignoredStatements);
    this.preparedStatement = preparedStatement;
    this.query = query;
    this.dbType = dbType;
    this.dbUser = dbUser;
    this.withActiveSpanOnly = withActiveSpanOnly;
    this.ignoredQueries = ignoredStatements;
    this.tracer = tracer;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    final Scope scope = buildScope("Query", query, dbType, dbUser, withActiveSpanOnly, ignoredQueries,
        tracer, params);
    try {
        LOG.debug("executeQuery: tracer:{}/span:{}", tracer, scope.span());
        return preparedStatement.executeQuery();
    } catch (final Exception e) {
        JdbcTracingUtils.onError(e, scope.span());
        throw e;
    } finally {
        scope.close();
    }
  }

  @Override
  public int executeUpdate() throws SQLException {
    final Scope scope = buildScope("Update", query, dbType, dbUser, withActiveSpanOnly, ignoredQueries,
        tracer, params);
    try {
        LOG.debug("executeUpdate: tracer:{}/span:{}", tracer, scope.span());
        return preparedStatement.executeUpdate();
    } catch (final Exception e) {
        JdbcTracingUtils.onError(e, scope.span());
        throw e;
    } finally {
        scope.close();
    }
  }

  private String idx2key(final int parameterIndex) {
      return "sql.param." + parameterIndex;
  }

  @Override
  public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
    preparedStatement.setNull(parameterIndex, sqlType);
    params.put(idx2key(parameterIndex), "(null)");
  }

  @Override
  public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {
    preparedStatement.setBoolean(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setByte(final int parameterIndex, final byte x) throws SQLException {
    preparedStatement.setByte(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setShort(final int parameterIndex, final short x) throws SQLException {
    preparedStatement.setShort(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setInt(final int parameterIndex, final int x) throws SQLException {
    preparedStatement.setInt(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setLong(final int parameterIndex, final long x) throws SQLException {
    preparedStatement.setLong(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setFloat(final int parameterIndex, final float x) throws SQLException {
    preparedStatement.setFloat(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setDouble(final int parameterIndex, final double x) throws SQLException {
    preparedStatement.setDouble(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
    preparedStatement.setBigDecimal(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setString(final int parameterIndex, final String x) throws SQLException {
    preparedStatement.setString(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
    preparedStatement.setBytes(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setDate(final int parameterIndex, final Date x) throws SQLException {
    preparedStatement.setDate(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setTime(final int parameterIndex, final Time x) throws SQLException {
    preparedStatement.setTime(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException {
    preparedStatement.setTimestamp(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
    preparedStatement.setAsciiStream(parameterIndex, x, length);
    params.put(idx2key(parameterIndex), "ascii stream[" + length + "]");
  }

  @Override
  @Deprecated
  public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
    preparedStatement.setUnicodeStream(parameterIndex, x, length);
    params.put(idx2key(parameterIndex), "unicode stream[" + length + "]");
  }

  @Override
  public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
    preparedStatement.setBinaryStream(parameterIndex, x, length);
    params.put(idx2key(parameterIndex), "binary stream[" + length + "]");
  }

  @Override
  public void clearParameters() throws SQLException {
    preparedStatement.clearParameters();
    params.clear();
  }

  @Override
  public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException {
    preparedStatement.setObject(parameterIndex, x, targetSqlType);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setObject(final int parameterIndex, final Object x) throws SQLException {
    preparedStatement.setObject(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public boolean execute() throws SQLException {
    final Scope scope = buildScope("Execute", query, dbType, dbUser, withActiveSpanOnly,
        ignoredQueries, tracer, params);
    try {
        LOG.debug("execute: tracer:{}/span:{}", tracer, scope.span());
        return preparedStatement.execute();
    } catch (final Exception e) {
        JdbcTracingUtils.onError(e, scope.span());
        throw e;
    } finally {
        scope.close();
    }
  }

  @Override
  public void addBatch() throws SQLException {
    preparedStatement.addBatch();
  }

  @Override
  public void setCharacterStream(final int parameterIndex, final Reader reader, final int length)
      throws SQLException {
    preparedStatement.setCharacterStream(parameterIndex, reader, length);
    params.put(idx2key(parameterIndex), "character reader[" + length + "]");
  }

  @Override
  public void setRef(final int parameterIndex, final Ref x) throws SQLException {
    preparedStatement.setRef(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setBlob(final int parameterIndex, final Blob x) throws SQLException {
    preparedStatement.setBlob(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setClob(final int parameterIndex, final Clob x) throws SQLException {
    preparedStatement.setClob(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setArray(final int parameterIndex, final Array x) throws SQLException {
    preparedStatement.setArray(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return preparedStatement.getMetaData();
  }

  @Override
  public void setDate(final int parameterIndex, final Date x, final Calendar cal) throws SQLException {
    preparedStatement.setDate(parameterIndex, x, cal);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setTime(final int parameterIndex, final Time x, final Calendar cal) throws SQLException {
    preparedStatement.setTime(parameterIndex, x, cal);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException {
    preparedStatement.setTimestamp(parameterIndex, x, cal);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException {
    preparedStatement.setNull(parameterIndex, sqlType, typeName);
    params.put(idx2key(parameterIndex), "(null)");
  }

  @Override
  public void setURL(final int parameterIndex, final URL x) throws SQLException {
    preparedStatement.setURL(parameterIndex, x);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return preparedStatement.getParameterMetaData();
  }

  @Override
  public void setRowId(final int parameterIndex, final RowId x) throws SQLException {
    preparedStatement.setRowId(parameterIndex, x);
  }

  @Override
  public void setNString(final int parameterIndex, final String value) throws SQLException {
    preparedStatement.setNString(parameterIndex, value);
    params.put(idx2key(parameterIndex), value);
  }

  @Override
  public void setNCharacterStream(final int parameterIndex, final Reader value, final long length)
      throws SQLException {
    preparedStatement.setNCharacterStream(parameterIndex, value, length);
    params.put(idx2key(parameterIndex), "ncharacter reader[" + length + "]");
  }

  @Override
  public void setNClob(final int parameterIndex, final NClob value) throws SQLException {
    preparedStatement.setNClob(parameterIndex, value);
    params.put(idx2key(parameterIndex), value);
  }

  @Override
  public void setClob(final int parameterIndex, final Reader reader, final long length) throws SQLException {
    preparedStatement.setClob(parameterIndex, reader, length);
    params.put(idx2key(parameterIndex), "clob reader[" + length + "]");
  }

  @Override
  public void setBlob(final int parameterIndex, final InputStream inputStream, final long length)
      throws SQLException {
    preparedStatement.setBlob(parameterIndex, inputStream, length);
    params.put(idx2key(parameterIndex), "blob stream[" + length + "]");
  }

  @Override
  public void setNClob(final int parameterIndex, final Reader reader, final long length) throws SQLException {
    preparedStatement.setNClob(parameterIndex, reader, length);
    params.put(idx2key(parameterIndex), "nclob reader[" + length + "]");
  }

  @Override
  public void setSQLXML(final int parameterIndex, final SQLXML xmlObject) throws SQLException {
    preparedStatement.setSQLXML(parameterIndex, xmlObject);
    params.put(idx2key(parameterIndex), xmlObject);
  }

  @Override
  public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scaleOrLength)
      throws SQLException {
    preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    params.put(idx2key(parameterIndex), x);
  }

  @Override
  public void setAsciiStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {
    preparedStatement.setAsciiStream(parameterIndex, x, length);
    params.put(idx2key(parameterIndex), "ascii stream[" + length + "]");
  }

  @Override
  public void setBinaryStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {
    preparedStatement.setBinaryStream(parameterIndex, x, length);
    params.put(idx2key(parameterIndex), "binary stream[" + length + "]");
  }

  @Override
  public void setCharacterStream(final int parameterIndex, final Reader reader, final long length)
      throws SQLException {
    preparedStatement.setCharacterStream(parameterIndex, reader, length);
    params.put(idx2key(parameterIndex), "character reader[" + length + "]");
  }

  @Override
  public void setAsciiStream(final int parameterIndex, final InputStream x) throws SQLException {
    preparedStatement.setAsciiStream(parameterIndex, x);
    params.put(idx2key(parameterIndex), "ascii stream");
  }

  @Override
  public void setBinaryStream(final int parameterIndex, final InputStream x) throws SQLException {
    preparedStatement.setBinaryStream(parameterIndex, x);
    params.put(idx2key(parameterIndex), "binary stream");
  }

  @Override
  public void setCharacterStream(final int parameterIndex, final Reader reader) throws SQLException {
    preparedStatement.setCharacterStream(parameterIndex, reader);
    params.put(idx2key(parameterIndex), "character reader");
  }

  @Override
  public void setNCharacterStream(final int parameterIndex, final Reader value) throws SQLException {
    preparedStatement.setNCharacterStream(parameterIndex, value);
    params.put(idx2key(parameterIndex), "ncharacter reader");
  }

  @Override
  public void setClob(final int parameterIndex, final Reader reader) throws SQLException {
    preparedStatement.setClob(parameterIndex, reader);
    params.put(idx2key(parameterIndex), "clob");
  }

  @Override
  public void setBlob(final int parameterIndex, final InputStream inputStream) throws SQLException {
    preparedStatement.setBlob(parameterIndex, inputStream);
    params.put(idx2key(parameterIndex), "blob");
  }

  @Override
  public void setNClob(final int parameterIndex, final Reader reader) throws SQLException {
    preparedStatement.setNClob(parameterIndex, reader);
    params.put(idx2key(parameterIndex), "nclob");
  }

}
