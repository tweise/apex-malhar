/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.contrib.cassandra;

import java.math.BigDecimal;
import java.util.*;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.*;

/**
 * <p>
 * CassandraOutputOperator class.</p>
 * A Generic implementation of AbstractCassandraTransactionableOutputOperatorPS which takes in any POJO.
 *
 * @displayName Cassandra Output Operator
 * @category Output
 * @tags database, nosql, pojo, cassandra
 * @since 2.1.0
 */
@Evolving
public class CassandraPOJOOutputOperator extends AbstractCassandraTransactionableOutputOperatorPS<Object> implements Operator.ActivationListener<Context.OperatorContext>
{
  @NotNull
  private List<FieldInfo> fieldInfos;
  @NotNull
  private String tablename;

  protected final transient ArrayList<DataType> columnDataTypes;
  protected final transient ArrayList<Object> getters;
  protected transient Class<?> pojoClass;

  /**
   * The input port on which tuples are received for writing.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object tuple)
    {
      CassandraPOJOOutputOperator.super.input.process(tuple);
    }

  };

  /*
   * Tablename in cassandra.
   */
  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  public CassandraPOJOOutputOperator()
  {
    super();
    columnDataTypes = new ArrayList<DataType>();
    getters = new ArrayList<Object>();
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    com.datastax.driver.core.ResultSet rs = store.getSession().execute("select * from " + store.keyspace + "." + tablename);

    final ColumnDefinitions rsMetaData = rs.getColumnDefinitions();

    final int numberOfColumns = rsMetaData.size();

    for (int i = 0; i < numberOfColumns; i++) {
      // get the designated column's data type.
      final DataType type = rsMetaData.getType(i);
      columnDataTypes.add(type);
      final Object getter;
      final String getterExpr = fieldInfos.get(i).getPojoFieldExpression();
      switch (type.getName()) {
        case ASCII:
        case TEXT:
        case VARCHAR:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, String.class);
          break;
        case BOOLEAN:
          getter = PojoUtils.createGetterBoolean(pojoClass, getterExpr);
          break;
        case INT:
          getter = PojoUtils.createGetterInt(pojoClass, getterExpr);
          break;
        case BIGINT:
        case COUNTER:
          getter = PojoUtils.createGetterLong(pojoClass, getterExpr);
          break;
        case FLOAT:
          getter = PojoUtils.createGetterFloat(pojoClass, getterExpr);
          break;
        case DOUBLE:
          getter = PojoUtils.createGetterDouble(pojoClass, getterExpr);
          break;
        case DECIMAL:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, BigDecimal.class);
          break;
        case SET:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Set.class);
          break;
        case MAP:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Map.class);
          break;
        case LIST:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, List.class);
          break;
        case TIMESTAMP:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Date.class);
          break;
        case UUID:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, UUID.class);
          break;
        default:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Object.class);
          break;
      }
      getters.add(getter);
    }
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  protected PreparedStatement getUpdateCommand()
  {
    StringBuilder queryfields = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (FieldInfo fieldInfo: fieldInfos) {
      if (queryfields.length() == 0) {
        queryfields.append(fieldInfo.getColumnName());
        values.append("?");
      }
      else {
        queryfields.append(",").append(fieldInfo.getColumnName());
        values.append(",").append("?");
      }
    }
    String statement
            = "INSERT INTO " + store.keyspace + "."
            + tablename
            + " (" + queryfields.toString() + ") "
            + "VALUES (" + values.toString() + ");";
    LOG.debug("statement is {}", statement);
    return store.getSession().prepare(statement);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Statement setStatementParameters(PreparedStatement updateCommand, Object tuple) throws DriverException
  {
    final BoundStatement boundStmnt = new BoundStatement(updateCommand);
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final DataType type = columnDataTypes.get(i);
      switch (type.getName()) {
        case UUID:
          final UUID id = ((Getter<Object, UUID>)getters.get(i)).get(tuple);
          boundStmnt.setUUID(i, id);
          break;
        case ASCII:
        case VARCHAR:
        case TEXT:
          final String ascii = ((Getter<Object, String>)getters.get(i)).get(tuple);
          boundStmnt.setString(i, ascii);
          break;
        case BOOLEAN:
          final boolean bool = ((GetterBoolean<Object>)getters.get(i)).get(tuple);
          boundStmnt.setBool(i, bool);
          break;
        case INT:
          final int intValue = ((GetterInt<Object>)getters.get(i)).get(tuple);
          boundStmnt.setInt(i, intValue);
          break;
        case BIGINT:
        case COUNTER:
          final long longValue = ((GetterLong<Object>)getters.get(i)).get(tuple);
          boundStmnt.setLong(i, longValue);
          break;
        case FLOAT:
          final float floatValue = ((GetterFloat<Object>)getters.get(i)).get(tuple);
          boundStmnt.setFloat(i, floatValue);
          break;
        case DOUBLE:
          final double doubleValue = ((GetterDouble<Object>)getters.get(i)).get(tuple);
          boundStmnt.setDouble(i, doubleValue);
          break;
        case DECIMAL:
          final BigDecimal decimal = ((Getter<Object, BigDecimal>)getters.get(i)).get(tuple);
          boundStmnt.setDecimal(i, decimal);
          break;
        case SET:
          Set<?> set = ((Getter<Object, Set<?>>)getters.get(i)).get(tuple);
          boundStmnt.setSet(i, set);
          break;
        case MAP:
          final Map<?,?> map = ((Getter<Object, Map<?,?>>)getters.get(i)).get(tuple);
          boundStmnt.setMap(i, map);
          break;
        case LIST:
          final List<?> list = ((Getter<Object, List<?>>)getters.get(i)).get(tuple);
          boundStmnt.setList(i, list);
          break;
        case TIMESTAMP:
          final Date date = ((Getter<Object, Date>)getters.get(i)).get(tuple);
          boundStmnt.setDate(i, date);
          break;
        default:
          throw new RuntimeException("unsupported data type " + type.getName());
      }
    }
    return boundStmnt;
  }

  /**
   * A list of {@link FieldInfo}s where each item maps a column name to a pojo field name.
   */
  public List<FieldInfo> getFieldInfos()
  {
    return fieldInfos;
  }

  /**
   * Sets the {@link FieldInfo}s. A {@link FieldInfo} maps a store column to a pojo field name.<br/>
   * The value from fieldInfo.column is assigned to fieldInfo.pojoFieldExpression.
   *
   * @description $[].columnName name of the database column name
   * @description $[].pojoFieldExpression pojo field name or expression
   * @useSchema $[].pojoFieldExpression input.fields[].name
   */
  public void setFieldInfos(List<FieldInfo> fieldInfos)
  {
    this.fieldInfos = fieldInfos;
  }

  private static final Logger LOG = LoggerFactory.getLogger(CassandraPOJOOutputOperator.class);
}
