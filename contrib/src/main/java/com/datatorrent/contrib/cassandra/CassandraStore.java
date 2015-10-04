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

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.lib.db.Connectable;

/**
 * A {@link Connectable} that uses cassandra to connect to stores and implements Connectable interface.
 * <p>
 * @displayName Cassandra Store
 * @category Output
 * @tags cassandra
 * @since 1.0.2
 */
public class CassandraStore implements Connectable
{
  protected static final Logger logger = LoggerFactory.getLogger(CassandraStore.class);
  private String userName;
  private String password;
  @NotNull
  private String node;
  protected transient Cluster cluster = null;
  protected transient Session session = null;

  @NotNull
  protected String keyspace;

  /*
   * The Cassandra keyspace is a namespace that defines how data is replicated on nodes.
   * Typically, a cluster has one keyspace per application. Replication is controlled on a per-keyspace basis, so data that has different replication requirements typically resides in different keyspaces.
   * Keyspaces are not designed to be used as a significant map layer within the data model. Keyspaces are designed to control data replication for a set of tables.
   */
  public String getKeyspace()
  {
    return keyspace;
  }

  /**
   * Sets the keyspace.
   *
   * @param keyspace keyspace.
   */
  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  /**
   * Sets the user name.
   *
   * @param userName user name.
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * Sets the password.
   *
   * @param password password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  @NotNull
  public String getNode() {
    return node;
  }

  /**
   * Sets the node.
   *
   * @param node node
   */
  public void setNode(@NotNull String node) {
    this.node = node;
  }

  public Cluster getCluster() {
    return cluster;
  }

  public Session getSession() {
    return session;
  }

  /**
   * Creates a cluster object.
   */
  public void buildCluster(){

    try {

      cluster = Cluster.builder()
          .addContactPoint(node).withCredentials(userName, password).build();
    }
    catch (DriverException ex) {
      throw new RuntimeException("closing database resource", ex);
    }
    catch (Throwable t) {
      DTThrowable.rethrow(t);
    }
  }


  /**
   * Create connection with database.
   */
  @Override
  public void connect()
  {
    try {
      if(cluster==null)
        buildCluster();
      session = cluster.connect();
      logger.debug("Cassandra connection Success");
    }
    catch (DriverException ex) {
      throw new RuntimeException("closing database resource", ex);
    }
    catch (Throwable t) {
      DTThrowable.rethrow(t);
    }
  }


  /**
   * Close connection.
   */
  @Override
  public void disconnect()
  {
    try {
      session.close();
      cluster.close();
    }
    catch (DriverException ex) {
      throw new RuntimeException("closing database resource", ex);
    }
    catch (Throwable t) {
      DTThrowable.rethrow(t);
    }
  }

  @Override
  public boolean isConnected()
  {
    try {
      return !session.isClosed();
    }
    catch (DriverException ex) {
      throw new RuntimeException("closing database resource", ex);
    }
  }
}
