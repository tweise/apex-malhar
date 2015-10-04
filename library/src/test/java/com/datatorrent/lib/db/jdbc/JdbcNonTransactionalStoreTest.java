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
package com.datatorrent.lib.db.jdbc;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link JdbcNonTransactionalStore}
 */
public class JdbcNonTransactionalStoreTest
{
  @Test
  public void beginTransactionTest()
  {
    JdbcNonTransactionalStore jdbcNonTransactionalStore = new JdbcNonTransactionalStore();
    try {
      jdbcNonTransactionalStore.beginTransaction();
    }
    catch(RuntimeException e) {
      return;
    }
    Assert.fail("Exception should be thrown");
  }

  @Test
  public void commitTransactionTest()
  {
    JdbcNonTransactionalStore jdbcNonTransactionalStore = new JdbcNonTransactionalStore();
    try {
      jdbcNonTransactionalStore.commitTransaction();
    }
    catch(RuntimeException e) {
      return;
    }
    Assert.fail("Exception should be thrown");
  }

  @Test
  public void rollbackTransactionTest()
  {
    JdbcNonTransactionalStore jdbcNonTransactionalStore = new JdbcNonTransactionalStore();
    try {
      jdbcNonTransactionalStore.rollbackTransaction();
    }
    catch(RuntimeException e) {
      return;
    }
    Assert.fail("Exception should be thrown");
  }

  @Test
  public void isInTransactionTest()
  {
    JdbcNonTransactionalStore jdbcNonTransactionalStore = new JdbcNonTransactionalStore();
    try {
      jdbcNonTransactionalStore.isInTransaction();
    }
    catch(RuntimeException e) {
      return;
    }
    Assert.fail("Exception should be thrown");
  }
}
