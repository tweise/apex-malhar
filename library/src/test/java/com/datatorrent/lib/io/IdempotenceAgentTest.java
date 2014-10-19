/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.io;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.*;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class IdempotenceAgentTest
{
  public static final int OPERATOR_ID_A = 0;
  public static final int OPERATOR_ID_B = 1;
  public static final int OPERATOR_ID_C = 2;

  public static final String APPLICATION_ID = "1";

  public static NoIdempotenceAgent<Integer> agentA;
  public static NoIdempotenceAgent<Integer> agentB;
  public static NoIdempotenceAgent<Integer> agentC;
  public static OperatorContextTestHelper.TestIdOperatorContext contextA;
  public static OperatorContextTestHelper.TestIdOperatorContext contextB;
  public static OperatorContextTestHelper.TestIdOperatorContext contextC;

  public static Set<Integer> allAgentIds = new HashSet<Integer>();

    @Rule
  public PrivateTestWatcher testWatcher = new PrivateTestWatcher();

  public static class PrivateTestWatcher extends TestWatcher
  {
    @Override
    protected void starting(Description description)
    {
      agentA = new NoIdempotenceAgent<Integer>();
      agentB = new NoIdempotenceAgent<Integer>();
      agentC = new NoIdempotenceAgent<Integer>();

      AttributeMap attributeMapA = new DefaultAttributeMap();
      attributeMapA.put(DAG.APPLICATION_ID, APPLICATION_ID);
      AttributeMap attributeMapB = new DefaultAttributeMap();
      attributeMapB.put(DAG.APPLICATION_ID, APPLICATION_ID);
      AttributeMap attributeMapC = new DefaultAttributeMap();
      attributeMapC.put(DAG.APPLICATION_ID, APPLICATION_ID);

      contextA = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID_A,
                                                                     attributeMapA);
      contextB = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID_B,
                                                                     attributeMapB);
      contextC = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID_C,
                                                                     attributeMapC);

      allAgentIds = new HashSet<Integer>();
      allAgentIds.add(OPERATOR_ID_A);
      allAgentIds.add(OPERATOR_ID_B);
      allAgentIds.add(OPERATOR_ID_C);
    }
  }

  private Collection<IdempotenceAgent<Integer>> createRepartitionAgents()
  {
    agentA.setup(contextA);
    agentA.teardown();

    agentB.setup(contextB);
    agentB.teardown();

    agentC.setup(contextC);
    agentC.teardown();

    Collection<IdempotenceAgent<Integer>> agents = Lists.newArrayList();
    agents.add((IdempotenceAgent<Integer>) agentA);
    agents.add((IdempotenceAgent<Integer>) agentB);
    agents.add((IdempotenceAgent<Integer>) agentC);

    return agents;
  }

  @Test
  public void testRepartitionEvenNonPositive()
  {
    Collection<IdempotenceAgent<Integer>> agents = createRepartitionAgents();

    boolean threwException = false;

    try {
      IdempotenceAgent.repartitionEven(agents,
                                       0);
    }
    catch(IllegalArgumentException e) {
      threwException = true;
    }

    Assert.assertTrue(threwException);
  }

  @Test
  public void testRepartitionEvenTwo()
  {
    Collection<IdempotenceAgent<Integer>> agents = createRepartitionAgents();

    List<IdempotenceAgent<Integer>> newAgents = Lists.newArrayList();
    newAgents.addAll(IdempotenceAgent.repartitionEven(agents,
                                                      2));

    IdempotenceAgent<Integer> agent1 = newAgents.get(0);
    IdempotenceAgent<Integer> agent2 = newAgents.get(1);

    Set<Integer> firstAgentRecoveryIds = Sets.newHashSet(0, 2);
    Set<Integer> secondAgentRecoveryIds = Sets.newHashSet(1);

    Assert.assertEquals((Integer) 0, agent1.getIdempotentAgentId());
    Assert.assertEquals(firstAgentRecoveryIds, agent1.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent1.getAllAgentIds());

    Assert.assertEquals((Integer) 1, agent2.getIdempotentAgentId());
    Assert.assertEquals(secondAgentRecoveryIds, agent2.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent2.getAllAgentIds());
  }

  @Test
  public void testRepartitionEvenFour()
  {
    Collection<IdempotenceAgent<Integer>> agents = createRepartitionAgents();

    List<IdempotenceAgent<Integer>> newAgents = Lists.newArrayList();
    newAgents.addAll(IdempotenceAgent.repartitionEven(agents,
                                                      4));

    IdempotenceAgent<Integer> agent1 = newAgents.get(0);
    IdempotenceAgent<Integer> agent2 = newAgents.get(1);
    IdempotenceAgent<Integer> agent3 = newAgents.get(2);
    IdempotenceAgent<Integer> agent4 = newAgents.get(3);

    Assert.assertEquals((Integer) 0, agent1.getIdempotentAgentId());
    Assert.assertEquals(Sets.newHashSet(0), agent1.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent1.getAllAgentIds());
    Assert.assertEquals((Integer) 1, agent2.getIdempotentAgentId());
    Assert.assertEquals(Sets.newHashSet(1), agent2.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent2.getAllAgentIds());
    Assert.assertEquals((Integer) 2, agent3.getIdempotentAgentId());
    Assert.assertEquals(Sets.newHashSet(2), agent3.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent3.getAllAgentIds());
    Assert.assertEquals(null, agent4.getIdempotentAgentId());
    Assert.assertEquals(Sets.newHashSet(), agent4.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent4.getAllAgentIds());
  }

  @Test
  public void testRepartitionAllNonPositive()
  {
    Collection<IdempotenceAgent<Integer>> agents = createRepartitionAgents();

    boolean threwException = false;

    try {
      IdempotenceAgent.repartitionAll(agents,
                                      0);
    }
    catch(IllegalArgumentException e) {
      threwException = true;
    }

    Assert.assertTrue(threwException);
  }

  @Test
  public void testRepartitionAllTwo()
  {
    Collection<IdempotenceAgent<Integer>> agents = createRepartitionAgents();

    List<IdempotenceAgent<Integer>> newAgents = Lists.newArrayList();
    newAgents.addAll(IdempotenceAgent.repartitionAll(agents,
                                                     2));

    IdempotenceAgent<Integer> agent1 = newAgents.get(0);
    IdempotenceAgent<Integer> agent2 = newAgents.get(1);

    Assert.assertEquals((Integer) 0, agent1.getIdempotentAgentId());
    Assert.assertEquals(allAgentIds, agent1.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent1.getAllAgentIds());

    Assert.assertEquals((Integer) 1, agent2.getIdempotentAgentId());
    Assert.assertEquals(allAgentIds, agent2.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent2.getAllAgentIds());
  }

  @Test
  public void testRepartitionAllFour()
  {
    Collection<IdempotenceAgent<Integer>> agents = createRepartitionAgents();

    List<IdempotenceAgent<Integer>> newAgents = Lists.newArrayList();
    newAgents.addAll(IdempotenceAgent.repartitionAll(agents,
                                                     4));

    IdempotenceAgent<Integer> agent1 = newAgents.get(0);
    IdempotenceAgent<Integer> agent2 = newAgents.get(1);
    IdempotenceAgent<Integer> agent3 = newAgents.get(2);
    IdempotenceAgent<Integer> agent4 = newAgents.get(3);

    Assert.assertEquals((Integer) 0, agent1.getIdempotentAgentId());
    Assert.assertEquals(allAgentIds, agent1.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent1.getAllAgentIds());
    Assert.assertEquals((Integer) 1, agent2.getIdempotentAgentId());
    Assert.assertEquals(allAgentIds, agent2.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent2.getAllAgentIds());
    Assert.assertEquals((Integer) 2, agent3.getIdempotentAgentId());
    Assert.assertEquals(allAgentIds, agent3.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent3.getAllAgentIds());
    Assert.assertEquals(null, agent4.getIdempotentAgentId());
    Assert.assertEquals(allAgentIds, agent4.getRecoverIdempotentAgentIds());
    Assert.assertEquals(allAgentIds, agent4.getAllAgentIds());
  }
}
