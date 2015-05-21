/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.dimensions;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@OperatorAnnotation(checkpointableWithinAppWindow=false)
public abstract class DimensionsComputation<INPUT_EVENT> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsComputation.class);

  @Min(0)
  private int aggregationWindowCount = 1;
  private int windowCount = 0;

  @VisibleForTesting
  public Map<EventKey, DimensionsEvent> aggregationBuffer = Maps.newHashMap();
  private transient List<DimensionsEvent> aggregateEventBuffer = Lists.newArrayList();

  @NotNull
  protected AggregatorRegistry aggregatorInfo;

  public transient final DefaultInputPort<INPUT_EVENT> inputEvent = new DefaultInputPort<INPUT_EVENT>() {
    @Override
    public void process(INPUT_EVENT tuple)
    {
      processInputEvent(tuple);
    }
  };

  public final transient DefaultOutputPort<DimensionsEvent> aggregateOutput = new DefaultOutputPort<DimensionsEvent>()
  {
    @Override
    public Unifier<DimensionsEvent> getUnifier()
    {
      return new DimensionsComputation.DimensionsComputationUnifier(getAggregatorInfo());
    }
  };

  public DimensionsComputation()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    aggregatorInfo.setup();
  }

  @Override
  public void beginWindow(long windowId)
  {
    windowCount++;
  }

  @Override
  public void endWindow()
  {
    if(windowCount != aggregationWindowCount ||
       aggregationWindowCount == 0) {
      //This is the case when its not time to dump the buffer yet,
      //or when we are doing no buffering.
      return;
    }

    for(Map.Entry<EventKey, DimensionsEvent> entry: aggregationBuffer.entrySet()) {
      outputAggregateEvent(entry.getValue());
    }

    aggregationBuffer.clear();
    windowCount = 0;
  }

  @Override
  public void teardown()
  {
  }

  public abstract void convertInputEvent(INPUT_EVENT inputEvent, List<DimensionsEvent> aggregateEventBuffer);
  public abstract FieldsDescriptor getAggregateFieldsDescriptor(int schemaID,
                                                                int dimensionDescriptorID,
                                                                int aggregatorID);

  /**
   * @param aggregatorInfo the aggregatorInfo to set
   */
  public void setAggregatorInfo(@NotNull AggregatorRegistry aggregatorInfo)
  {
    this.aggregatorInfo = aggregatorInfo;
  }

  public AggregatorRegistry getAggregatorInfo()
  {
    return aggregatorInfo;
  }

  public void processInputEvent(INPUT_EVENT inputEvent)
  {
    convertInputEvent(inputEvent, aggregateEventBuffer);

    if(aggregationWindowCount == 0) {
      //No Buffering
      for(int index = 0;
          index < aggregateEventBuffer.size();
          index++) {
        processGenericEventNoBuffering(aggregateEventBuffer.get(index));
      }
    }
    else {
      //Buffering
      for(int index = 0;
          index < aggregateEventBuffer.size();
          index++) {
        processGenericEvent(aggregateEventBuffer.get(index));
      }
    }

    aggregateEventBuffer.clear();
  }

  public void processGenericEventNoBuffering(DimensionsEvent gae)
  {
    DimensionsIncrementalAggregator aggregator = getAggregatorInfo().getIncrementalAggregatorIDToAggregator().get(gae.getAggregatorID());
    outputAggregateEvent(aggregator.createDest(gae,
                         getAggregateFieldsDescriptor(gae.getSchemaID(),
                                                      gae.getDimensionDescriptorID(),
                                                      gae.getAggregatorID())));
  }

  public void processGenericEvent(DimensionsEvent gae)
  {
    DimensionsIncrementalAggregator aggregator = getAggregatorInfo().getIncrementalAggregatorIDToAggregator().get(gae.getAggregatorID());
    DimensionsEvent aggregate = aggregationBuffer.get(gae.getEventKey());

    if(aggregate == null) {
      gae = aggregator.createDest(gae,
                                  getAggregateFieldsDescriptor(gae.getSchemaID(),
                                                               gae.getDimensionDescriptorID(),
                                                               gae.getAggregatorID()));
      aggregationBuffer.put(gae.getEventKey(), gae);
      return;
    }

    aggregator.aggregate(aggregate, gae);

    DimensionsEvent newAggregate = new DimensionsEvent(aggregate.getEventKey(),
                                                                   new GPOMutable(aggregate.getAggregates()));
    aggregationBuffer.put(newAggregate.getEventKey(), newAggregate);
  }

  public void outputAggregateEvent(DimensionsEvent event)
  {
    aggregateOutput.emit(event);
  }

  /**
   * @return the aggregationWindowCount
   */
  public int getAggregationWindowCount()
  {
    return aggregationWindowCount;
  }

  /**
   * @param aggregationWindowCount the aggregationWindowCount to set
   */
  public void setAggregationWindowCount(int aggregationWindowCount)
  {
    this.aggregationWindowCount = aggregationWindowCount;
  }

  @OperatorAnnotation(checkpointableWithinAppWindow=false)
  public static class DimensionsComputationUnifier extends BaseOperator implements Operator.Unifier<DimensionsEvent>
  {
    public final transient DefaultOutputPort<DimensionsEvent> output = new DefaultOutputPort<DimensionsEvent>();

    private final Map<EventKey, DimensionsEvent> aggregationBuffer = Maps.newHashMap();
    private AggregatorRegistry aggregatorInfo;

    public DimensionsComputationUnifier()
    {
    }

    public DimensionsComputationUnifier(@NotNull AggregatorRegistry aggregatorInfo)
    {
      setAggregatorInfo(aggregatorInfo);
    }

    private void setAggregatorInfo(AggregatorRegistry aggregatorInfo)
    {
      this.aggregatorInfo = Preconditions.checkNotNull(aggregatorInfo,
                                                       "aggregatorInfo");
    }

    @Override
    public void process(DimensionsEvent srcAgg)
    {
      DimensionsIncrementalAggregator aggregator = aggregatorInfo.getIncrementalAggregatorIDToAggregator().get(srcAgg.getAggregatorID());
      DimensionsEvent destAgg = aggregationBuffer.get(srcAgg.getEventKey());

      if(destAgg == null) {
        aggregationBuffer.put(srcAgg.getEventKey(), srcAgg);
        return;
      }

      aggregator.aggregateAggs(destAgg, srcAgg);
    }

    @Override
    public void setup(OperatorContext context)
    {
      aggregatorInfo.setup();
    }

    @Override
    public void endWindow()
    {
      for(Map.Entry<EventKey, DimensionsEvent> entry: aggregationBuffer.entrySet()) {
        output.emit(entry.getValue());
      }

      aggregationBuffer.clear();
    }
  }
}