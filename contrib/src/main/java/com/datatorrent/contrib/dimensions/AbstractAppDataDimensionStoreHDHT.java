/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import com.datatorrent.api.AppData;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.appdata.query.QueryExecutor;
import com.datatorrent.lib.appdata.query.QueryManagerAsynchronous;
import com.datatorrent.lib.appdata.query.SimpleQueueManager;
import com.datatorrent.lib.appdata.query.serde.MessageDeserializerFactory;
import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.Result;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.appdata.schemas.SchemaResult;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAppDataDimensionStoreHDHT extends DimensionsStoreHDHT
{
  @NotNull
  protected ResultFormatter resultFormatter = new ResultFormatter();
  @NotNull
  protected AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;

  protected transient SimpleQueueManager<SchemaQuery, Void, Void> schemaQueueManager;
  protected transient DimensionsQueueManager dimensionsQueueManager;
  protected transient QueryManagerAsynchronous<SchemaQuery, Void, Void, SchemaResult> schemaProcessor;
  protected transient QueryManagerAsynchronous<DataQueryDimensional, QueryMeta, MutableLong, Result> queryProcessor;
  protected final transient MessageDeserializerFactory queryDeserializerFactory;

  @VisibleForTesting
  public SchemaRegistry schemaRegistry;
  protected transient MessageSerializerFactory resultSerializerFactory;

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @InputPortFieldAnnotation(optional = true)
  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      LOG.debug("Received {}", s);
      Message query;
      try {
        query = queryDeserializerFactory.deserialize(s);
      }
      catch (IOException ex) {
        LOG.error("error parsing query {}", s, ex);
        return;
      }

      if (query instanceof SchemaQuery) {
        schemaQueueManager.enqueue((SchemaQuery) query, null, null);
      }
      else if (query instanceof DataQueryDimensional) {
        processDimensionalDataQuery((DataQueryDimensional) query);
      }
      else {
        LOG.error("Invalid query {}", s);
      }
    }
  };

  @SuppressWarnings("unchecked")
  public AbstractAppDataDimensionStoreHDHT()
  {
    queryDeserializerFactory = new MessageDeserializerFactory(SchemaQuery.class, DataQueryDimensional.class);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    aggregatorRegistry.setup();

    schemaRegistry = getSchemaRegistry();

    resultSerializerFactory = new MessageSerializerFactory(resultFormatter);
    queryDeserializerFactory.setContext(DataQueryDimensional.class, schemaRegistry);

    dimensionsQueueManager = new DimensionsQueueManager(this, schemaRegistry);
    queryProcessor =
    new QueryManagerAsynchronous<DataQueryDimensional, QueryMeta, MutableLong, Result>(queryResult,
                                                                                       dimensionsQueueManager,
                                                                                       new DimensionsQueryExecutor(this, schemaRegistry),
                                                                                       resultSerializerFactory);

    schemaQueueManager = new SimpleQueueManager<SchemaQuery, Void, Void>();
    schemaProcessor = new QueryManagerAsynchronous<SchemaQuery, Void, Void, SchemaResult>(queryResult,
                                                                                          schemaQueueManager,
                                                                                          new SchemaQueryExecutor(),
                                                                                          resultSerializerFactory);


    dimensionsQueueManager.setup(context);
    queryProcessor.setup(context);

    schemaQueueManager.setup(context);
    schemaProcessor.setup(context);
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    schemaQueueManager.beginWindow(windowId);
    schemaProcessor.beginWindow(windowId);

    dimensionsQueueManager.beginWindow(windowId);
    queryProcessor.beginWindow(windowId);

    super.beginWindow(windowId);
  }

  protected void processDimensionalDataQuery(DataQueryDimensional dataQueryDimensional)
  {
    dimensionsQueueManager.enqueue(dataQueryDimensional, null, null);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();

    queryProcessor.endWindow();
    dimensionsQueueManager.endWindow();

    schemaProcessor.endWindow();
    schemaQueueManager.endWindow();
  }

  @Override
  public void teardown()
  {
    queryProcessor.teardown();
    dimensionsQueueManager.teardown();

    schemaProcessor.teardown();
    schemaQueueManager.teardown();

    super.teardown();
  }

  /**
   * Processes schema queries.
   * @param schemaQuery a schema query
   * @return The corresponding schema result.
   */
  protected abstract SchemaResult processSchemaQuery(SchemaQuery schemaQuery);

  /**
   * @return the schema registry
   */
  protected abstract SchemaRegistry getSchemaRegistry();

  @Override
  public IncrementalAggregator getAggregator(int aggregatorID)
  {
    return aggregatorRegistry.getIncrementalAggregatorIDToAggregator().get(aggregatorID);
  }

  @Override
  protected int getAggregatorID(String aggregatorName)
  {
    return aggregatorRegistry.getIncrementalAggregatorNameToID().get(aggregatorName);
  }

  public void setAppDataFormatter(ResultFormatter resultFormatter)
  {
    this.resultFormatter = resultFormatter;
  }

  /**
   * @return the resultFormatter
   */
  public ResultFormatter getAppDataFormatter()
  {
    return resultFormatter;
  }

  /**
   * @return the aggregatorRegistry
   */
  public AggregatorRegistry getAggregatorRegistry()
  {
    return aggregatorRegistry;
  }

  /**
   * @param aggregatorRegistry the aggregatorRegistry to set
   */
  public void setAggregatorRegistry(@NotNull AggregatorRegistry aggregatorRegistry)
  {
    this.aggregatorRegistry = aggregatorRegistry;
  }

  public class SchemaQueryExecutor implements QueryExecutor<SchemaQuery, Void, Void, SchemaResult>
  {
    public SchemaQueryExecutor()
    {
    }

    @Override
    public SchemaResult executeQuery(SchemaQuery query, Void metaQuery, Void queueContext)
    {
      return processSchemaQuery(query);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppDataDimensionStoreHDHT.class);
}
