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

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.lib.appbuilder.convert.pojo.PojoFieldRetrieverExpression;
import com.datatorrent.lib.appdata.dimensions.DimensionsComputationSingleSchemaPOJO;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.datatorrent.demos.dimensions.ads.generic.AdsDimensionsDemo.EVENT_SCHEMA;

public class DimensionsComputationSingleSchemaTest
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsComputationSingleSchemaTest.class);

  @Test
  public void test()
  {
    DimensionsComputationSingleSchemaPOJO dimensions = new DimensionsComputationSingleSchemaPOJO();

    //Set input properties
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);

    //Set dimensions properties
    PojoFieldRetrieverExpression pfre = new PojoFieldRetrieverExpression();
    pfre.setFQClassName(AdInfo.class.getName());
    Map<String, String> fieldToExpression = Maps.newHashMap();
    fieldToExpression.put("publisher", "getPublisher()");
    fieldToExpression.put("advertiser", "getAdvertiser()");
    fieldToExpression.put("location", "getLocation()");
    fieldToExpression.put("cost", "getCost()");
    fieldToExpression.put("revenue", "getRevenue()");
    fieldToExpression.put("impressions", "getImpressions()");
    fieldToExpression.put("clicks", "getClicks()");
    fieldToExpression.put("time", "getTime()");
    pfre.setFieldToExpression(fieldToExpression);
    dimensions.getConverter().setPojoFieldRetriever(pfre);
    dimensions.setEventSchemaJSON(eventSchema);

    logger.debug("{}", dimensions.getAggregatorInfo());
  }
}
