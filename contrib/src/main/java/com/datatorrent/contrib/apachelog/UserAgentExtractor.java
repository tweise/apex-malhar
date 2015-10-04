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
package com.datatorrent.contrib.apachelog;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.lib.logs.InformationExtractor;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.service.UADetectorServiceFactory;
import net.sf.uadetector.UserAgentStringParser;

/**
 * An implementation of Information Extractor that extracts the browser and the OS from a user-agent string.
 * <p>
 * The user of this extractor needs to include the jars which contain these classes in DAGContext.LIBRARY_JARS
 *
 * net.sf.uadetector.UserAgentStringParser.class
 * net.sf.uadetector.service.UADetectorServiceFactory.class
 * net.sf.qualitycheck.Check.class
 * @displayName User Agent Extractor
 * @category Output
 * @tags cache, extraction
 * @since 0.9.4
 */
public class UserAgentExtractor implements InformationExtractor
{
  private static final long serialVersionUID = 201404221817L;

  public static class CachedUserAgentStringParser implements UserAgentStringParser
  {
    private final UserAgentStringParser parser = UADetectorServiceFactory.getCachingAndUpdatingParser();
    private final Cache<String, ReadableUserAgent> cache = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(2, TimeUnit.HOURS).build();

    @Override
    public String getDataVersion()
    {
      return parser.getDataVersion();
    }

    @Override
    public ReadableUserAgent parse(final String userAgentString)
    {
      ReadableUserAgent result = cache.getIfPresent(userAgentString);
      if (result == null) {
        result = parser.parse(userAgentString);
        cache.put(userAgentString, result);
      }
      return result;
    }

    @Override
    public void shutdown()
    {
      parser.shutdown();
    }

  }

  private transient UserAgentStringParser parser;

  @Override
  public void setup()
  {
    parser = new CachedUserAgentStringParser();
  }

  @Override
  public void teardown()
  {
    parser.shutdown();
  }

  @Override
  public Map<String, Object> extractInformation(Object value)
  {
    Map<String, Object> m = new HashMap<String, Object>();
    ReadableUserAgent agent = parser.parse(value.toString());
    m.put("browser", agent.getName());
    m.put("os", agent.getOperatingSystem().getName());
    return m;
  }

}
