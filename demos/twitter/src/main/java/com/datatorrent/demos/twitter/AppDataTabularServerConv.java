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

package com.datatorrent.demos.twitter;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.SchemaTabular;
import com.datatorrent.lib.converter.Converter;

import java.util.Map;


public class AppDataTabularServerConv<CONVERTER extends Converter<Map<String, Object>, GPOMutable, SchemaTabular>> extends AppDataTabularServer
{
  protected CONVERTER converter;

  public AppDataTabularServerConv()
  {
  }

  public void setConverter(CONVERTER converter)
  {
    this.converter = converter;
  }

  public CONVERTER getConverter()
  {
    return converter;
  }

  @Override
  public GPOMutable convert(Map<String, Object> inputEvent)
  {
    return converter.convert(inputEvent, schema);
  }
}
