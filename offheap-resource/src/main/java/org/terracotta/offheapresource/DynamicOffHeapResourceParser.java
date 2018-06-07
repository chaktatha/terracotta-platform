/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.offheapresource;
import org.terracotta.entity.DynamicConfigurationParser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class DynamicOffHeapResourceParser implements DynamicConfigurationParser<DynamicOffHeapResources>{

  @Override
  public byte[] marshallDynamicConfigurationEntity(final DynamicOffHeapResources entity) {
    ByteArrayOutputStream bos =new ByteArrayOutputStream();
    Properties properties = new Properties();
    if (entity != null){
      Map<String, String> propertiesMap = entity.getOffHearResources().stream().collect(Collectors.toMap(this::getPropertiesKey, this::getPropertiesValue));
      properties.putAll(propertiesMap);
    }
    try {
      properties.store(bos, "");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bos.toByteArray();
  }

  @Override
  public Class<DynamicOffHeapResources> getType() {
    return DynamicOffHeapResources.class;
  }

  @Override
  public DynamicOffHeapResources unmarshallDynamicConfigurationEntity(final byte[] configBytes) {
    Properties properties = new Properties();
    try {
      properties.load(new ByteArrayInputStream(configBytes));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    DynamicOffHeapResources offHeapResources = new DynamicOffHeapResources();
    properties.keySet().forEach(offHeapName -> offHeapResources.addOffHearResources(new DynamicOffHeapResource((String)offHeapName,
        extractCapacity(properties.getProperty((String)offHeapName)), getUnit(properties.getProperty((String)offHeapName)))));
    return offHeapResources;
  }

  protected long extractCapacity(String value){
    String[] values = value.split("\\s+");
    if(values.length < 1){
      throw new IllegalArgumentException("Offheap Value Missing");
    }
    return Long.parseLong(values[0]);
  }

  protected String getUnit(String value){
    String[] values = value.split("\\s+");
    if(values.length < 2){
      return "kB";
    }
    if("KB".equalsIgnoreCase(values[1])) {
      return "kB";
    }
    return values[1];
  }

  protected String convertUnit(String unitString){
    if("b".equalsIgnoreCase(unitString)) {
      return "B";
    }
    if ("k".equalsIgnoreCase(unitString) || "kb".equalsIgnoreCase(unitString)) {
      return "KB";
    } if ("m".equalsIgnoreCase(unitString) || "mb".equalsIgnoreCase(unitString)) {
      return "MB";
    } if("g".equalsIgnoreCase(unitString) || "gb".equalsIgnoreCase(unitString)) {
      return "GB";
    } if ("t".equalsIgnoreCase(unitString) || "tb".equalsIgnoreCase(unitString)) {
      return "TB";
    }
    return unitString;
  }

  protected String getPropertiesKey(DynamicOffHeapResource offHeapResource) {
    return offHeapResource.getName();
  }

  protected String getPropertiesValue(DynamicOffHeapResource offHeapResource) {
    return offHeapResource.getCapacity() + " " + convertUnit(offHeapResource.getUnit());
  }
}
