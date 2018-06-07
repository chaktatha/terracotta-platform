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

public class DynamicOffHeapResource {
  private final String name;
  private final long capacity;
  private String unit;

  public DynamicOffHeapResource(final String name, final long capacity, String unit) {
    this.name = name;
    this.capacity = capacity;
    this.unit = unit;
  }

  public String getName() {
    return name;
  }

  public long getCapacity() {
    return capacity;
  }

  public String getUnit() {
    return unit;
  }

  public long getCapacityInBytes(){
    if("B".equalsIgnoreCase(unit)) {
      return capacity;
    } else if ("MB".equalsIgnoreCase(unit) || "M".equalsIgnoreCase(unit)){
      return capacity << 20;
    } else if("KB".equalsIgnoreCase(unit) || "K".equalsIgnoreCase(unit)) {
      return capacity << 10;
    } else if("GB".equalsIgnoreCase(unit) || "G".equalsIgnoreCase(unit)) {
      return capacity << 30;
    } else if("TB".equalsIgnoreCase(unit) || "T".equalsIgnoreCase(unit)) {
      return capacity << 40;
    }
    throw new IllegalArgumentException("Unsupported memory unit "+unit);
  }
}