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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapresource.management.OffHeapResourceBinding;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * An implementation of {@link OffHeapResource}.
 */
class OffHeapResourceImpl implements OffHeapResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapResourceImpl.class);

  private static final String MESSAGE_PROPERTIES_RESOURCE_NAME = "/offheap-message.properties";
  private static final String OFFHEAP_INFO_KEY = "offheap.info";
  private static final String OFFHEAP_WARN_KEY = "offheap.warn";
  private static final String DEFAULT_MESSAGE = "Offheap allocation for resource \"{}\" reached {}%, you may run out of memory if allocation continues.";
  private static final Properties MESSAGE_PROPERTIES;

  static {
    Properties defaults = new Properties();
    defaults.setProperty(OFFHEAP_INFO_KEY, DEFAULT_MESSAGE);
    defaults.setProperty(OFFHEAP_WARN_KEY, DEFAULT_MESSAGE);
    MESSAGE_PROPERTIES = new Properties(defaults);
    boolean loaded = false;
    try {
      InputStream resource = OffHeapResourceImpl.class.getResourceAsStream(MESSAGE_PROPERTIES_RESOURCE_NAME);
      if (resource != null) {
        MESSAGE_PROPERTIES.load(resource);
        loaded = true;
      }
    } catch (IOException e) {
      LOGGER.debug("Exception loading {}", MESSAGE_PROPERTIES_RESOURCE_NAME, e);
    } finally {
      if (!loaded) {
        LOGGER.info("Unable to load {}, will be using default messages.", MESSAGE_PROPERTIES_RESOURCE_NAME);
      }

    }
  }

  private final String identifier;
  private final BiConsumer<OffHeapResourceImpl, ThresholdChange> onReservationThresholdReached;
  private final OffHeapResourceBinding managementBinding;

  private final AtomicReference<OffHeapStorageCapacity> storageCapacityRef;

  /**
   * Creates a resource of the given initial size.
   *
   *
   * @param identifier
   * @param size size of the resource
   * @throws IllegalArgumentException if the size is negative
   */
  OffHeapResourceImpl(String identifier, long size, BiConsumer<OffHeapResourceImpl, ThresholdChange> onReservationThresholdReached) throws IllegalArgumentException {
    this.onReservationThresholdReached = onReservationThresholdReached;
    this.managementBinding = new OffHeapResourceBinding(identifier, this);
    if (size < 0) {
      throw new IllegalArgumentException("Resource size cannot be negative");
    } else {
      this.storageCapacityRef = new AtomicReference<>(new OffHeapStorageCapacity(size));
      this.identifier = identifier;
    }
  }

  OffHeapResourceImpl(String identifier, long size) throws IllegalArgumentException {
    this(identifier, size, (r, p) -> {});
  }

  public OffHeapResourceBinding getManagementBinding() {
    return managementBinding;
  }

  /**
   * {@inheritDoc}
   * @throws IllegalArgumentException {@inheritDoc}
   */
  @Override
  public boolean reserve(long size) throws IllegalArgumentException {
    if (size < 0) {
      throw new IllegalArgumentException("Reservation size cannot be negative");
    }
    while(true) {
      OffHeapStorageCapacity currentCapacityStore = this.storageCapacityRef.get();
      long remaining = currentCapacityStore.remaining;
      if (remaining >= size) {
        long currentCapacity = currentCapacityStore.capacity;
        OffHeapStorageCapacity newCapacityStore = new OffHeapStorageCapacity(currentCapacity, (remaining - size));
        if (this.storageCapacityRef.compareAndSet(currentCapacityStore, newCapacityStore)){
          int newThreshold = newCapacityStore.threshold;
          onReservationThresholdReached.accept(this, new ThresholdChange(currentCapacityStore.threshold, newThreshold));
          if (newThreshold == 90) {
            LOGGER.warn(MESSAGE_PROPERTIES.getProperty(OFFHEAP_WARN_KEY), identifier, newCapacityStore.percentOccupied);
          } else {
            LOGGER.info(MESSAGE_PROPERTIES.getProperty(OFFHEAP_INFO_KEY), identifier, newCapacityStore.percentOccupied);
          }
          return true;
        }
      } else {
        break;
      }
    }
    return false;

  }

  /**
   * {@inheritDoc}
   * @throws IllegalArgumentException {@inheritDoc}
   */
  @Override
  public void release(long size) throws IllegalArgumentException {
    if (size < 0) {
      throw new IllegalArgumentException("Reservation size cannot be negative");
    }
    while(true) {
      OffHeapStorageCapacity currentCapacityStore = this.storageCapacityRef.get();
      long remaining = currentCapacityStore.remaining;
      long currentCapacity = currentCapacityStore.capacity;
      if (remaining + size < currentCapacity) {
        OffHeapStorageCapacity newCapacityStore = new OffHeapStorageCapacity(currentCapacity, (remaining + size));
        if (this.storageCapacityRef.compareAndSet(currentCapacityStore, newCapacityStore)){
          int newThreshold = newCapacityStore.threshold;
          onReservationThresholdReached.accept(this, new ThresholdChange(currentCapacityStore.threshold, newThreshold));
          if (newThreshold == 75) {
            LOGGER.info(MESSAGE_PROPERTIES.getProperty(OFFHEAP_INFO_KEY), identifier, newCapacityStore.percentOccupied);
          }
        }
      } else {
        break;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long available() {
    return this.storageCapacityRef.get().remaining;
  }

  @Override
  public long capacity() {
    return this.storageCapacityRef.get().capacity;
  }

  @Override
  public void alterCapacity(final long size) throws IllegalArgumentException {
    if (size < 0) {
      throw new IllegalArgumentException("Capacity increase size cannot be negative");
    }
    while (true) {
      OffHeapStorageCapacity currentCapacityStore = this.storageCapacityRef.get();
      long currentCapacity = currentCapacityStore.capacity;
      long currentRemainingCapacity = currentCapacityStore.remaining;
      if(currentCapacity == size) {
        break;
      }
      if (currentCapacity > size) {
        throw new IllegalArgumentException("Desired new capacity "+size+" is less than the current capacity of "+currentCapacity);
      }
      long newRemainingCapacity = currentRemainingCapacity + (size - currentCapacity);
      OffHeapStorageCapacity newCapacity = new OffHeapStorageCapacity(size, newRemainingCapacity);
      if(this.storageCapacityRef.compareAndSet(currentCapacityStore, newCapacity)){
        int newThreshold = newCapacity.threshold;
        onReservationThresholdReached.accept(this, new ThresholdChange(currentCapacityStore.threshold, newThreshold));
        if (newThreshold == 75) {
          LOGGER.info(MESSAGE_PROPERTIES.getProperty(OFFHEAP_INFO_KEY), identifier, newCapacity.percentOccupied);
        }
        break;
      }
    }
  }

  static class ThresholdChange {
    final int old;
    final int now;

    ThresholdChange(int old, int now) {
      this.old = old;
      this.now = now;
    }
  }

  static final class OffHeapStorageCapacity {
    private final long capacity;
    private final long remaining;
    private final int threshold;
    private final long percentOccupied;

    OffHeapStorageCapacity(long capacity){
      this.capacity = capacity;
      this.remaining = capacity;
      long percentOccupied;
      if (capacity == 0) {
        percentOccupied = 100L;
      } else {
        percentOccupied = (this.capacity - this.remaining)*100/capacity;
      }
      int newT;
      if (percentOccupied == 100) {
        newT = 100;
      } else if (percentOccupied >= 90) {
        newT = 90;
      } else if (percentOccupied >= 75) {
        newT = 75;
      } else {
        newT = 0;
      }
      this.percentOccupied = percentOccupied;
      this.threshold = newT;
    }

    OffHeapStorageCapacity(long capacity, long remaining){
      this.capacity = capacity;
      this.remaining = remaining;
      long percentOccupied;
      if (capacity == 0) {
        percentOccupied = 100L;
      } else {
        percentOccupied = (this.capacity - this.remaining)*100/capacity;
      }

      int newT;
      if (percentOccupied == 100) {
        newT = 100;
      } else if (percentOccupied >= 90) {
        newT = 90;
      } else if (percentOccupied >= 75) {
        newT = 75;
      } else {
        newT = 0;
      }
      this.percentOccupied = percentOccupied;
      this.threshold = newT;
    }

  }
}
