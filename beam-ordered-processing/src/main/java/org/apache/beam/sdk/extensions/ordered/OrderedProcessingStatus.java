/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.ordered;

import com.google.auto.value.AutoValue;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

/**
 * Indicates the status of ordered processing for a particular key.
 */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class OrderedProcessingStatus {

  public static OrderedProcessingStatus create(Long lastOutputSequence,
      long numberOfBufferedEvents,
      Long earliestBufferedSequence,
      Long latestBufferedSequence,
      long numberOfReceivedEvents,
      long resultCount,
      long duplicateCount,
      boolean lastEventReceived) {
    return new AutoValue_OrderedProcessingStatus.Builder()
        .setLastProcessedSequence(lastOutputSequence)
        .setNumberOfBufferedEvents(numberOfBufferedEvents)
        .setEarliestBufferedSequence(earliestBufferedSequence)
        .setLatestBufferedSequence(latestBufferedSequence)
        .setNumberOfReceivedEvents(numberOfReceivedEvents)
        .setLastEventReceived(lastEventReceived)
        .setDuplicateCount(duplicateCount)
        .setResultCount(resultCount)
        .setStatusDate(Instant.now()).build();
  }

  @Nullable
  public abstract Long getLastProcessedSequence();

  public abstract long getNumberOfBufferedEvents();

  @Nullable
  public abstract Long getEarliestBufferedSequence();

  @Nullable
  public abstract Long getLatestBufferedSequence();

  public abstract long getNumberOfReceivedEvents();

  public abstract long getDuplicateCount();

  public abstract long getResultCount();

  public abstract boolean isLastEventReceived();

  public abstract Instant getStatusDate();

  @Override
  public boolean equals(Object obj) {
    if (!OrderedProcessingStatus.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    OrderedProcessingStatus that = (OrderedProcessingStatus) obj;
    boolean result =
        Objects.equals(this.getEarliestBufferedSequence(), that.getEarliestBufferedSequence())
            && Objects.equals(this.getLastProcessedSequence(), that.getLastProcessedSequence())
            && Objects.equals(this.getLatestBufferedSequence(), that.getLatestBufferedSequence())
            && Objects.equals(this.getNumberOfBufferedEvents(), that.getNumberOfBufferedEvents())
            && Objects.equals(this.getDuplicateCount(), that.getDuplicateCount())
            && Objects.equals(this.getResultCount(), that.getResultCount())
            && this.getNumberOfReceivedEvents() == that.getNumberOfReceivedEvents();
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getEarliestBufferedSequence(), this.getLastProcessedSequence(),
        this.getLatestBufferedSequence(), this.getNumberOfBufferedEvents(),
        this.getNumberOfReceivedEvents(), this.getDuplicateCount(), this.getResultCount());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setLastProcessedSequence(Long value);

    public abstract Builder setNumberOfBufferedEvents(long value);

    public abstract Builder setEarliestBufferedSequence(Long value);

    public abstract Builder setLatestBufferedSequence(Long value);

    public abstract Builder setNumberOfReceivedEvents(long value);

    public abstract Builder setDuplicateCount(long value);

    public abstract Builder setResultCount(long value);

    public abstract Builder setLastEventReceived(boolean value);

    public abstract Builder setStatusDate(Instant value);

    public abstract OrderedProcessingStatus build();
  }
}
