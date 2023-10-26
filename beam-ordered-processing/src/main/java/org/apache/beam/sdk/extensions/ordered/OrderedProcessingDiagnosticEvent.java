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
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

/**
 * Diagnostic event
 */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class OrderedProcessingDiagnosticEvent {

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class QueriedBufferedEvents {

    public static QueriedBufferedEvents create(Instant queryStart, Instant queryEnd,
        Instant firstReturnedEvent) {
      return builder().setQueryStart(queryStart).setQueryEnd(queryEnd)
          .setFirstReturnedEvent(firstReturnedEvent).build();
    }

    public abstract Instant getQueryStart();

    public abstract Instant getQueryEnd();

    @Nullable
    public abstract Instant getFirstReturnedEvent();

    public static Builder builder() {
      return new org.apache.beam.sdk.extensions.ordered.AutoValue_OrderedProcessingDiagnosticEvent_QueriedBufferedEvents.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setQueryStart(Instant value);

      public abstract Builder setQueryEnd(Instant value);

      public abstract Builder setFirstReturnedEvent(Instant value);

      public abstract QueriedBufferedEvents build();
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ClearedBufferedEvents {

    public static ClearedBufferedEvents create(Instant rangeStart, Instant rangeEnd) {
      return builder().setRangeStart(rangeStart).setRangeEnd(rangeEnd).build();
    }

    public abstract Instant getRangeStart();

    public abstract Instant getRangeEnd();

    public static Builder builder() {
      return new AutoValue_OrderedProcessingDiagnosticEvent_ClearedBufferedEvents.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setRangeStart(Instant value);

      public abstract Builder setRangeEnd(Instant value);

      public abstract ClearedBufferedEvents build();
    }
  }

  public static Builder builder() {
    return new AutoValue_OrderedProcessingDiagnosticEvent.Builder();
  }

  @javax.annotation.Nullable
  public abstract Long getSequenceNumber();

  @javax.annotation.Nullable
  public abstract Long getReceivedOrder();

  public abstract Instant getProcessingTime();

  @Nullable
  public abstract Instant getEventBufferedTime();

  @Nullable
  public abstract QueriedBufferedEvents getQueriedBufferedEvents();

  @Nullable
  public abstract ClearedBufferedEvents getClearedBufferedEvents();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSequenceNumber(long value);

    public abstract Builder setReceivedOrder(long value);

    public abstract Builder setProcessingTime(Instant value);

    public abstract Builder setEventBufferedTime(Instant value);

    public abstract Builder setQueriedBufferedEvents(QueriedBufferedEvents value);

    public abstract Builder setClearedBufferedEvents(ClearedBufferedEvents value);

    public abstract OrderedProcessingDiagnosticEvent build();
  }
}
