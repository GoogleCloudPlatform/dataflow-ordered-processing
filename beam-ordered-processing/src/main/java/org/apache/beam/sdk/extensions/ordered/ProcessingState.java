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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

/**
 * Class used to store the status of processing for a particular key.
 *
 * @param <KeyT>
 */
class ProcessingState<KeyT> {

  private Long lastOutputSequence;
  private Long latestBufferedSequence;
  private Long earliestBufferedSequence;
  private long bufferedRecordCount;

  private boolean lastEventReceived;

  private long recordsReceived = 0;

  private KeyT key;

  public ProcessingState(KeyT key) {
    this.key = key;
    this.bufferedRecordCount = 0;
  }

  /**
   * Only to be used by the coder
   *
   * @param key
   * @param lastOutputSequence
   * @param earliestBufferedSequence
   * @param latestBufferedSequence
   * @param bufferedRecordCount
   */
  ProcessingState(KeyT key, Long lastOutputSequence, Long earliestBufferedSequence,
      Long latestBufferedSequence, long bufferedRecordCount, long recordsReceived,
      boolean allEventsProcessed) {
    this(key);
    this.lastOutputSequence = lastOutputSequence;
    this.earliestBufferedSequence = earliestBufferedSequence;
    this.latestBufferedSequence = latestBufferedSequence;
    this.bufferedRecordCount = bufferedRecordCount;
    this.recordsReceived = recordsReceived;
    this.lastEventReceived = allEventsProcessed;
  }

  public Long getLastOutputSequence() {
    return lastOutputSequence;
  }

  public Long getLatestBufferedSequence() {
    return latestBufferedSequence;
  }

  public Long getEarliestBufferedSequence() {
    return earliestBufferedSequence;
  }

  public long getBufferedRecordCount() {
    return bufferedRecordCount;
  }

  public long getRecordsReceived() {
    return recordsReceived;
  }

  public boolean isLastEventReceived() {
    return lastEventReceived;
  }

  public KeyT getKey() {
    return key;
  }

  /**
   * Current event matched the sequence and was processed.
   *
   * @param sequence
   * @param lastEvent
   */
  public void eventAccepted(long sequence, boolean lastEvent) {
    this.lastOutputSequence = sequence;
    setLastEventReceived(lastEvent);
  }

  private void setLastEventReceived(boolean lastEvent) {
    // Only one last event can be received.
    this.lastEventReceived = this.lastEventReceived ? true : lastEvent;
  }

  /**
   * New event added to the buffer
   *
   * @param sequenceNumber of the event
   * @param isLastEvent
   */
  void eventBuffered(long sequenceNumber, boolean isLastEvent) {
    bufferedRecordCount++;
    latestBufferedSequence = Math.max(sequenceNumber, latestBufferedSequence == null ?
        Long.MIN_VALUE : latestBufferedSequence);
    earliestBufferedSequence = Math.min(sequenceNumber, earliestBufferedSequence == null ?
        Long.MAX_VALUE : earliestBufferedSequence);

    setLastEventReceived(isLastEvent);
  }

  /**
   * An event was processed and removed from the buffer.
   *
   * @param sequence of the processed event
   */
  public void processedBufferedEvent(long sequence) {
    bufferedRecordCount--;
    lastOutputSequence = sequence;

    if (bufferedRecordCount == 0) {
      earliestBufferedSequence = latestBufferedSequence = null;
    } else {
      // TODO: We don't know for sure that it's the earliest record - we would need to test that.
      earliestBufferedSequence = sequence + 1;
    }
  }

  /**
   * A set of records was pulled from the buffer, but it turned out that the element is not
   * sequential. Record this newest sequence number - it will prevent unnecessary batch retrieval.
   *
   * @param newEarliestSequence
   */
  public void foundSequenceGap(long newEarliestSequence) {
    earliestBufferedSequence = newEarliestSequence;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ProcessingState)) {
      return false;
    }
    ProcessingState<KeyT> that = (ProcessingState<KeyT>) o;
    return bufferedRecordCount == that.bufferedRecordCount && lastOutputSequence.equals(
        that.lastOutputSequence) && Objects.equals(latestBufferedSequence,
        that.latestBufferedSequence) && Objects.equals(earliestBufferedSequence,
        that.earliestBufferedSequence) && Objects.equals(lastEventReceived, that.lastEventReceived)
        && Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lastOutputSequence, latestBufferedSequence, earliestBufferedSequence,
        bufferedRecordCount, key, lastEventReceived);
  }

  public boolean isProcessingCompleted() {
    return lastEventReceived && bufferedRecordCount == 0;
  }

  public void recordReceived() {
    recordsReceived++;
  }

  public boolean isNextEvent(long sequence) {
    return lastOutputSequence != null && sequence == lastOutputSequence + 1;
  }

  /**
   * Coder for the processing status
   *
   * @param <KeyT>
   */
  static class ProcessingStateCoder<KeyT> extends Coder<ProcessingState<KeyT>> {

    private static final NullableCoder<Long> NULLABLE_LONG_CODER = NullableCoder.of(
        VarLongCoder.of());
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final VarIntCoder INTEGER_CODER = VarIntCoder.of();
    private static final BooleanCoder BOOLEAN_CODER = BooleanCoder.of();

    private Coder<KeyT> keyCoder;

    public static <KeyT> ProcessingStateCoder<KeyT> of(Coder<KeyT> keyCoder) {
      ProcessingStateCoder<KeyT> result = new ProcessingStateCoder<>();
      result.keyCoder = keyCoder;
      return result;
    }

    @Override
    public void encode(ProcessingState<KeyT> value, OutputStream outStream) throws IOException {
      NULLABLE_LONG_CODER.encode(value.getLastOutputSequence(), outStream);
      NULLABLE_LONG_CODER.encode(value.getEarliestBufferedSequence(), outStream);
      NULLABLE_LONG_CODER.encode(value.getLatestBufferedSequence(), outStream);
      LONG_CODER.encode(value.getBufferedRecordCount(), outStream);
      LONG_CODER.encode(value.getRecordsReceived(), outStream);
      BOOLEAN_CODER.encode(value.isLastEventReceived(), outStream);
      keyCoder.encode(value.getKey(), outStream);
    }

    @Override
    public ProcessingState<KeyT> decode(InputStream inStream) throws IOException {
      Long lastOutputSequence = NULLABLE_LONG_CODER.decode(inStream);
      Long earliestBufferedSequence = NULLABLE_LONG_CODER.decode(inStream);
      Long latestBufferedSequence = NULLABLE_LONG_CODER.decode(inStream);
      int bufferedRecordCount = INTEGER_CODER.decode(inStream);
      long recordsReceivedCount = LONG_CODER.decode(inStream);
      boolean allEventsProcessed = BOOLEAN_CODER.decode(inStream);
      KeyT key = keyCoder.decode(inStream);

      return new ProcessingState<>(key, lastOutputSequence, earliestBufferedSequence,
          latestBufferedSequence, bufferedRecordCount, recordsReceivedCount, allEventsProcessed);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return List.of();
    }

    @Override
    public void verifyDeterministic() {
    }
  }
}
