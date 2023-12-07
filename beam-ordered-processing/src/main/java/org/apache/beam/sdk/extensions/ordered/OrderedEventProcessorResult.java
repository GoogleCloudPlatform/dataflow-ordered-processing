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

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * The result of the ordered processing. Two PCollections are returned:
 * <li>output - the key/value of the mutated states</li>
 * <li>processingStatuses - the key/value of the status of processing for a particular key</li>
 *
 * @param <Key>
 * @param <Result>
 */

public class OrderedEventProcessorResult<Key, Result, Event> implements POutput {

  private final PCollection<KV<Key, Result>> outputPCollection;
  private final TupleTag<KV<Key, Result>> outputPCollectionTupleTag;

  private final PCollection<KV<Key, OrderedProcessingStatus>> eventProcessingStatusPCollection;
  private final TupleTag<KV<Key, OrderedProcessingStatus>> eventProcessingStatusTupleTag;

  private final PCollection<KV<Key, OrderedProcessingDiagnosticEvent>> diagnosticsEventPCollection;
  private final TupleTag<KV<Key, OrderedProcessingDiagnosticEvent>> diagnosticsTupleTag;

  private final PCollection<KV<Key, KV<Long, UnprocessedEvent<Event>>>> unprocessedEventPCollection;
  private final TupleTag<KV<Key, KV<Long, UnprocessedEvent<Event>>>> unprocessedEventTupleTag;

  OrderedEventProcessorResult(Pipeline pipeline,
      PCollection<KV<Key, Result>> outputPCollection,
      TupleTag<KV<Key, Result>> outputPCollectionTupleTag,
      PCollection<KV<Key, OrderedProcessingStatus>> eventProcessingStatusPCollection,
      TupleTag<KV<Key, OrderedProcessingStatus>> eventProcessingStatusTupleTag,
      PCollection<KV<Key, OrderedProcessingDiagnosticEvent>> diagnosticsEventPCollection,
      TupleTag<KV<Key, OrderedProcessingDiagnosticEvent>> diagnosticsEventTupleTag,
      PCollection<KV<Key, KV<Long, UnprocessedEvent<Event>>>> unprocessedEventPCollection,
      TupleTag<KV<Key, KV<Long, UnprocessedEvent<Event>>>> unprocessedEventTupleTag) {
    this.outputPCollection = outputPCollection;
    this.outputPCollectionTupleTag = outputPCollectionTupleTag;
    this.eventProcessingStatusPCollection = eventProcessingStatusPCollection;
    this.eventProcessingStatusTupleTag = eventProcessingStatusTupleTag;
    this.diagnosticsEventPCollection = diagnosticsEventPCollection;
    this.diagnosticsTupleTag = diagnosticsEventTupleTag;

    this.pipeline = pipeline;
    this.unprocessedEventPCollection = unprocessedEventPCollection;
    this.unprocessedEventTupleTag = unprocessedEventTupleTag;
  }

  private final Pipeline pipeline;

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(eventProcessingStatusTupleTag, eventProcessingStatusPCollection,
        outputPCollectionTupleTag, outputPCollection);
  }

  @Override
  public void finishSpecifyingOutput(String transformName, PInput input,
      PTransform<?, ?> transform) {
  }

  /**
   * @return processing status for a particular key. The elements will have the timestamp of the
   * instant the status was emitted.
   */
  public PCollection<KV<Key, OrderedProcessingStatus>> processingStatuses() {
    return eventProcessingStatusPCollection;
  }

  /**
   * @return processed states keyed by the original key
   */
  public PCollection<KV<Key, Result>> output() {
    return outputPCollection;
  }

  public PCollection<KV<Key, OrderedProcessingDiagnosticEvent>> diagnosticEvents() {
    return diagnosticsEventPCollection;
  }

  public PCollection<KV<Key, KV<Long, UnprocessedEvent<Event>>>> unprocessedEvents() {
    return unprocessedEventPCollection;
  }
}
