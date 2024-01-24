/*
 * Copyright 2024 Google LLC
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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;

public abstract class OrderedProcessingHandler<EventT, KeyT, StateT, ResultT> {

  private final Class<EventT> eventTClass;
  private final Class<KeyT> keyTClass;
  private final Class<StateT> stateTClass;
  private final Class<ResultT> resultTClass;

  public OrderedProcessingHandler(Class<EventT> eventTClass, Class<KeyT> keyTClass,
      Class<StateT> stateTClass, Class<ResultT> resultTClass) {
    this.eventTClass = eventTClass;
    this.keyTClass = keyTClass;
    this.stateTClass = stateTClass;
    this.resultTClass = resultTClass;
  }

  @NonNull
  abstract EventExaminer getEventExaminer();

  @NonNull
  public Coder<EventT> getEventCoder(Pipeline pipeline) throws CannotProvideCoderException {
    return pipeline.getCoderRegistry().getCoder(eventTClass);
  }

  public Coder<StateT> getStateCoder(Pipeline pipeline) throws CannotProvideCoderException {
    return pipeline.getCoderRegistry().getCoder(stateTClass);
  }

  public Coder<KeyT> getKeyCoder(Pipeline pipeline) throws CannotProvideCoderException {
    return pipeline.getCoderRegistry().getCoder(keyTClass);
  }

  public Coder<ResultT> getResultCoder(Pipeline pipeline) throws CannotProvideCoderException {
    return pipeline.getCoderRegistry().getCoder(resultTClass);
  }

  int getStatusFrequencyUpdateSeconds() {
    return -1;
  }

  boolean isProduceStatusUpdateOnEveryEvent() {
    return true;
  }

  ;

  int getMaxResultCountPerBundle() {
    return 10_000;
  }
}
