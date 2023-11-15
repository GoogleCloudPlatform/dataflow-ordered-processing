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

import java.io.Serializable;

/**
 * Mutable state mutates when events apply to it. It will be stored in a Beam state
 */
public interface MutableState<Event, Result> extends Serializable {

  /**
   * The interface assumes that events will mutate the state without the possibility of throwing an
   * error.
   * TODO: this might be too simplistic and a mechanism for failure of applying the event to a state
   * would need to be created.
   *
   * @param mutation
   * @return
   */
  void mutate(Event mutation);

  /**
   * Will be called after each state mutation. Can return null if the result is not yet ready.
   *
   * @return
   */
  Result produceResult();
}
