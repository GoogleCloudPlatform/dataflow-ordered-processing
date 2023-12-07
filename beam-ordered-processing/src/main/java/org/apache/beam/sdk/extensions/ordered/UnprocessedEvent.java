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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;

@AutoValue
public abstract class UnprocessedEvent<EventT> {

  public static <EventT> UnprocessedEvent<EventT> create(EventT event, Reason reason) {
    return new AutoValue_UnprocessedEvent<>(event, reason);
  }

  public enum Reason {duplicate, buffered}

  ;

  public abstract EventT getEvent();

  public abstract Reason getReason();

  static class UnprocessedEventCoder<EventT> extends Coder<UnprocessedEvent<EventT>> {

    private final Coder<EventT> eventCoder;

    UnprocessedEventCoder(Coder<EventT> eventCoder) {
      this.eventCoder = eventCoder;
    }

    @Override
    public void encode(UnprocessedEvent<EventT> value, OutputStream outStream) throws IOException {
      ByteCoder.of().encode((byte) value.getReason().ordinal(), outStream);
      eventCoder.encode(value.getEvent(), outStream);
    }

    @Override
    public UnprocessedEvent<EventT> decode(InputStream inputStream) throws IOException {
      Reason reason = Reason.values()[ByteCoder.of().decode(inputStream)];
      EventT event = eventCoder.decode(inputStream);
      return UnprocessedEvent.create(event, reason);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
//     TODO: implement
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
// TODO: implement
    }
  }

}
