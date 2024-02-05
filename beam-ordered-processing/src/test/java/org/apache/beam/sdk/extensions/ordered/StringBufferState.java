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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.ordered.StringBufferState.StringBufferStateCoder;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@DefaultCoder(StringBufferStateCoder.class)
public class StringBufferState implements MutableState<String, String> {

  private int emissionFrequency = 1;
  private long currentlyEmittedElementNumber = 0L;

  public StringBufferState(String initialEvent, int emissionFrequency) {
    this.emissionFrequency = emissionFrequency;
    mutate(initialEvent);
  }

  final private StringBuilder sb = new StringBuilder();

  @Override
  public void mutate(String event) {
    sb.append(event);
  }

  @Override
  public String produceResult() {
    return currentlyEmittedElementNumber++ % emissionFrequency == 0 ? sb.toString() : null;
  }

  public String toString() {
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StringBufferState)) {
      return false;
    }
    StringBufferState that = (StringBufferState) o;
    return emissionFrequency == that.emissionFrequency
        && currentlyEmittedElementNumber == that.currentlyEmittedElementNumber && sb.toString()
        .equals(that.sb.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(sb);
  }

  public static class StringBufferStateCoder extends Coder<StringBufferState> {

    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final Coder<Integer> INT_CODER = VarIntCoder.of();

    @Override
    public void encode(StringBufferState value,
        @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws IOException {
      INT_CODER.encode(value.emissionFrequency, outStream);
      LONG_CODER.encode(value.currentlyEmittedElementNumber, outStream);
      STRING_CODER.encode(value.sb.toString(), outStream);
    }

    @Override
    public StringBufferState decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws IOException {
      int emissionFrequency = INT_CODER.decode(inStream);
      long currentlyEmittedElementNumber = LONG_CODER.decode(inStream);
      String decoded = STRING_CODER.decode(inStream);
      StringBufferState result = new StringBufferState(decoded, emissionFrequency);
      result.currentlyEmittedElementNumber = currentlyEmittedElementNumber;
      return result;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized List<? extends @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>> getCoderArguments() {
      return List.of();
    }

    @Override
    public void verifyDeterministic() {

    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Object structuralValue(StringBufferState value) {
      return super.structuralValue(value);
    }
  }
}
