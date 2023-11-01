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

package com.google.cloud.dataflow.orderbook;

import com.google.cloud.orderbook.model.OrderBookEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class OrderBookCoder extends Coder<OrderBookMutableState> {
  private VarIntCoder intCoder = VarIntCoder.of();
  private BooleanCoder booleanCoder = BooleanCoder.of();
  private MapCoder mapCoder = MapCoder.of(VarLongCoder.of(), VarLongCoder.of());

  private Coder<OrderBookEvent> orderBookEventCoder = ProtoCoder.of(OrderBookEvent.class);

  private OrderBookCoder() {}

  /**
   * Returns a {@link OrderBookCoder} with the default settings.
   */
  public static OrderBookCoder of() {
    return new OrderBookCoder();
  }

  @Override
  public void encode(OrderBookMutableState state, OutputStream out) throws IOException {
    intCoder.encode(state.getDepth(), out);
    booleanCoder.encode(state.isWithTrade(), out);
    mapCoder.encode(state.getPrices(), out);
    orderBookEventCoder.encode(state.getLastOrderBookEvent(), out);
  }

  @Override
  public OrderBookMutableState decode(InputStream inStream) throws IOException {
    int depth = intCoder.decode(inStream);
    boolean withTrade = booleanCoder.decode(inStream);
    Map<Long, Long> prices = mapCoder.decode(inStream);
    OrderBookEvent lastOrderBookEvent = orderBookEventCoder.decode(inStream);
    OrderBookMutableState orderBookMutableState = new OrderBookMutableState(depth, withTrade, prices, lastOrderBookEvent);
    return orderBookMutableState;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<? extends @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>> getCoderArguments() {
    return Arrays.asList();
  }

  @Override
  public void verifyDeterministic()
      throws @UnknownKeyFor @NonNull @Initialized NonDeterministicException {

  }
}
