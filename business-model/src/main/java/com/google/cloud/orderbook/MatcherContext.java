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

package com.google.cloud.orderbook;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import com.google.cloud.orderbook.model.OrderBookEvent;
import com.google.cloud.orderbook.model.OrderBookEvent.Side;

public class MatcherContext implements Iterable<List<OrderBookEvent>> {
  
  enum TimeThrottleMode {
    /* Generate OrderBookEvents without throttling (no sleep),
     * and use the system time for the order events.
    */
    UNTHROTTLED_EVENTS_SYSTEM_TIME,

    /* Generate OrderBookEvents without throttling (no sleep),
     * and generate a simulated time that is throttled at a certain rate
    */
    UNTHROTTLED_EVENTS_THROTTLED_SIMULATED_TIME,

    /* Generate OrderBookEvents with throttling (sleep),
     * and use the system time for generation.
    */
    THROTTLED_SYSTEM_TIME,
  }

  // Global Sequence ID
  private long nextSeqId = 1;

  // Queued producer -- used to synchronise the output of the matchers
  final private QueuedProducer<OrderBookEvent> que = new QueuedProducer<>();

  // Number of milliseconds in a bucket for throttling
  final private static long MILLISECOND_BUCKET_SIZE = 100;

  // Throttling mode and number of events per second we expect
  final private TimeThrottleMode mode;
  final private long eventsPerBucket;
  final private long startTimeMillis;

  private final String sessionId;

  // Maximum duration (optional)
  final private long maxDurationSeconds;

  // Current throttling status
  private long eventsInBucket = 0;
  private long nextBucketTime;

  /*
   * Use unthrottled event generation and the system time.
   *
   * This generates events as fast as it can (no sleeping, no delays), and uses the system time
   * for the events.
   *
   * Recommended for PubSub or Dataflow usage when you want to stream things as fast as you can.
   */
  public MatcherContext(long maxSeconds, String sessionId) {
    this(TimeThrottleMode.UNTHROTTLED_EVENTS_SYSTEM_TIME, 0, System.currentTimeMillis(), maxSeconds,
        sessionId);
  }

  /*
   * This generates events as fast as it can (no sleeping, no delays), and uses a simulated time
   * at the specific events per second rate.
   *
   * Recommended for batch data generation (Dataflow) or testing purposes.
   */
  public MatcherContext(long eventsPerSecond, long startTimeMillis, long maxSeconds,
      String sessionId) {
    this(TimeThrottleMode.UNTHROTTLED_EVENTS_THROTTLED_SIMULATED_TIME, eventsPerSecond,
        startTimeMillis, maxSeconds, sessionId);
  }

  /*
   * This generates events at the specific event rate using system time.
   *
   * Recommended for general PubSub or Dataflow usage.
   */
  public MatcherContext(long eventsPerSecond, long maxSeconds, String sessionId) {
    this(TimeThrottleMode.THROTTLED_SYSTEM_TIME, eventsPerSecond, System.currentTimeMillis(),
        maxSeconds, sessionId);
  }

  private MatcherContext(TimeThrottleMode mode, long eventsPerSecond, long startTimeMillis,
      long maxSeconds, String sessionId) {
    this.mode = mode;
    this.eventsPerBucket = eventsPerSecond / (1000 / MILLISECOND_BUCKET_SIZE);
    this.nextBucketTime = startTimeMillis;
    this.startTimeMillis = startTimeMillis;
    this.maxDurationSeconds = maxSeconds;

    addAtShutdown(new Callable<List<OrderBookEvent>>() {
      @Override
      public List<OrderBookEvent> call() throws Exception {
        return Arrays.asList(buildFinalOrderBookEvent().build());
      }
    });

    this.sessionId = sessionId;
  }

  public void add(long delay, Callable<List<OrderBookEvent>> work) {
    this.que.add(delay, work);
  }

  public void addAtShutdown(Callable<List<OrderBookEvent>> work) {
    this.que.addAtShutdown(work);
  }

  private long getNextEventTimeMillis() {

    long eventTimeMillis;

    // Just use system time and go as fast as possible
    if (mode != TimeThrottleMode.UNTHROTTLED_EVENTS_THROTTLED_SIMULATED_TIME) {
      eventTimeMillis = System.currentTimeMillis();
    }

    //
    // Now we need to calculate the synthetic time (bucketing)
    //

    // Count events in the bucket
    eventsInBucket ++;

    // If bucket is full, need to delay and/or shift bucket
    if (eventsInBucket == eventsPerBucket) {

      // If throttled system time, we need a real delay
      // (we don't need absolute precision, so interruptions we can ignore)
      if (mode == TimeThrottleMode.THROTTLED_SYSTEM_TIME) {
        long neededDelay = nextBucketTime - System.currentTimeMillis();
        if (neededDelay > 0) {
          try {
            Thread.sleep(neededDelay);
          } catch (InterruptedException e) {}
        }
      }

      // Shift to next time bucket
      nextBucketTime += MILLISECOND_BUCKET_SIZE;
      eventsInBucket = 0;
    }

    // If it's simulated time, return the bucket time
    if (mode == TimeThrottleMode.UNTHROTTLED_EVENTS_THROTTLED_SIMULATED_TIME) {
      eventTimeMillis = nextBucketTime;
    } else {
      eventTimeMillis = System.currentTimeMillis();
    }

    // Check if we need to shutdown the queue
    if ((maxDurationSeconds > 0) &&
        (eventTimeMillis - startTimeMillis)/1000 >= maxDurationSeconds) {
      this.que.shutdown();
    }

    return eventTimeMillis;
  }

  OrderBookEvent.Builder buildFinalOrderBookEvent(long contractSeqId, long contractId) {
    long eventTimeMillis = getNextEventTimeMillis();

    return OrderBookEvent.newBuilder()
        .setTimestampMS(eventTimeMillis)
        .setSeqId(nextSeqId++)
        .setContractSeqId(contractSeqId)
        .setContractId(contractId)
        .setLastMessage(false)
        .setSessionId(sessionId)
        .setLastContractMessage(true);
  }

  public OrderBookEvent.Builder buildFinalOrderBookEvent() {
    long eventTimeMillis = getNextEventTimeMillis();

    return OrderBookEvent.newBuilder()
        .setTimestampMS(eventTimeMillis)
        .setSeqId(nextSeqId++)
        .setSessionId(sessionId)
        .setLastMessage(true);
  }

  OrderBookEvent.Builder buildOrderBookEvent(OrderBookEvent.Type type, long contractSeqId, long contractId, Order order) {
    long eventTimeMillis = getNextEventTimeMillis();

    return OrderBookEvent.newBuilder()
        .setTimestampMS(eventTimeMillis)
        .setSeqId(nextSeqId++)
        .setContractSeqId(contractSeqId)
        .setContractId(contractId)
        .setLastMessage(false)
        .setSessionId(sessionId)
        .setLastContractMessage(false)
        .setType(type)
        .setOrderId(order.getOrderId())
        .setPrice(order.getPrice())
        .setSide(order.getSide())
        .setQuantity(order.getQuantity())
        .setQuantityRemaining(order.getQuantityRemaining())
        .setQuantityFilled(0)
        .setMatchNumber(0);
  }

  @Override
  public Iterator<List<OrderBookEvent>> iterator() {
    return this.que;
  }

  private long currentOrderId = 1;
  public Order newOrder(Side side, long price, long quantity) {
    return new Order(currentOrderId++, side, price, quantity);
  }

}