package com.google.cloud.simulator;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import com.google.cloud.orderbook.model.OrderBookEvent;

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
  private long nextSeqId = 0;

  // Queued producer -- used to synchronise the output of the matchers
  final private QueuedProducer<OrderBookEvent> que = new QueuedProducer<OrderBookEvent>();

  // Number of milliseconds in a bucket for throttling
  final private static long MILLISECOND_BUCKET_SIZE = 100;

  // Throttling mode and number of events per second we expect
  final private TimeThrottleMode mode;
  final private long eventsPerBucket;

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
  public MatcherContext() {
    this(TimeThrottleMode.UNTHROTTLED_EVENTS_SYSTEM_TIME, 0, 0);
  }

  /*
   * This generates events as fast as it can (no sleeping, no delays), and uses a simulated time
   * at the specific events per second rate.
   * 
   * Recommended for batch data generation (Dataflow) or testing purposes.
   */
  public MatcherContext(long eventsPerSecond, long startTimeMillis) {
    this(TimeThrottleMode.UNTHROTTLED_EVENTS_THROTTLED_SIMULATED_TIME, eventsPerSecond, startTimeMillis);
  }

  /*
   * This generates events at the specific event rate using system time.
   * 
   * Recommended for general PubSub or Dataflow usage.
   */
  public MatcherContext(long eventsPerSecond) {
    this(TimeThrottleMode.THROTTLED_SYSTEM_TIME, eventsPerSecond, System.currentTimeMillis());
  }

  private MatcherContext(TimeThrottleMode mode, long eventsPerSecond, long startTimeMillis) {
    this.mode = mode;
    this.eventsPerBucket = eventsPerSecond / (1000 / MILLISECOND_BUCKET_SIZE);
    this.nextBucketTime = startTimeMillis;
  }

  void add(long delay, Callable<List<OrderBookEvent>> work) {
    this.que.add(delay, work);
  }

  OrderBookEvent.Builder buildOrderBookEvent(OrderBookEvent.Type type, long contractSeqId, long contractId, Order order) {

    long eventTimeMillis;

    // Simulated time is the bucket time, otherwise use the system time
    if (mode == TimeThrottleMode.UNTHROTTLED_EVENTS_THROTTLED_SIMULATED_TIME) {
      eventTimeMillis = nextBucketTime;
    } else {
      eventTimeMillis = System.currentTimeMillis();
    }

    // If not unthrottled system time, we need to track bucketing of time
    if (mode != TimeThrottleMode.UNTHROTTLED_EVENTS_SYSTEM_TIME) {

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

        // Shift to next time target
        nextBucketTime += MILLISECOND_BUCKET_SIZE;
        eventsInBucket = 0;
      }
    }

    OrderBookEvent.Builder builder = OrderBookEvent.newBuilder()
        .setTimestampMS(eventTimeMillis)
        .setSeqId(nextSeqId++)
        .setContractSeqId(contractSeqId)
        .setContractId(contractId)
        .setType(type)
        .setOrderId(order.getOrderId())
        .setPrice(order.getPrice())
        .setSide(order.getSide())
        .setQuantity(order.getQuantity())
        .setQuantityRemaining(order.getQuantityRemaining())
        .setQuantityFilled(0)
        .setMatchNumber(0);

    return builder;
  }

  @Override
  public Iterator<List<OrderBookEvent>> iterator() {
    return this.que;
  }
}