package com.google.cloud;

import java.util.ArrayDeque;
import java.util.function.Consumer;

public class RateLimiter <T> implements Consumer<T> {
  // Two sliding windows:
  // One -- the maximum we don't want to exceed for any single update.
  // Two -- the maximum we never want to exceed over a sliding window.

  // We also want to incorporate a ramp to the rate as well.

  final private ArrayDeque<Long> lastTimes = new ArrayDeque<Long>(100);

  final private long limitPerSecond;

  public RateLimiter()
}

  @Override
  public void accept(T t) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'accept'");
  }
