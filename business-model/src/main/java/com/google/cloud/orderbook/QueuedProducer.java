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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

/*
 * General utility for scheduling order events in a queue (especially for cancellations).
 * 
 * This gives a semblence of time based on ''ticks''. A tick is a measure of activity (namely,
 * generating orders), so that after N ticks the order can be cancelled.
 * 
 * This also enables having a trailing average for execution price to be also measured by
 * ticks.
 */
class QueuedProducer<T> implements Iterator<List<T>> {

  /*
   * A unit of work is a Callable with a tick time of when it should execute.
   */
  static private class QueuedItem<T> implements Comparable<QueuedItem<T>>, Callable<List<T>> {
    final Callable<List<T>> work;
    final long tick;
    QueuedItem(long tick, Callable<List<T>> work) {
      this.tick = tick;
      this.work = work;
    }
    public List<T> call() throws Exception {
      return this.work.call();
    }
    @Override
    public int compareTo(QueuedItem<T> arg0) {
      if (this.tick < arg0.tick) {
        return -1;
      } else if (this.tick == arg0.tick) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  /*
   * PriorityQueue is used to capture all outstanding actions and associated tick time --
   * next item to execute will be at the nearest tick. Ordering within a tick is not
   * important.
   */
  final private PriorityQueue<QueuedItem<T>> que = new PriorityQueue<QueuedItem<T>>();
  private long lastTick = 0;

  /**
   * Add a bit of a work after a certain tick delay (relative to current tick),
   * with a bit of work.
   * 
   * @param delay Number of ticks to delay (0 for as soon as possible)
   * @param work  Callable for work
   */
  void add(long delay, Callable<List<T>> work) {
    delay += lastTick;
    que.add(new QueuedItem<T>(delay, work));
  }

  // At shutdown work queue
  final private ArrayList<Callable<List<T>>> atShutdownWork = new ArrayList<Callable<List<T>>>();

  /**
   * Add work to execute when shutting down. All outstanding work is ignored, and these tasks are
   * executed instead.
   * 
   * @param work  Callable for work at shutdown
   */
  void addAtShutdown(Callable<List<T>> work) {
    atShutdownWork.add(0, work);
  }

  /**
   * Shutdown work queue.
   * 
   * This should be called from the work itself.
   * 
   * - Stop adding new work to the queue (even if asked)
   * - Finish processing the last bit of work in the queue
   * - Execute the shutdown work and append to the return results
   * - Stop returning events from next() after that
   */
  private boolean isActive = true;
  void shutdown() {
    isActive = false;
  }

  @Override
  public boolean hasNext() {
    //if (shutdownEvents != null) {
    //  return !shutdownEvents.isEmpty();
    //}
    return isActive && !que.isEmpty();
  }

  @Override
  public List<T> next() {

    // If already shutdown, return empty list.
    //
    // This should not happen, as a shutdown() should be called
    // within the work itself.
    if (!isActive) {
      return Arrays.asList();
    }

    // Get next item to execute -- if nothing is there,
    // we need to stop and return null.
    Callable<List<T>> workTask = null;
    QueuedItem<T> t = this.que.poll();
    if (t == null) {
      return null;
    }
    workTask = t.work;
    lastTick = t.tick;

    // Execute the work
    // On exception, return empty list to keep going.
    List<T> results = null;
    try {
      results = workTask.call();
    } catch (Exception e) {
      System.out.println("Exception: " + e.toString());
      return Arrays.asList();
    }

    // If not shutdown, return the results
    if (isActive) {
      return results;
    }

    // Calculate and merge the shutdown events after the current events
    ArrayList<T> returnEvents = new ArrayList<T>();
    returnEvents.addAll(results);
    for (Callable<List<T>> task : atShutdownWork) {
      try {
        returnEvents.addAll(task.call());
      } catch (Exception e) {
        System.out.println("Exception: " + e.toString());
      }
    }

    return returnEvents;
  }
}
