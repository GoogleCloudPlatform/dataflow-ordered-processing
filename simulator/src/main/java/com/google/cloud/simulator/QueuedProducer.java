package com.google.cloud.simulator;

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

  private List<T> getNext() {

    // Get next item to execute -- if nothing is there,
    // we need to stop and return null.
    QueuedItem<T> t = this.que.poll();
    if (t == null) {
      return null;
    }

    // Update the last tick (for relative delays)
    this.lastTick = t.tick;

    // Execute the work
    // On exception, return empty list to keep going.
    try {
      return t.work.call();
    } catch (Exception e) {
      System.out.println("Exception: " + e.toString());
      return Arrays.asList();
    }
  }

  //@Override
  //public Iterator<List<T>> iterator() {
  //  return new Iterator<List<T>>() {
      @Override
      public boolean hasNext() {
        return !que.isEmpty();
      }

      @Override
      public List<T> next() {
        return getNext();
      }
  //  };
  //}

  /*
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private List<T> items = null;
      private int itemIdx = 0;
  
      @Override
      public boolean hasNext() {

        // If we're on a list of result items, return true
        // if there's more in the list
        if (items != null && itemIdx < items.size()) {
          return true;
        }

        // If there's more in the queue, return true
        // (more work coming)
        return !que.isEmpty();
      }
  
      @Override
      public T next() {
        if (items == null) {
          items = getNext();
        }

        if (items == null) {
          return null;
        }

        while (items == null || itemIdx == items.size()) {
          items = getNext();
        }
        T next = items.get(itemIdx);
        itemIdx ++;
        return next;
        }
      }
    };
  }
  */
}
