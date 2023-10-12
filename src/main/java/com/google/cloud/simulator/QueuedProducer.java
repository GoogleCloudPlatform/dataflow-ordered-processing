package com.google.cloud.simulator;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/*
 * General utility for scheduling order events in a queue (especially for cancellations).
 */
public class QueuedProducer<T> implements Iterable<T> {
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

  final private PriorityQueue<QueuedItem<T>> que = new PriorityQueue<QueuedItem<T>>();

  private long lastTick = 0;
  public void add(long delay, Callable<List<T>> work) {
    delay += lastTick;
    que.add(new QueuedItem<T>(delay, work));
  }

  private Callable<List<T>> poll() {
    QueuedItem<T> t = this.que.poll();
    if (t == null) {
      return null;
    }
    this.lastTick = t.tick;
    return t.work;
  }

  public void apply(Consumer<T> consumer) {
    for (T t : this) {
      consumer.accept(t);
    }
  }
  
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private List<T> items = null;
      private int itemIdx = 0;
  
      @Override
      public boolean hasNext() {
        if (items != null && itemIdx < items.size()) {
          return true;
        }

        return !que.isEmpty();
      }
  
      @Override
      public T next() {
        while (items == null || itemIdx == items.size()) {
          Callable<List<T>> qi = poll();
          if (qi == null) {
            return null;
          }
          try {
            items = qi.call();
          } catch (Exception e) {
            System.out.println("Exception: " + e.toString());
            return null;
          }
          itemIdx = 0;
        }
        T next = items.get(itemIdx);
        itemIdx ++;
        return next;
      }
    };
  }
}
