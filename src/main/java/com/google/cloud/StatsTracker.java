
package com.google.cloud;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimerTask;

public class StatsTracker extends TimerTask {

  final private static long GBYTE  = 1024 * 1024 * 1024;
  final private static long MBYTE  = 1024 * 1024;
  final private static long KBYTE  = 1024;
  static private String getFormattedBytes(double bytes) {
    if (bytes >= GBYTE) {
      return String.format("%3.0fgb/s", bytes/GBYTE);
    } else if (bytes >= MBYTE) {
      return String.format("%3.0fmb/s", bytes/MBYTE);
    } else if (bytes >= KBYTE) {
      return String.format("%3.0fkb/s", bytes/KBYTE);
    } else {
      return String.format("%3.0f  /s", bytes);
    }
  }

  static private String getFormattedOps(double ops) {
    if (ops >= 1_000_000_000) {
      return String.format("%3.0fg/s", ops/1_000_000_000);
    } else if (ops >= 1_000_000) {
      return String.format("%3.0fm/s", ops/1_000_000);
    } else if (ops >= 1_000) {
      return String.format("%3.0fk/s", ops/1_000);
    } else {
      return String.format("%3.0f /s", ops);
    }
  }

  final private static long ns_Minutes = 60_000_000_000L;
  final private static long ns_Seconds = 1_000_000_000L;
  final private static long ns_MS = 1_000_000L;
  final private static long ns_US = 1_000L;
  static String getFormattedLatency(long t) {
    if (t >= ns_Minutes) {
      return String.format("%3dm ", t / ns_Minutes);
    }
    if (t >= ns_Seconds) {
      return String.format("%3ds ", t / ns_Seconds);
    }
    if (t >= ns_MS) {
      return String.format("%3dms", t / ns_MS);
    }
    if (t >= ns_US) {
      return String.format("%3dµs", t / ns_US);
    }
    return String.format("%3dns", t);
  }

  static private class TimePoint implements Comparable<TimePoint> {
    final long nanoDuration;
    final long weight;
    TimePoint(long weight, long nanoDuration) {
      this.weight = weight;
      this.nanoDuration = nanoDuration;
    }
    @Override
    public int compareTo(TimePoint arg0) {
      long diff = arg0.nanoDuration - this.nanoDuration;
      if (diff < 0) {
        return -1;
      } else if (diff > 0) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  static private class TimeDistribution {
    ArrayList<TimePoint> points = new ArrayList<TimePoint>();
    long total;

    void clear() {
      points.clear();
      total = 0;
    }

    void add(long weight, long nanoDuration) {
      points.add(new TimePoint(weight, nanoDuration));
      total += weight;
    }

    List<Long> getPercentile(List<Double> percentile) {
      // Biggest to smallest!
      Collections.sort(this.points);

      // Assume correct percentile values
      ArrayList<Long> timings = new ArrayList<Long>();

      long total = this.total;  // start at 100%
      for (int i = 0, j = 0; i < points.size() && j < percentile.size(); i++) {
        total -= points.get(i).weight;
        double curr_perc = (double)total / (double)this.total;

        while (j < percentile.size() && curr_perc <= percentile.get(j)) {
          timings.add(points.get(i).nanoDuration);
          j++;
        }
      }

      return timings;
    }
  }

  static public class Stats {
    private TimeDistribution timings = new TimeDistribution();
    private long startTime = System.currentTimeMillis();
    private long ops = 0;
    private long bytes = 0;
    private final String name;
    private final List<Double> buckets = Arrays.asList(1.0, 0.99, 0.95, 0.5, 0.0);

    public Stats(String name) {
      this.name = name;
    }

    synchronized void add(long nanoDuration, long ops, long bytes) {
      ops += ops;
      bytes += bytes;
      timings.add(ops, nanoDuration);
    }

    String statsHeader() {
      return String.format("%s: %7.7s %6.6s %5.5s %5.5s %5.5s %5.5s %5.5s",
        name,
        "bytes",
        "ops",
        "max",
        "99th",
        "95th",
        "50th",
        "min");
    }

    String stats(boolean reset) {

      // Fetch relevant stats as quckly as possible -- but synchronized
      long windowOps, windowBytes;
      double duration;
      List<Long> latencies;
      synchronized(this) {
        if (this.ops == 0) {
          return "N/A";
        }

        duration = (System.currentTimeMillis() - this.startTime)/1000.0;
        latencies = this.timings.getPercentile(buckets);
        windowOps = ops;
        windowBytes = bytes;

        if (reset) {
          startTime = System.currentTimeMillis();
          ops = 0;
          bytes = 0;
          timings.clear();
        }
      }

      // Format output
      return String.format("%s: %s %s %s %s %s %s %s",
        name,
        getFormattedBytes(windowBytes/duration),
        getFormattedOps(windowOps/duration),
        getFormattedLatency(latencies.get(0)),
        getFormattedLatency(latencies.get(1)),
        getFormattedLatency(latencies.get(2)),
        getFormattedLatency(latencies.get(3)),
        getFormattedLatency(latencies.get(4)));
    }
  }

  private final List<Stats> stats;
  public StatsTracker(Stats... stats) {
    this.stats = Arrays.asList(stats);

    ArrayList<String> headers = new ArrayList<String>();
    for (Stats stat : stats) {
      headers.add(stat.statsHeader());
    }
    System.out.println(String.join(" ", headers));
  }

  @Override
  public void run() {
    ArrayList<String> ostats = new ArrayList<String>();
    for (Stats stat : stats) {
      ostats.add(stat.stats(true));
    }
    System.out.println(String.join(" ", ostats));
  }
}
