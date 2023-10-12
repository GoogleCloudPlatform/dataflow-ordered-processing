package com.google.cloud;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.google.cloud.simulator.MarketDepth;
import com.google.cloud.simulator.OrderBookEvent;
import com.google.cloud.simulator.Simulator;

/**
 * Hello world!
 *
 */
public class AppPublisher
{
  static private Options options = new Options();
  static {
    options.addOption(
      Option.builder("t")
        .longOpt("topic")
        .optionalArg(false)
        .desc("Limit of events to produce")
        .required()
        .type(String.class)
        .build()
    );
    options.addOption(
      Option.builder("n")
        .hasArg(true)
        .longOpt("limit")
        .optionalArg(false)
        .desc("Limit of events to produce")
        .type(Number.class)
        .build()
    );
    options.addOption(
      Option.builder("seed")
        .hasArg(true)
        .optionalArg(false)
        .desc("Seed for random number generator (if deterministic)")
        .type(Number.class)
        .build()
    );
  }

  private static void showHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(App.class.getSimpleName(), options);
  }

  public static void main( String[] args )
  {
    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine line = parser.parse(options, args);

      if (line.hasOption("help")) {
        showHelp();
        System.exit(0);
      }

      String project = (String)line.getOptionValue("project");
      String topic = (String)line.getOptionValue("topic");
      Long limit = (Long)line.getParsedOptionValue("limit");
      Long seed = (Long)line.getParsedOptionValue("seed");
      runMain(
        (limit == null) ? 0 : limit,
        (seed == null) ? 0 : seed);
    }
    catch (ParseException exp) {
      System.err.println("Parsing failed.  Reason: " + exp.getMessage());
      showHelp();
    }
    System.out.println( "Hello World!" );
  }

  private static String PriceQuantityToString(MarketDepth.PriceQuantity pq) {
    return pq.getQuantity() + "@" + pq.getPrice();
  }
  private static String PriceQuantityListToString(List<MarketDepth.PriceQuantity> pqs) {
    ArrayList<String> prices = new ArrayList<String>();
    for (MarketDepth.PriceQuantity pq : pqs) {
      prices.add(PriceQuantityToString(pq));
    }
    return "[" + String.join(" ", prices) + "]";
  }

  private static String OrderBookEventToString(OrderBookEvent obe) {
    switch (obe.getType()) {
      case NEW: {
        return String.format("NEW [%d] %s %d @ %d",
          obe.getOrderId(),
          obe.getSide(),
          obe.getQuantityRemaining(),
          obe.getPrice()
        );
      }
      case EXECUTED: {
        return String.format("EXE [%d] %s %d @ %d",
          obe.getOrderId(),
          obe.getSide(),
          obe.getQuantityFilled(),
          obe.getPrice()
        );
      }
      case DELETED: {
        return String.format("CAN [%d] %s %d @ %d",
          obe.getOrderId(),
          obe.getSide(),
          obe.getQuantityRemaining(),
          obe.getPrice()
        );
      }
      default: {
        return "BROKEN";
      }
    }
  }

  /*
   * We can:
   *   - Publish Simulated OrderBookEvents into PubSub or standard out
   *      - Optionally also subscribe to MarkeDepth events
   *   - Publish Market Depth events into PubSub or standard out
   * 
   * - If we subscribe to MarketDepth events, we must be publishing into PubSub OrderBook events.
   * - The only reason to subscribe to MarketDepth events is for latency testing.
   */

  public static void runMain(long limit, long seed) {
    OrderBookBuilder obb = new OrderBookBuilder();
    Iterable<OrderBookEvent> it = Simulator.getSimpleSimulator(100, limit, seed);
    for (OrderBookEvent obe : it) {
      System.out.println("Event: " + OrderBookEventToString(obe));
      MarketDepth md = obb.updateDepth(obe, 2);
      if (md != null) {
        StringBuilder sb = new StringBuilder();
        sb.append("Bids: " + PriceQuantityListToString(md.getBidsList()));
        sb.append(" Asks: " + PriceQuantityListToString(md.getOffersList()));
        if (md.getLastTrade().getQuantity() != 0) {
          sb.append(" LastTrade: " + PriceQuantityToString(md.getLastTrade()));
        }
        System.out.println(sb.toString());
      }
    }
  }
}
