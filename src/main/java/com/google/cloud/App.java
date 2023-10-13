package com.google.cloud;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.google.cloud.orderbook.OrderBookBuilder;
import com.google.cloud.simulator.MarketDepth;
import com.google.cloud.simulator.OrderBookEvent;
import com.google.cloud.simulator.Simulator;

public class App 
{
  static private Options options = new Options();
  static {
    options.addOption(
      Option.builder("h")
        .longOpt("help")
        .optionalArg(false)
        .desc("Limit of events to produce")
        .type(Number.class)
        .build()
    );
    options.addOption(
      Option.builder("l")
        .hasArg(true)
        .longOpt("limit")
        .optionalArg(false)
        .desc("Limit of events to produce")
        .type(Number.class)
        .build()
    );
    options.addOption(
      Option.builder("s")
        .hasArg(true)
        .longOpt("seed")
        .optionalArg(false)
        .desc("Seed for random number generator (if deterministic)")
        .type(Number.class)
        .build()
    );
    options.addOption(
      Option.builder("c")
        .hasArg(true)
        .longOpt("contracts")
        .optionalArg(false)
        .desc("Number of contracts to create (default is 1)")
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

      Long limit = (Long)line.getParsedOptionValue("limit");
      Long seed = (Long)line.getParsedOptionValue("seed");
      Long maxContracts = (Long)line.getParsedOptionValue("contracts");
      runSimulator(
        (maxContracts == null) ? 1 : maxContracts,
        (limit == null) ? 0 : limit,
        (seed == null) ? 0 : seed);
    }
    catch (ParseException exp) {
      System.err.println("Parsing failed.  Reason: " + exp.getMessage());
      showHelp();
    }
  }

  /**
   * @param limit Number of orders to generate (0 = unlimited)
   * @param seed  Random seed for running simulator (0 = standard method)
   */
  public static void runSimulator(long maxContracts, long limit, long seed) {
    OrderBookBuilder obb = new OrderBookBuilder();
    Iterator<List<OrderBookEvent>> it = Simulator.getComplexSimulator(
      0,
      maxContracts,
      100,
      limit,
      seed
    );
    while (it.hasNext()) {
      for (OrderBookEvent obe : it.next()) {
        System.out.println("Event: " + OrderBookEventToString(obe));

        // Modify the orderbook
        obb.mutate(obe);

        // Produce the latest MarketDepth
        MarketDepth md = obb.produceResult(2, true);

        // If there's anything new in the MarketDepth, then print to stdout
        if (md != null) {
          System.out.println(MarketDepthToString(md));
        }
      }
    }
  }

  // Stringify MarketDepth in a compact way
  private static String MarketDepthToString(MarketDepth md) {
    StringBuilder sb = new StringBuilder();
    sb.append("Id:" + md.getContractId() + " Seq:" + md.getContractSeqId() + " ");
    sb.append("Bids: " + PriceQuantityListToString(md.getBidsList()) + " ");
    sb.append("Asks: " + PriceQuantityListToString(md.getOffersList()));
    if (md.getLastTrade().getQuantity() != 0) {
      sb.append(" LastTrade: " + PriceQuantityToString(md.getLastTrade()));
    }
    return sb.toString();
  }

  // Stringify PriceQuantity in a compact way
  private static String PriceQuantityToString(MarketDepth.PriceQuantity pq) {
    return pq.getQuantity() + "@" + pq.getPrice();
  }

  // Stringify PriceQuantity List in a compact way
  private static String PriceQuantityListToString(List<MarketDepth.PriceQuantity> pqs) {
    ArrayList<String> prices = new ArrayList<String>();
    for (MarketDepth.PriceQuantity pq : pqs) {
      prices.add(PriceQuantityToString(pq));
    }
    return "[" + String.join(" ", prices) + "]";
  }

  // Stringify OrderBookEvent in a compact way
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

}
