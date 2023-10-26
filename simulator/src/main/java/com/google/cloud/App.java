package com.google.cloud;

import com.google.cloud.orderbook.OrderBookBuilder;
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import com.google.cloud.simulator.Simulator;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class App {

  static private Options options = new Options();

  public static final String LIMIT = "limit";

  public static final String SEED = "seed";

  public static final String CONTRACTS = "contracts";

  public static final String HELP = "help";

  public static final String ORDER_TOPIC = "ordertopic";
  public static final String MARKET_DEPTH_TOPIC = "marketdepthtopic";

  static {
    // TODO: refactor to use option groups
    options.addOption(Option.builder("h").longOpt(HELP).optionalArg(false)
        .desc("Display the usage documentation.").type(String.class).build());
    options.addOption(Option.builder("l").hasArg(true).longOpt(LIMIT).optionalArg(false)
        .desc("Limit of events to produce").type(Number.class).build());
    options.addOption(Option.builder("s").hasArg(true).longOpt(SEED).optionalArg(false)
        .desc("Seed for random number generator (if deterministic)").type(Number.class).build());
    options.addOption(Option.builder("c").hasArg(true).longOpt(CONTRACTS).optionalArg(false)
        .desc("Number of contracts to create (default is 1)").type(Number.class).build());
    options.addOption(Option.builder("o").hasArg(true).longOpt(ORDER_TOPIC).optionalArg(true)
        .desc("Pub/Sub topic to publish orders").type(String.class).build());
    options.addOption(Option.builder("m").hasArg(true).longOpt(MARKET_DEPTH_TOPIC).optionalArg(true)
        .desc("Pub/Sub topic to publish orders").type(String.class).build());
  }

  private static void showHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(App.class.getSimpleName(), options);
  }

  public static void main(String[] args) {

    String orderTopic = null;
    String marketDepthTopic = null;
    Long limit = null;
    Long seed = null;
    Long maxContracts = null;
    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine line = parser.parse(options, args);

      limit = (Long) line.getParsedOptionValue(LIMIT);

      seed = (Long) line.getParsedOptionValue(SEED);

      maxContracts = (Long) line.getParsedOptionValue(CONTRACTS);
      orderTopic = line.getOptionValue(ORDER_TOPIC);
      marketDepthTopic = line.getOptionValue(MARKET_DEPTH_TOPIC);

      if (line.hasOption(HELP)) {
        showHelp();
        System.exit(0);
      }
    } catch (ParseException e) {
      System.err.println("Parsing failed.  Reason: " + e.getMessage());
      showHelp();
      System.exit(-1);
    }

    try (EventConsumer eventConsumer = orderTopic == null ? new StandardOutputConsumer()
        : new PubSubConsumer(orderTopic, marketDepthTopic)) {

//      System.out.println(
//          "Starting simulator with max contracts=" + maxContracts + ", limit=" + limit + ", seed="
//              + seed);

      runSimulator((maxContracts == null) ? 1 : maxContracts, (limit == null) ? 0 : limit,
          (seed == null) ? 0 : seed, eventConsumer);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param limit Number of orders to generate (0 = unlimited)
   * @param seed  Random seed for running simulator (0 = standard method)
   */
  public static void runSimulator(long maxContracts, long limit, long seed,
      EventConsumer eventConsumer) {

    OrderBookBuilder obb = new OrderBookBuilder();
    Iterator<List<OrderBookEvent>> it = Simulator.getComplexSimulator(0, maxContracts, 100, limit,
        seed);
    while (it.hasNext()) {
      for (OrderBookEvent orderBookEvent : it.next()) {
        eventConsumer.accept(orderBookEvent);

        // Modify the orderbook
        obb.mutate(orderBookEvent);

        // Produce the latest MarketDepth
        MarketDepth marketDepth = obb.produceResult(2, true);

        // If there's anything new in the MarketDepth, then print to stdout
        if (marketDepth != null) {
          eventConsumer.accept(marketDepth);
        }
      }
    }
  }
}
