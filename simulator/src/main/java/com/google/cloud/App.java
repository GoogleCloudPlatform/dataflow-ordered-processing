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

package com.google.cloud;

import com.google.cloud.orderbook.OrderBookBuilder;
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import com.google.cloud.orderbook.MatcherContext;
import com.google.cloud.simulator.Simulator;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
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

  static private final Options options = new Options();

  public static final String LIMIT = "limit";

  public static final String DURATION = "duration";

  public static final String SEED = "seed";

  public static final String DEGREE = "degree";

  public static final String RATE = "rate";

  public static final String SIM_TIME = "simtime";

  public static final String CONTRACTS = "contracts";

  public static final String HELP = "help";

  public static final String ORDER_TOPIC = "ordertopic";
  public static final String MARKET_DEPTH_TOPIC = "marketdepthtopic";
  public static final String REGION = "region";

  static {
    // TODO: refactor to use option groups to make sure the two topics are always provided
    options.addOption(Option.builder("h").longOpt(HELP).optionalArg(false)
        .desc("Display the usage documentation.").type(String.class).build());
    options.addOption(Option.builder("l").hasArg(true).longOpt(LIMIT).optionalArg(false)
        .desc("Limit of events to produce").type(Number.class).build());
    options.addOption(Option.builder("d").hasArg(true).longOpt(DURATION).optionalArg(false)
        .desc("Duration of exchange before finishing (format in ISO-8601, e.g., PT2M for 2 minutes)").type(String.class).build());
    options.addOption(Option.builder("s").hasArg(true).longOpt(SEED).optionalArg(false)
        .desc("Seed for random number generator (if deterministic)").type(Number.class).build());
    options.addOption(Option.builder("g").hasArg(true).longOpt(DEGREE).optionalArg(false)
        .desc("Degree for distribution of frequency of orders (0 is unform, 1 is linear, 2 for ^2, etc)").type(Number.class).build());
    options.addOption(Option.builder("c").hasArg(true).longOpt(CONTRACTS).optionalArg(false)
        .desc("Number of contracts to create (default is 1)").type(Number.class).build());
    options.addOption(Option.builder("o").hasArg(true).longOpt(ORDER_TOPIC).optionalArg(true)
        .desc("Pub/Sub topic to publish orders").type(String.class).build());
    options.addOption(Option.builder("m").hasArg(true).longOpt(MARKET_DEPTH_TOPIC).optionalArg(true)
        .desc("Pub/Sub topic to publish market depth").type(String.class).build());
    options.addOption(Option.builder("r").hasArg(true).longOpt(RATE).optionalArg(true)
        .desc("Rate for event generation (per second, minimum 10)").type(Number.class).build());
    options.addOption(Option.builder("t").hasArg(false).longOpt(SIM_TIME).optionalArg(true)
        .desc("Use simulated time rather than real time (requires rate for events per second)")
        .build());
    options.addOption(Option.builder("z").hasArg(true).longOpt(REGION).optionalArg(true)
        .desc("Pub/Sub region to publish to").build());
  }

  private static void showHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(App.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {

    String orderTopic = null;
    String marketDepthTopic = null;
    String region = null;
    Long limit = null;
    Long seed = null;
    Long maxContracts = null;
    Long eventsPerSecond = null;
    long maxSeconds = 0;
    Long degree = null;
    boolean simtime = false;
    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine line = parser.parse(options, args);

      limit = (Long)line.getParsedOptionValue(LIMIT);

      if (line.hasOption(DURATION)) {
        maxSeconds = Duration.parse(line.getOptionValue(DURATION)).getSeconds();
      }

      seed = (Long) line.getParsedOptionValue(SEED);

      degree = (Long) line.getParsedOptionValue(DEGREE);

      eventsPerSecond = (Long) line.getParsedOptionValue(RATE);

      simtime = line.hasOption(SIM_TIME);

      maxContracts = (Long) line.getParsedOptionValue(CONTRACTS);
      orderTopic = line.getOptionValue(ORDER_TOPIC);
      marketDepthTopic = line.getOptionValue(MARKET_DEPTH_TOPIC);
      region = line.getOptionValue(REGION);

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
        : new PubSubConsumer(orderTopic, marketDepthTopic, region)) {

      runSimulator(
          (maxContracts == null) ? 1 : maxContracts,
          (limit == null) ? 0 : limit,
          maxSeconds,
          (seed == null) ? 0 : seed,
        (eventsPerSecond == null) ? 0 : eventsPerSecond,
        simtime,
        (degree == null) ? 2 : degree,
        eventConsumer);
    }
  }

  /**
   * @param limit Number of orders to generate (0 = unlimited)
   * @param seed  Random seed for running simulator (0 = standard method)
   */
  public static void runSimulator(
    long maxContracts, long limit, long maxSeconds, long seed,
    long eventsPerSecond, boolean simTime, long degree,
    EventConsumer eventConsumer) {

    String sessionId = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm").format(LocalDateTime.now());

    // Initialize the MatcherContext
    //
    // This object governs the rate of order book events and the timestamp that is
    // recorded on them.
    MatcherContext.Builder builder;
    if (eventsPerSecond > 0) {
      if (simTime) {
        builder = MatcherContext.buildSimulated(sessionId, eventsPerSecond);
      } else {
        builder = MatcherContext.buildThrottled(sessionId, eventsPerSecond);
      }
    } else {
      if (simTime) {
        System.out.println("Cannot specify simulated time with no rate!");
        System.exit(1);
      }
      builder = MatcherContext.build(sessionId);
    }
    if (maxSeconds > 0)
      builder.withMaxSeconds(maxSeconds);
    if (limit > 0)
      builder.withMaxEvents(limit);

    MatcherContext context = builder.build();
    OrderBookBuilder obb = new OrderBookBuilder();

    Iterator<List<OrderBookEvent>> it = Simulator.getComplexSimulator(context, maxContracts, 100, seed, degree);
    while (it.hasNext()) {
      for (OrderBookEvent orderBookEvent : it.next()) {
        eventConsumer.accept(orderBookEvent);

        // Modify the orderbook
        obb.processEvent(orderBookEvent);

        // Produce the latest MarketDepth
        MarketDepth marketDepth = obb.getCurrentMarketDepth(2, true);

        // If there's anything new in the MarketDepth, then print to stdout
        if (marketDepth != null) {
          eventConsumer.accept(marketDepth);
        }
      }
    }
  }
}
