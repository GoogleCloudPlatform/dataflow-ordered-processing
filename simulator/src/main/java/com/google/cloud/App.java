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

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.cloud.orderbook.MatcherContext;
import com.google.cloud.orderbook.OrderBookBuilder;
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import com.google.cloud.simulator.Simulator;

public class App {

  // Program parameters
  @Parameter(names = {"--help", "-h"}, description = "Display the usage documentation", help = true)
  private boolean help;

  // Simulator Generator
  @Parameter(names = {"--limit"}, description = "Limit of events to produce")
  private long limit = 0;
  @Parameter(names = {
      "--seed"}, description = "Seed for random number generator (if deterministic)")
  private long seed = 0;
  @Parameter(names = {
      "--degree"}, description = "Degree for distribution of frequency of orders (0 is uniforn, 1 is linear, 2 for ^2, etc)")
  private long degree = 2;
  @Parameter(names = {"--contracts"}, description = "Number of contracts to create")
  private long contracts = 100;
  @Parameter(names = {"--zero_contract"}, description = "Contract number to start from (excluding)")
  private long zero_contract = 0;
  @Parameter(names = {"--rate"}, description = "Rate for event generation (per second, minimum 10)")
  private long rate = 10;
  @Parameter(names = {"--depth"}, description = "Depth of marketdepth captured")
  private int depth = 2;
  @Parameter(names = {
      "--simtime"}, description = "Use simulated time rather than real time (requires rate for events per second)")
  private long simtime = 0;
  @Parameter(names = {
      "--duration"}, description = "Duration of the exchange before finishing (format in ISO-8601, e.g., PT2M for 2 minutes")
  private String duration = null;
  @Parameter(names = {
      "--json"}, description = "Output json when not publishing to Pub/Sub")
  private boolean json = false;
  @Parameter(names = {
      "--avro"}, description = "Avro prefix for output")  
  private String avroPrefix = null;

  // Related to PubSub
  @Parameter(names = {"--region"}, description = "Pub/Sub region to publish to")
  private String region = null;
  @Parameter(names = {"--ordertopic"}, description = "Pub/Sub topic to publish orders")
  private String orderTopic = null;
  @Parameter(names = {"--marketdepthtopic"}, description = "Pub/Sub topic to publish market depth")
  private String marketDepthTopic = null;

  public static void main(String argv[]) {

    // Initialize the parsing objects
    App app = new App();
    JCommander parser = JCommander
        .newBuilder()
        .addObject(app)
        .build();

    try {

      // Parse the user-specifiedc arguments
      parser.parse(argv);
      if (app.help) {
        parser.usage();
        System.exit(1);
      }

      // Run the simulator
      app.runSimulator();
    }

    // Failure parsing an argument (show usage)
    catch (ParameterException e) {
      System.out.println(
          String.format("Failed parsing command line: %s", e.getMessage()));
      parser.usage();
      System.exit(1);
    }

    // Runtime exception -- just show error
    catch (Exception e) {
      e.printStackTrace();
      System.out.println(
          String.format("Failed running: %s", e.getMessage()));
      System.exit(1);
    }
  }

  /**
   * Run the simulator based on the App arguments
   *
   * @throws Exception
   */
  private void runSimulator() throws Exception {
    try (EventConsumer eventConsumer = buildConsumer()) {

      OrderBookBuilder obb = new OrderBookBuilder();
      Iterator<List<OrderBookEvent>> it = buildSimulator();

      while (it.hasNext()) {
        for (OrderBookEvent orderBookEvent : it.next()) {

          // Publish the order book event
          eventConsumer.accept(orderBookEvent);

          // Modify the orderbook
          obb.processEvent(orderBookEvent);

          // Produce the latest MarketDepth
          MarketDepth marketDepth = obb.getCurrentMarketDepth(depth, true);

          // If there's anything new in the MarketDepth, then publish the market depth
          if (marketDepth != null) {
            eventConsumer.accept(marketDepth);
          }
        }
      }
    }
  }

  /**
   * Build the consumer (publishing) objects based on the App arguments
   *
   * @return EventConsumer
   * @throws ParameterException
   * @throws IOException
   */
  EventConsumer buildConsumer() throws ParameterException, IOException {
    if (orderTopic == null && marketDepthTopic == null && region == null) {
      if (json) {
        return new JSONOutputConsumer();
      } else if (avroPrefix != null) {
        return new AvroOutputConsumer(avroPrefix);
      } else {
        return new StandardOutputConsumer();
      }
    }

    if (orderTopic != null && marketDepthTopic != null) {
      return new PubSubConsumer(orderTopic, marketDepthTopic, region);
    }

    throw new ParameterException("Please specify both --orderTopic and --marketDepthTopic");
  }

  /**
   * Build the simulator based on the App arguments
   *
   * @return Iterator<List < OrderBookEvent>>
   */
  Iterator<List<OrderBookEvent>> buildSimulator() {
    return Simulator.getComplexSimulator(
        buildMatcherContext(),
        contracts,
        zero_contract,
        100,
        seed,
        degree);
  }

  /**
   * Build the MatcherContext based on the App arguments
   *
   * @return MatcherContext
   * @throws ParameterException
   */
  MatcherContext buildMatcherContext() throws ParameterException {
    String sessionId = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm").format(LocalDateTime.now());

    MatcherContext.Builder builder;
    if (rate > 0) {
      if (simtime > 0) {
        builder = MatcherContext.buildSimulated(sessionId, rate);
        builder.withStartTimeMillis(simtime);
      } else {
        builder = MatcherContext.buildThrottled(sessionId, rate);
      }
    } else {
      if (simtime > 0) {
        throw new ParameterException("Cannot specify simulated time (--simtime) with no rate");
      }
      builder = MatcherContext.build(sessionId);
    }

    if (duration != null) {
      builder.withMaxSeconds(Duration.parse(duration).getSeconds());
    }

    if (limit > 0) {
      builder.withMaxEvents(limit);
    }

    return builder.build();
  }
}
