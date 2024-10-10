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

package com.google.cloud.dataflow.orderbook;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest.MissingValueInterpretation;
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.ordered.OrderedEventProcessorResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderBookProcessingPipeline {

  @SuppressWarnings({"unused"})
  private static final Logger LOG = LoggerFactory.getLogger(OrderBookProcessingPipeline.class);

  public interface Options extends GcpOptions {

    @Description("Subscription to read order book events from")
    @Required
    String getSubscription();

    void setSubscription(String value);

    @Description("BigQuery table to store market depths")
    @Required
    String getMarketDepthTable();

    void setMarketDepthTable(String value);

    @Description("(Optional) BigQuery table to store processing statuses")
    String getProcessingStatusTable();

    void setProcessingStatusTable(String value);

    @Description("(Optional) BigQuery table to store order events")
    String getOrderEventTable();

    void setOrderEventTable(String value);

    @Description("Order Book depth (default is 5)")
    @Required
    @Default.Integer(5)
    int getOrderBookDepth();

    void setOrderBookDepth(int depth);

    @Description("Include the last trade in the order book? (default is yes)")
    @Required
    @Default.Boolean(true)
    boolean isIncludeLastTrade();

    void setIncludeLastTrade(boolean value);

    @Description("Maximum number of elements to output per each bundle. Total size of the data is not exceed 2GB.")
    @Default.Integer(10000)
    int getMaxOutputElementsPerBundle();

    void setMaxOutputElementsPerBundle(int value);

    @Description("If set true  per-key-sequencing will be used, otherwise global sequencing.")
    @Default.Boolean(true)
    boolean isSequencingPerKey();

    void setSequencingPerKey(boolean value);
  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(Options.class);
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    OrderBookProcessingPipeline orderBookProcessingPipeline = new OrderBookProcessingPipeline();
    orderBookProcessingPipeline.run(options);
  }

  private void run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    PCollection<OrderBookEvent> orderBookEvents = pipeline.apply("Read from PubSub",
        PubsubIO.readProtos(
            OrderBookEvent.class).fromSubscription(options.getSubscription()));

    OrderedEventProcessorResult<SessionContractKey, MarketDepth, OrderBookEvent> processingResults = orderBookEvents
        .apply("Build Order Book", new OrderBookProducer(
            options.getOrderBookDepth(),
            options.isIncludeLastTrade(),
            options.getMaxOutputElementsPerBundle(),
            options.isSequencingPerKey()).produceStatusUpdatesOnEveryEvent());

    storeInBigQuery(processingResults.output(), options.getMarketDepthTable(), "Market Depth",
        new MarketDepthToTableRowConverter());

    if (options.getProcessingStatusTable() != null) {
      storeInBigQuery(processingResults.processingStatuses(), options.getProcessingStatusTable(),
          "Processing Status",
          new ProcessingStatusToTableRowConverter());
    }

    if (options.getOrderEventTable() != null) {
      storeInBigQuery(orderBookEvents, options.getOrderEventTable(),
          "Order Event",
          new OrderBookEventToTableRowConverter());
    }

    pipeline.run();
  }

  private static <Input> void storeInBigQuery(PCollection<Input> input, String tableName,
      String shortTableDescription, SerializableFunction<Input, TableRow> formatFunction) {
    WriteResult processingStatusWriteResult = input.apply(shortTableDescription + " to BQ",
        BigQueryIO.<Input>write()
            .to(tableName)
            .withFormatFunction(formatFunction)
            .withMethod(Method.STORAGE_WRITE_API)
            .withAutoSharding()
            .withTriggeringFrequency(Duration.standardSeconds(2))
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withDefaultMissingValueInterpretation(MissingValueInterpretation.DEFAULT_VALUE)
    );
    processingStatusWriteResult.getFailedStorageApiInserts()
        .apply("DLQ for " + shortTableDescription,
            new FailedBigQueryInsertProcessor(tableName));
  }

}
