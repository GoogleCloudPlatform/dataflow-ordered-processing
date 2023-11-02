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
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.ordered.OrderedEventProcessorResult;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingStatus;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderBookProcessingPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(OrderBookProcessingPipeline.class);

  public interface Options extends GcpOptions {

    @Description("Subscription to read order book events from")
    @Required
    String getSubscription();

    void setSubscription(String value);

    @Description("BigQuery table to write Order Books to")
    @Required
    String getOrderBookTable();

    void setOrderBookTable(String value);

    @Description("(Optional) BigQuery table to write processing status to")
    String getStatusTable();

    void setStatusTable(String value);

    @Description("Order Book depth (default is 5)")
    @Required
    @Default.Integer(5)
    int getOrderBookDepth();

    void setOrderBookDepth(int depth);

    @Description("Include the last trade in the order book? (default is yes)")
    @Default.Boolean(true)
    boolean isIncludeLastTrade();

    void setIncludeLastTrade(boolean value);
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

    OrderedEventProcessorResult<Long, MarketDepth> processingResults = orderBookEvents
        .apply("Convert to KV", ParDo.of(new ConvertOrderToKV()))
        .apply("Build Order Book", new OrderBookBuilderTransform(
            options.getOrderBookDepth(), options.isIncludeLastTrade()));

    SerializableFunction<KV<Long, MarketDepth>, TableRow> marketDepthToRowConverter = (e) -> {
      MarketDepth marketDepth = e.getValue();
      TableRow result = new TableRow();
      result.set("contract_id", marketDepth.getContractId());
      return result;
    };

    SerializableFunction<KV<Long, OrderedProcessingStatus>, TableRow> processingStatusToRowConverter = (processingStatus) -> {

      TableRow result = new TableRow();
      return result;
    };

    WriteResult marketDepthWriteResult = processingResults.output().apply(
        "Persist Market Depth to BigQuery",
        BigQueryIO.<KV<Long, MarketDepth>>write()
            .to(options.getOrderBookTable())
            .withFormatFunction(marketDepthToRowConverter)
            .withMethod(Method.STORAGE_WRITE_API)
            .withAutoSharding()
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
    );

    marketDepthWriteResult.getFailedStorageApiInserts().apply("Process Failed Market Depth Inserts",
        ParDo.of(new FailedInsertProcessor(options.getOrderBookTable())));

    if (options.getStatusTable() != null) {
      WriteResult processingStatusWriteResult = processingResults.processingStatuses().apply(
          "Persist Processing Status to BigQuery",
          BigQueryIO.<KV<Long, OrderedProcessingStatus>>write()
              .to(options.getStatusTable())
              .withFormatFunction(processingStatusToRowConverter)
              .withMethod(Method.STORAGE_WRITE_API)
              .withAutoSharding()
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
      );
      processingStatusWriteResult.getFailedStorageApiInserts()
          .apply("Process Failed Processing Status Inserts",
              ParDo.of(new FailedInsertProcessor(options.getStatusTable())));
    }

    pipeline.run();
  }

  static class FailedInsertProcessor extends DoFn<BigQueryStorageApiInsertError, PDone> {

    private String table;

    FailedInsertProcessor(String table) {
      this.table = table;
    }

    @ProcessElement
    public void process(@Element BigQueryStorageApiInsertError error) {
      // In production pipelines logging errors is not recommended. A proper sink should be used instead.
      LOG.error("Failed to insert into " + table + " : " + error);
    }
  }
}
