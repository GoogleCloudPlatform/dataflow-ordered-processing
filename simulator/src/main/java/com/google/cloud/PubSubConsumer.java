package com.google.cloud;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.StatsTracker.Stats;
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.TimerTask;
import java.util.function.Consumer;
import org.threeten.bp.Duration;

public class PubSubConsumer implements EventConsumer {

  private final Publisher orderPublisher;
  private final Publisher marketDepthPublisher;

  private final int STATS_FREQUENCY = 5;
  private final Stats orderStats = new Stats("orders");
  private final Stats marketDepthStats = new Stats("market-depths");
  private final TimerTask statsLogger = StatsTracker.logStats(STATS_FREQUENCY, orderStats, marketDepthStats);

  public PubSubConsumer(String orderTopic, String marketDepthTopic) throws IOException {

    BatchingSettings settings = BatchingSettings.newBuilder()
      .setElementCountThreshold(10*1000L) // default: 100
      .setRequestByteThreshold(10*1024L)  // default: 1000 bytes
      .setDelayThreshold(Duration.ofMillis(50))     // default: 1ms
      .build();

    // NOTE - this is where .setEndpoint() can be called for regional endpoints and
    //        .setEnableMessageOrdering()
    orderPublisher = Publisher.newBuilder(TopicName.parse(orderTopic))
      .setBatchingSettings(settings)
      .build();
    marketDepthPublisher = Publisher.newBuilder(TopicName.parse(marketDepthTopic))
      .setBatchingSettings(settings)
      .build();
  }

  static public Consumer<MarketDepth> publishMarketDepth(Consumer<PubsubMessage> consumer) {
    return payload -> {
      PubsubMessage.Builder b = PubsubMessage.newBuilder();
      b.setData(payload.toByteString());

      consumer.accept(b.build());
    };
  }

  private static void publish(Publisher publisher, PubsubMessage message, Stats stats) {

    // Capture stats and publish
    final long messageSize = message.getSerializedSize();
    final long pubTime = System.nanoTime();
    ApiFuture<String> future = publisher.publish(message);

    ApiFutures.addCallback(future, new ApiFutureCallback<>() {

      @Override
      public void onFailure(Throwable throwable) {
        if (throwable instanceof ApiException) {
          ApiException apiException = ((ApiException) throwable);
          // details on the API exception
          System.out.println("Code: " + apiException.getStatusCode().getCode());
          System.out.println("isRetryable: " + apiException.isRetryable());
        }
        System.out.println("Error publishing message: " + throwable.getMessage());
      }

      // Track stats on confirmed publish
      @Override
      public void onSuccess(String messageId) {
        stats.add(System.nanoTime() - pubTime, 1, messageSize);
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public void accept(OrderBookEvent orderBookEvent) {
    PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder();
    messageBuilder.setData(orderBookEvent.toByteString());

    publish(orderPublisher, messageBuilder.build(), orderStats);
  }

  @Override
  public void accept(MarketDepth marketDepth) {
    PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder();
    messageBuilder.setData(marketDepth.toByteString());

    publish(marketDepthPublisher, messageBuilder.build(), marketDepthStats);
  }

  @Override
  public void close() {
    orderPublisher.shutdown();
    marketDepthPublisher.shutdown();
    statsLogger.cancel();
  }
}
