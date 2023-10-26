package com.google.cloud;

import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import java.io.IOException;
import java.util.function.Consumer;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.StatsTracker.Stats;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

public class PubSubPublisher {
  static public Consumer<MarketDepth> publishMarketDepth(Consumer<PubsubMessage> consumer) {
    return payload -> {
      PubsubMessage.Builder b = PubsubMessage.newBuilder();
      b.setOrderingKey(Long.toString(payload.getContractId()));
      b.setData(payload.toByteString());

      consumer.accept(b.build());
    };
  }

  static public Consumer<OrderBookEvent> publishOrderBookEvent(Consumer<PubsubMessage> consumer) {
    return payload -> {
      PubsubMessage.Builder b = PubsubMessage.newBuilder();
      b.setOrderingKey(Long.toString(payload.getContractId()));
      b.setData(payload.toByteString());

      consumer.accept(b.build());
    };
  }

  static public Consumer<PubsubMessage> publishWithErrorHandlerExample(
    String projectId,
    String topicId,
    Stats stats) 
      throws IOException, InterruptedException {

    // Create a publisher instance with default settings bound to the topic
    final TopicName topicName = TopicName.of(projectId, topicId);
    final Publisher publisher = Publisher.newBuilder(topicName).build();

    return new Consumer<PubsubMessage>() {

      @Override
      public void accept(PubsubMessage payload) {

        // Capture stats and publish
        final long messageSize = payload.getSerializedSize();
        final long pubTime = System.nanoTime();
        ApiFuture<String> future = publisher.publish(payload);

        ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

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
    };
  }
}
