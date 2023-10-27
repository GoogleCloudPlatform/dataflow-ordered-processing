package com.google.cloud.orderbook;

import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.MarketDepth.PriceQuantity;
import com.google.cloud.orderbook.model.OrderBookEvent;
import java.util.Arrays;
import java.util.GregorianCalendar;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of OrderBookBuilder.
 */
@RunWith(JUnit4.class)
public class OrderBookBuilderTest {

  // 2023-01-01 noon in milliseconds.
  final static long startTime = (new GregorianCalendar(2023, 0, 1, 12, 0, 0)).getTimeInMillis();

  void add(OrderBookBuilder builder, Matcher m, Order... orders) {
    for (Order o : orders) {
      for (OrderBookEvent obe : m.add(o)) {
        builder.mutate(obe);
      }
    }
  }

  @Test
  public void simpleTest() {
    OrderBookBuilder builder = new OrderBookBuilder();
    Matcher m = new Matcher(new MatcherContext(1000, startTime), 1);

    OrderFactory orderFactory = new OrderFactory();
    // Add a series of orders.
    add(builder, m, orderFactory.newOrder(OrderBookEvent.Side.BUY, 100, 100));
    add(builder, m, orderFactory.newOrder(OrderBookEvent.Side.SELL, 101, 100));

    MarketDepth d = builder.produceResult(10, false);
    Assert.assertEquals("expected depth to match", d,
        MarketDepth.newBuilder()
            .setTimestampMS(startTime)
            .setContractId(1)
            .setContractSeqId(1)
            .setSeqId(1)
            .addAllBids(Arrays.asList(
                PriceQuantity.newBuilder().setPrice(100).setQuantity(100).build()
            ))
            .addAllOffers(Arrays.asList(
                PriceQuantity.newBuilder().setPrice(101).setQuantity(100).build()
            ))
            .build());
  }
}