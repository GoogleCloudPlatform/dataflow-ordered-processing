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

package com.google.cloud.orderbook;

import com.google.cloud.orderbook.model.OrderBookEvent;
import java.util.Objects;

/*
 * Order is for the simulator.
 */
public class Order {

  public Order(long orderId, OrderBookEvent.Side side, long price, long quantity) {
    this.side = side;
    this.price = price;
    this.quantity = quantity;
    this.quantityRemaining = quantity;
    this.orderId = orderId;
  }

  final private OrderBookEvent.Side side;
  public OrderBookEvent.Side getSide() {
    return side;
  }

  final private long price;
  public long getPrice() {
    return price;
  }

  final private long quantity;
  public long getQuantity() {
    return quantity;
  }

  private long orderId = 0;
  public long getOrderId() {
    return orderId;
  }

  private long quantityRemaining;
  public long getQuantityRemaining() {
    return quantityRemaining;
  }

  public void fillOrder(long quantity) {
    this.quantityRemaining -= quantity;
  }

  @Override
  public String toString() {
    return "Order [side=" + side + ", price=" + price + ", quantity=" + quantity
        + ", orderId=" + orderId + ", quantityRemaining="
        + quantityRemaining + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Order)) {
      return false;
    }
    Order order = (Order) o;
    return price == order.price && quantity == order.quantity && orderId == order.orderId
        && quantityRemaining == order.quantityRemaining && side == order.side;
  }

  @Override
  public int hashCode() {
    return Objects.hash(side, price, quantity, orderId, quantityRemaining);
  }
}