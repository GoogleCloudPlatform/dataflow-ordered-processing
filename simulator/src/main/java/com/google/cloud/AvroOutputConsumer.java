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

import com.google.protobuf.GeneratedMessageV3;
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.MarketDepth.PriceQuantity;
import com.google.cloud.orderbook.model.OrderBookEvent;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.file.DataFileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.IOException;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.protobuf.ProtobufDatumWriter;

public class AvroOutputConsumer implements EventConsumer {

  private Schema obeSchema = ProtobufData.get().getSchema(OrderBookEvent.class);
  private DataFileWriter<OrderBookEvent> obeWriter = new DataFileWriter<>(
    ProtobufData.get().createDatumWriter(obeSchema)
  );
  private Schema mdSchema = ProtobufData.get().getSchema(MarketDepth.class);
  private DataFileWriter<MarketDepth> mdWriter = new DataFileWriter<>(
    ProtobufData.get().createDatumWriter(mdSchema)
  );

  AvroOutputConsumer(String prefix) throws IOException {
    CodecFactory codec = CodecFactory.deflateCodec(6);
    obeWriter.setCodec(codec);
    obeWriter.create(obeSchema, new File(prefix + "-obe.avro"));    
    mdWriter.setCodec(codec);
    mdWriter.create(mdSchema, new File(prefix + "-md.avro"));    
  }

  @Override
  public void accept(OrderBookEvent orderBookEvent) throws IOException {
    obeWriter.append(orderBookEvent);
  }

  @Override
  public void accept(MarketDepth marketDepth) throws IOException {
    mdWriter.append(marketDepth);
  }

  @Override
  public void close() throws Exception {
    obeWriter.close();
    mdWriter.close();
  }
}
