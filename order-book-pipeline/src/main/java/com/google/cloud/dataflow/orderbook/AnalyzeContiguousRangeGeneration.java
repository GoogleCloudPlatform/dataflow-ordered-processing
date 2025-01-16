/*
 * Copyright 2025 Google LLC
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

import org.apache.beam.sdk.extensions.ordered.ContiguousSequenceRange;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyzeContiguousRangeGeneration extends PTransform<PBegin, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(AnalyzeContiguousRangeGeneration.class);
  private static final String LATEST_RANGE_SIDE_INPUT = "latestRange";
  private final PCollectionView<Iterable<ContiguousSequenceRange>> sideInput;

  public AnalyzeContiguousRangeGeneration(
      PCollectionView<Iterable<ContiguousSequenceRange>> sideInput) {
    this.sideInput = sideInput;
  }

  @Override
  public PDone expand(PBegin input) {
    input.getPipeline()
        .apply("Generate Sequence",
            GenerateSequence.from(0).withRate(1, Duration.standardSeconds(20)))
        .apply("Log range",
            ParDo.of(new SideInputEmitter()).withSideInput(LATEST_RANGE_SIDE_INPUT, sideInput));
    return PDone.in(input.getPipeline());
  }

  static class SideInputEmitter
      extends DoFn<Long, Void> {

    @ProcessElement
    public void produceCompletedRange(
        @SideInput(LATEST_RANGE_SIDE_INPUT) Iterable<ContiguousSequenceRange> sideInput) {
      LOG.info("Latest range: " + ContiguousSequenceRange.largestRange(sideInput));
    }
  }
}
