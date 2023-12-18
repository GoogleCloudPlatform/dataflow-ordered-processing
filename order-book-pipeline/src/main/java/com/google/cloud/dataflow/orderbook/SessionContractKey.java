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

import com.google.auto.value.AutoValue;
import com.google.cloud.dataflow.orderbook.SessionContractKey.SessionContractKeyCoder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;

@DefaultCoder(SessionContractKeyCoder.class)
@AutoValue
public abstract class SessionContractKey {

  public static SessionContractKey create(String sessionId, long contractId) {
    return new AutoValue_SessionContractKey(sessionId, contractId);
  }

  public abstract String getSessionId();

  public abstract long getContractId();

  public static class SessionContractKeyCoder extends CustomCoder<SessionContractKey> {

    public static SessionContractKeyCoder of() {
      return new SessionContractKeyCoder();
    }

    @Override
    public void encode(SessionContractKey value, OutputStream outStream)
        throws CoderException, IOException {
      StringUtf8Coder.of().encode(value.getSessionId(), outStream);
      VarLongCoder.of().encode(value.getContractId(), outStream);
    }

    @Override
    public SessionContractKey decode(InputStream inStream) throws CoderException, IOException {
      String sessionId = StringUtf8Coder.of().decode(inStream);
      long contractId = VarLongCoder.of().decode(inStream);
      return SessionContractKey.create(sessionId, contractId);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }
}
