/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.emitter.scribe;

import com.twitter.logging.BareFormatter$;
import com.twitter.logging.Level;
import com.twitter.logging.LogRecord;
import com.twitter.logging.QueueingHandler;
import com.twitter.logging.ScribeHandler;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import java.util.Base64;

public class TwitterLogScriber
{
  private QueueingHandler queueingHandler;
  private static final int MAX_QUEUE_SIZE = 1000;

  // TSerializer is not thread safe
  private final ThreadLocal<TSerializer> serializer = new ThreadLocal<TSerializer>() {
    @Override
    protected TSerializer initialValue()
    {
      return new TSerializer();
    }
  };

  public TwitterLogScriber(String scribeCategory)
  {
    ScribeHandler scribeHandler = new ScribeHandler(
        ScribeHandler.DefaultHostname(),
        ScribeHandler.DefaultPort(),
        scribeCategory,
        ScribeHandler.DefaultBufferTime(),
        ScribeHandler.DefaultConnectBackoff(),
        ScribeHandler.DefaultMaxMessagesPerTransaction(),
        ScribeHandler.DefaultMaxMessagesToBuffer(),
        BareFormatter$.MODULE$,
        scala.Option.apply((Level) null));
    queueingHandler = new QueueingHandler(scribeHandler, MAX_QUEUE_SIZE);
  }

  public void scribe(TBase thriftMessage)
      throws TException
  {
    scribe(serializeThriftToString(thriftMessage));
  }

  public void flush()
  {
    queueingHandler.flush();
  }

  public void close()
  {
    queueingHandler.close();
  }

  /**
   * Serialize a thrift object to bytes, compress, then encode as a base64 string.
   * Throws TException
   */
  private String serializeThriftToString(TBase thriftMessage)
      throws TException
  {
    return Base64.getEncoder().encodeToString(serializer.get().serialize(thriftMessage));
  }

  private void scribe(String message)
  {
    LogRecord logRecord = new LogRecord(Level.ALL, message);
    queueingHandler.publish(logRecord);
  }
}
