package org.cstamas.vertx.content;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;

/**
 * {@link Transport} implements actual transport for sending content.
 */
public interface Transport
{
  void send(JsonObject contentHandle,
            FlowControl flowControl,
            ReadStream<Buffer> stream);

  void receive(JsonObject contentHandle,
               FlowControl flowControl,
               Handler<AsyncResult<ReadStream<Buffer>>> streamHandler);
}
