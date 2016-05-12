package org.cstamas.vertx.content;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;

/**
 * {@link Transport} implements actual transport for sending content.
 */
public interface Transport
{
  String name();

  void send(JsonObject contentHandle, FlowControl flowControl, ReadStream<Buffer> stream);

  ReadStream<Buffer> receive(final JsonObject contentHandle, final FlowControl flowControl);
}
