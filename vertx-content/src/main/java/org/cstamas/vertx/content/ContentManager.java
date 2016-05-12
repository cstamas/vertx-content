package org.cstamas.vertx.content;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.impl.ContentManagerImpl;
import org.cstamas.vertx.content.impl.EventBusTransport;
import org.cstamas.vertx.content.impl.HttpTransport;

/**
 * Content manager, to send streams across verticle instances backed by various strategies and channels.
 */
public interface ContentManager
{
  /**
   * Creates {@link ContentManager} instance using {@link EventBusTransport}.
   */
  static ContentManager eventBus(final Vertx vertx) {
    return new ContentManagerImpl(vertx, new EventBusTransport(vertx));
  }

  /**
   * Creates {@link ContentManager} instance using {@link HttpTransport}.
   */
  static ContentManager http(final Vertx vertx, final HttpServerOptions httpServerOptions) {
    return new ContentManagerImpl(vertx, new HttpTransport(vertx, httpServerOptions));
  }

  /**
   * Should be called by content sender to send a stream. Method will return a {@link JsonObject} that needs to be
   * passed over to far end that uses this same service by any means (event bus?).
   */
  ContentManager send(ReadStream<Buffer> stream, Handler<AsyncResult<JsonObject>> handler);

  /**
   * Should be called by content receiver, to actually get the content. It is detail how {@link JsonObject} content
   * handle arrives, but sending it via {@link EventBus} seems as one of the viable options.
   */
  ContentManager receive(JsonObject contentHandle, Handler<AsyncResult<ReadStream<Buffer>>> streamHandler);
}
