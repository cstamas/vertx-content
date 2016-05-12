package org.cstamas.vertx.content;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.impl.ContentManagerImpl;
import org.cstamas.vertx.content.impl.EventBusTransport;
import org.cstamas.vertx.content.impl.HttpTransport;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Content manager, to send streams across verticle instances backed by various strategies and channels.
 */
public interface ContentManager
{
  String TXID = "txId";

  /**
   * Helper to ensure that requested key is in JSON.
   */
  static String require(final JsonObject jsonObject, final String key) {
    checkArgument(jsonObject.containsKey(key), "JSON %s does not have required key %s", jsonObject, key);
    return jsonObject.getString(key);
  }

  /**
   * Helper to ensure that transaction ID is present and get it.
   */
  static String txId(final JsonObject jsonObject) {
    return require(jsonObject, TXID);
  }

  /**
   * Creates {@link ContentManager} instance that uses {@link EventBus} for transport.
   */
  static ContentManager eventBus(Vertx vertx) {
    return new ContentManagerImpl(vertx, new EventBusTransport(vertx));
  }

  /**
   * Creates {@link ContentManager} instance that uses {@link HttpServer} for transport.
   */
  static ContentManager http(Vertx vertx) {
    return new ContentManagerImpl(vertx, new HttpTransport(vertx));
  }

  /**
   * Should be called by content sender to send a stream. Method will return a {@link JsonObject} that needs to be
   * passed over to far end that uses this same service by any means (event bus?).
   */
  ContentManager send(final ReadStream<Buffer> stream, final Handler<AsyncResult<JsonObject>> handler);

  /**
   * Should be called by content receiver, to actually get the content. It is detail how {@link JsonObject} content
   * handle arrives, but sending it via {@link EventBus} seems as one of the viable options.
   */
  ContentManager receive(final JsonObject contentHandle,
                         final Handler<AsyncResult<ReadStream<Buffer>>> streamHandler);
}
