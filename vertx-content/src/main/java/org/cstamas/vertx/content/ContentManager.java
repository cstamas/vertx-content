package org.cstamas.vertx.content;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.impl.ContentManagerImpl;
import org.cstamas.vertx.content.impl.HttpTransport;
import org.cstamas.vertx.content.impl.SharedFileTransport;

/**
 * Content manager, to send streams across verticle instances backed by various strategies and channels.
 */
public interface ContentManager
{
  /**
   * Creates {@link ContentManager} instance using {@link HttpTransport}.
   */
  static ContentManager http(final Vertx vertx, final HttpServerOptions httpServerOptions) {
    return new ContentManagerImpl(vertx, new HttpTransport(vertx, httpServerOptions));
  }

  /**
   * Creates {@link ContentManager} instance using {@link SharedFileTransport}.
   */
  static ContentManager sharedFile(final Vertx vertx, final String sharedDirectory) {
    return new ContentManagerImpl(vertx, new SharedFileTransport(vertx, sharedDirectory));
  }

  /**
   * Should be called by content sender to send a stream. Method will return a {@link JsonObject} that needs to be
   * passed over by some means to the far end that want to receive the content.
   */
  ContentManager send(ReadStream<Buffer> stream, Handler<AsyncResult<JsonObject>> handler);

  /**
   * Should be called by content receiver, to get the content.
   */
  ContentManager receive(JsonObject contentHandle, Handler<AsyncResult<ReadStream<Buffer>>> streamHandler);
}
