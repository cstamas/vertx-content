package org.cstamas.vertx.content.impl;

import java.util.UUID;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.ContentManager;
import org.cstamas.vertx.content.Transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link ContentManager} implementation that establishes {@link Transport}.
 */
public class ContentManagerImpl
    implements ContentManager
{
  private static final String TX_ID = "txId";

  /**
   * Helper to ensure that requested key is in JSON.
   */
  public static String require(final JsonObject jsonObject, final String key) {
    checkArgument(jsonObject.containsKey(key), "JSON %s does not have required key %s", jsonObject, key);
    return jsonObject.getString(key);
  }

  /**
   * Helper to ensure that transaction ID is present and get it.
   */
  public static String txId(final JsonObject jsonObject) {
    return require(jsonObject, TX_ID);
  }

  private static final Logger log = LoggerFactory.getLogger(ContentManagerImpl.class);

  private final Vertx vertx;

  private final Transport transport;

  public ContentManagerImpl(final Vertx vertx, final Transport transport) {
    this.vertx = checkNotNull(vertx);
    this.transport = checkNotNull(transport);
  }

  @Override
  public ContentManagerImpl send(final ReadStream<Buffer> stream,
                                 final Handler<AsyncResult<JsonObject>> handler)
  {
    checkNotNull(stream);
    checkNotNull(handler);
    vertx.getOrCreateContext().runOnContext(
        w -> {
          Future<JsonObject> future = Future.future();
          try {
            final String txId = UUID.randomUUID().toString();
            JsonObject contentHandle = new JsonObject()
                .put(TX_ID, txId);
            transport.send(contentHandle, stream);
            future.complete(contentHandle);
          }
          catch (Exception e) {
            future.fail(e);
          }
          handler.handle(future);
        }
    );
    return this;
  }

  @Override
  public ContentManagerImpl receive(final JsonObject contentHandle,
                                    final Handler<AsyncResult<ReadStream<Buffer>>> streamHandler)
  {
    checkNotNull(contentHandle);
    checkNotNull(streamHandler);
    vertx.getOrCreateContext().runOnContext(
        w -> {
          transport.receive(
              contentHandle,
              streamHandler
          );
        }
    );
    return this;
  }
}
