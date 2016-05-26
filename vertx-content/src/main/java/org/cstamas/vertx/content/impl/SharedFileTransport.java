package org.cstamas.vertx.content.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.Transport;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.cstamas.vertx.content.impl.ContentManagerImpl.txId;

/**
 * {@link Transport} implementation that uses shared (visible by sender and receiver) file system, maybe a network
 * mounted volume or so. Note that volume path on sender and receiver end does not have to be the same paths.
 */
public class SharedFileTransport
    implements Transport
{
  private static final Logger log = LoggerFactory.getLogger(SharedFileTransport.class);

  private final Vertx vertx;

  private final String directoryPath;

  public SharedFileTransport(final Vertx vertx, final String directoryPath) {
    this.vertx = checkNotNull(vertx);
    this.directoryPath = directoryPath;
    log.info("Created " + getClass().getSimpleName() + " (path=" + directoryPath + ")");
  }

  private String resolve(String path) {
    return directoryPath + "/" + path;
  }

  private String notifyAddress(String txId) {
    return SharedFileTransport.class.getName() + "." + txId;
  }

  @Override
  public void send(final JsonObject contentHandle,
                   final ReadStream<Buffer> stream)
  {
    String txId = txId(contentHandle);
    vertx.fileSystem().open(
        resolve(txId),
        new OpenOptions().setCreateNew(true),
        r -> {
          if (r.succeeded()) {
            AsyncFile file = r.result();
            stream.endHandler(
                v -> {
                  file.flush(
                      f -> {
                        file.close(
                            c -> {
                              // send notify to receiver
                              vertx.eventBus().send(
                                  notifyAddress(txId),
                                  new JsonObject().put("file", "available")
                              );
                            }
                        );
                      }
                  );
                }
            );
            Pump.pump(stream, file).start();
          }
          else {
            throw new IllegalStateException("Cannot write to file", r.cause());
          }
        }
    );
  }

  @Override
  public void receive(final JsonObject contentHandle,
                      final Handler<AsyncResult<ReadStream<Buffer>>> streamHandler)
  {
    String txId = txId(contentHandle);
    MessageConsumer<JsonObject> receiver = vertx.eventBus().consumer(notifyAddress(txId));
    receiver.handler(
        m -> {
          vertx.fileSystem().open(
              resolve(txId),
              new OpenOptions().setCreate(false),
              r -> {
                try {
                  if (r.succeeded()) {
                    streamHandler.handle(Future.succeededFuture(r.result()));
                  }
                  else {
                    streamHandler.handle(Future.failedFuture(r.cause()));
                  }
                }
                finally {
                  receiver.unregister();
                }
              }
          );
        }
    );
  }
}

