package org.cstamas.vertx.content.examples;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.ContentManager;

/**
 * Content receiver verticle, that receives the file and saves it under path it got from sender appending it with
 * {@code
 * .received}.
 */
public class ContentReceiverVerticle
    extends AbstractVerticle
    implements Handler<Message<JsonObject>>
{
  private static final Logger log = LoggerFactory.getLogger(ContentReceiverVerticle.class);

  public static final String ADDRESS = "content.receiver";

  private ContentManager contentManager;

  @Override
  public void start(final Future<Void> startFuture) throws Exception {
    contentManager = Utility.create(vertx, config());
    vertx.eventBus().consumer(ADDRESS, this);
    super.start(startFuture);
  }

  @Override
  public void handle(final Message<JsonObject> message) {
    JsonObject contentHandle = message.body();
    String path = contentHandle.getString("path") + ".received";
    log.info("R: " + path);
    vertx.fileSystem().open(
        path,
        new OpenOptions(),
        oh -> {
          if (oh.failed()) {
            log.error("E:", oh.cause());
            message.fail(500, oh.cause().getMessage());
          }
          else {
            contentManager.receive(
                contentHandle,
                streamResult -> {
                  if (streamResult.failed()) {
                    log.error("R: ", streamResult.cause());
                    message.fail(500, streamResult.cause().getMessage());
                  }
                  else {
                    ReadStream<Buffer> stream = streamResult.result();
                    AsyncFile file = oh.result();
                    stream.endHandler(
                        h -> {
                          file.flush().end();
                          message.reply(new JsonObject().put("status", 201).put("path", path));
                        }
                    );
                    Pump.pump(stream, file).start();
                  }
                }
            );
          }
        }
    );
  }
}
