package org.cstamas.vertx.content.examples;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.cstamas.vertx.content.ContentManager;

/**
 * Content sender verticle, that is triggered by a message that contains {@code path} key, the path of the file to send
 * to {@link ContentReceiverVerticle}.
 */
public class ContentSenderVerticle
    extends AbstractVerticle
    implements Handler<Message<JsonObject>>
{
  private static final Logger log = LoggerFactory.getLogger(ContentSenderVerticle.class);

  public static final String ADDRESS = "sendFile";

  private ContentManager contentManager;

  @Override
  public void start(final Future<Void> startFuture) throws Exception {
    contentManager = Utility.create(vertx, config());
    vertx.eventBus().consumer(ADDRESS, this);
    super.start(startFuture);
  }

  @Override
  public void handle(final Message<JsonObject> message) {
    String path = message.body().getString("path");
    vertx.fileSystem().open(
        path,
        new OpenOptions().setCreate(false),
        fileResult -> {
          if (fileResult.succeeded()) {
            AsyncFile file = fileResult.result();
            log.info("S: " + path);
            contentManager.send(file,
                contentHandlerResult -> {
                  if (contentHandlerResult.succeeded()) {
                    JsonObject contentHandle = contentHandlerResult.result();
                    contentHandle.put("path", path);
                    vertx.eventBus().send(
                        ContentReceiverVerticle.ADDRESS,
                        contentHandle,
                        rh -> {
                          if (rh.succeeded()) {
                            message.reply(rh.result().body());
                          }
                          else {
                            ReplyException replyException = (ReplyException) rh.cause();
                            message.fail(replyException.failureCode(), replyException.getMessage());
                          }
                        });
                  }
                  else {
                    message.fail(500, contentHandlerResult.cause().getMessage());
                  }
                }
            );
          }
          else {
            log.info("File" + path + " does not exists");
            message.reply(new JsonObject().put("status", 404));
          }
        }
    );
  }
}
