package org.cstamas.vertx.content.examples;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.cstamas.vertx.content.ContentManager;

/**
 * Created by cstamas on 11/05/16.
 */
public class ContentSenderVerticle
    extends AbstractVerticle
{
  private static final Logger log = LoggerFactory.getLogger(ContentSenderVerticle.class);

  @Override
  public void init(final Vertx vertx, final Context context) {
    super.init(vertx, context);
  }

  @Override
  public void start(final Future<Void> startFuture) throws Exception {
    ContentManager contentManager = ContentManager.http(vertx, new HttpServerOptions().setHost("localhost").setPort(8081));
    //ContentManager contentManager = ContentManager.eventBus(vertx);

    vertx.eventBus().consumer("sendFile", handler(contentManager));
    super.start(startFuture);
  }

  private Handler<Message<String>> handler(final ContentManager contentManager) {
    return message -> {
      vertx.fileSystem().open(
          message.body(),
          new OpenOptions(),
          fileResult -> {
            log.info("S: " + message.body() + " (" + fileResult.result() + ")");
            contentManager.send(fileResult.result(),
                ch -> {
                  JsonObject contentHandle = ch.result();
                  contentHandle.put("name", message.body());
                  vertx.eventBus().send(ContentReceiverVerticle.ADDRESS, contentHandle);
                }
            );
          }
      );
    };
  }
}
