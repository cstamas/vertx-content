package org.cstamas.vertx.content.examples;

import com.google.common.io.Files;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import org.cstamas.vertx.content.ContentManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class Utility
{
  private Utility() {
    // nop
  }

  private static final String sharedDirectoryPath = Files.createTempDir().getAbsolutePath();

  public static ContentManager create(final Vertx vertx, final JsonObject config) {
    final String type = config.getString("type");
    checkArgument(!isNullOrEmpty(type), "null type");
    if ("http".equals(type)) {
      final String host = config.getString("host");
      final int port = config.getInteger("port");
      return ContentManager.http(vertx, new HttpServerOptions().setHost(host).setPort(port));
    }
    else if ("sharedFile".equals(type)) {
      return ContentManager.sharedFile(vertx, sharedDirectoryPath);
    }
    else {
      throw new IllegalArgumentException("Unknown manager type: " + type);
    }
  }
}
