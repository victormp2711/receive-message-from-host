package com.dbconnect.PostgresProject;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;


public class HttpWebVerticle extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(HttpWebVerticle.class);
  public static final String MESSAGE_ADDR = "message.domic.account";

  @Override
  public void start() throws Exception {
    System.out.println("RouterVerticle is deployed");

    ConfigStoreOptions defaultConfig = new ConfigStoreOptions()
      .setType("file")
      .setFormat("json")
      .setConfig(new JsonObject().put("path", "config.json"));
    ConfigStoreOptions cliConfig = new ConfigStoreOptions()
      .setType("json")
      .setConfig(config());

    ConfigRetrieverOptions opts = new ConfigRetrieverOptions()
      .addStore(defaultConfig)
      .addStore(cliConfig);

    ConfigRetriever retriever = ConfigRetriever.create(vertx, opts);

    log.info("doConfig");

    Router router = Router.router(vertx);

    // Bind "/" to our hello message - so we are still compatible.
    router.route("/").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();
      response
        .putHeader("content-type", "text/html")
        .end("<h1>Hello from my first Vert.x 3 application</h1>");
    });

    // Serve static resources from the /assets directory
    router.route("/assets/*").handler(StaticHandler.create("assets"));
    router.post("/api/helloJs").handler(this::helloJs);
    router.post("/api/articles").handler(this::listAllArticles2);
    router.post("/api/message").consumes("application/json").handler(this::sendMessage);
    router.post("/api/message2").consumes("application/json").handler(this::sendMessage2);
    router.post("/api/accountBalance").consumes("application/json").handler(this::accountBalance);
    router.post("/api/accountBalanceLib").consumes("application/json").handler(this::accountBalanceLib);

    retriever.getConfig().onComplete(ar -> {
      if (ar.failed()) {
        log.info("Failed to open file configure");
      } else {
        JsonObject env = ar.result();
        createHttpServer2(env, router);
        log.info("✅ doConfig properties is ready");
      }
    });

  }

  public void createHttpServer2(JsonObject env, Router router) {
    vertx.createHttpServer().requestHandler(router)
      .listen(env.getInteger("http.port_qa", 80), result -> {
        if (result.succeeded()) {
          log.info("HTTP server running on port {}", result.result().actualPort());
        } else {
          log.error("Could not start a HTTP server ", result.cause());
        }
      });
  }

  private void listAllArticles2(RoutingContext context) {
    String uuid = java.util.UUID.randomUUID().toString();
    final String message = context.request().getParam("message");
    log.info("message: " + message);
    vertx.eventBus().request("incoming.message.articles", message, reply -> {
      if (reply.succeeded()) {
        context.json(reply.result().body());
      } else {
        reply.cause().printStackTrace();
        JsonObject body = new JsonObject()
          .put("code", 500)
          .put("message", reply.cause().getMessage());
        context.json(body);
      }
    });
  }

  private void sendMessage(RoutingContext context) {
    String uuid = java.util.UUID.randomUUID().toString();
    final String message = context.request().getParam("cuenta");
    log.info("cuenta: " + message);
    // creating json object for message
    JsonObject entries = new JsonObject();
    entries.put("id", uuid);
    entries.put("message", message);
    long yourmilliseconds = System.nanoTime();
    SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm");
    Date resultdate = new Date(yourmilliseconds);
    entries.put("time", resultdate.toString());

    vertx.eventBus().request("incoming.message", entries, reply -> {
      if (reply.succeeded()) {
        log.info("ok" + entries);
        context.json(reply.result().body());
      } else {
        reply.cause().printStackTrace();
        JsonObject body = new JsonObject()
          .put("code", 500)
          .put("message", reply.cause().getMessage());
        context.json(body);
      }
    });
  }

  private void sendMessage2(RoutingContext context) {
    String uuid = java.util.UUID.randomUUID().toString();
    //JsonObject jsonObjectIn = (JsonObject) context.body();
    context.request().bodyHandler(bodyHandler -> {
      vertx.eventBus().request("incoming.message2", bodyHandler.toJsonObject(), reply -> {
        if (reply.succeeded()) {
          log.info("✅ sendMessage2 success");
          context.json(reply.result().body());
        } else {
          System.out.println("No reply");
        }
      });
    });
  }

  private void accountBalance(RoutingContext context) {
    String uuid = java.util.UUID.randomUUID().toString();
    //JsonObject jsonObjectIn = (JsonObject) context.body();
    context.request().bodyHandler(bodyHandler -> {
      vertx.eventBus().request("incoming.account.balance", bodyHandler.toJsonObject(), reply -> {
        if (reply.succeeded()) {
          log.info("✅ accountBalance success");
          context.json(reply.result().body());
        } else {
          System.out.println("No reply");
        }
      });
    });
  }

  private void accountBalanceLib(RoutingContext context) {
    String uuid = java.util.UUID.randomUUID().toString();
    //JsonObject jsonObjectIn = (JsonObject) context.body();
    context.request().bodyHandler(bodyHandler -> {
      vertx.eventBus().request("incoming.account.balance.lib", bodyHandler.toJsonObject(), reply -> {
        if (reply.succeeded()) {
          log.info("✅ accountBalanceLib success");
          context.json(reply.result().body());
        } else {
          System.out.println("No reply");
        }
      });
    });
  }

  private void helloJs(RoutingContext context) {
    String uuid = java.util.UUID.randomUUID().toString();
    //JsonObject jsonObjectIn = (JsonObject) context.body();
    context.request().bodyHandler(bodyHandler -> {
      vertx.eventBus().request("hello.vertx.addr", bodyHandler.toJsonObject(), reply -> {
        if (reply.succeeded()) {
          log.info("✅ helloJs success");
          context.json(reply.result().body());
        } else {
          System.out.println("No reply");
        }
      });
    });
  }


  private Future<Void> receiveMessage(RoutingContext context) {
    return Future.<Void>future(promise -> {
      String uuid = java.util.UUID.randomUUID().toString();
      final String message = context.request().getParam("cuenta");
      log.info("cuenta: " + message);
      // creating json object for message
      JsonObject entries = new JsonObject();
      entries.put("id", uuid);
      entries.put("message", message);
      long yourmilliseconds = System.nanoTime();
      SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm");
      Date resultdate = new Date(yourmilliseconds);
      entries.put("time", resultdate.toString());
      context.json(entries);
      //vertx.eventBus().consumer(TEST_ADDR).handler(this::test);
    });
  }

}
