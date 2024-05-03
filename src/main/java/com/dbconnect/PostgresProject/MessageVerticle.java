package com.dbconnect.PostgresProject;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger( MessageVerticle.class );
    public static final String MESSAGE_ADDR = "message.domic.account";

    @Override
    public void start(Promise<Void> start) {
      configureEventBusConsumers();
      configureEventBusConsumers2();
      WebClientOptions options = new WebClientOptions()
        .setUserAgent("My-App/1.2.3");
      options.setKeepAlive(false);
      WebClient client = WebClient.create(vertx, options);
      start.complete();
    }


    void configureEventBusConsumers() {
      Future.<Void>future(promise -> {
        vertx.eventBus().consumer("incoming.message", this::onMessage);
      });
    }

    void configureEventBusConsumers2() {
      Future.<Void>future(promise -> {
        vertx.eventBus().<JsonObject>consumer("incoming.message2", this::onMessage2);
        vertx.eventBus().<JsonObject>consumer("incoming.account.balance", this::accountBalance);
      });
    }

    private <T> void onMessage(io.vertx.core.eventbus.Message<T> tMessage) {
      log.info("🚀 onMessage received message : " + tMessage.body());
      //WebClientOptions options = new WebClientOptions()
      //  .setConnectTimeout(5000)
      //  .setUserAgent("My-App/1.2.3");
      //options.setKeepAlive(false);
      //WebClient client = WebClient.create(vertx, options);
      //WebClient client = WebClient.create(vertx);

        BigSerializedObject obj = new BigSerializedObject();
        obj.setId(java.util.UUID.randomUUID().toString());
        obj.setTitle("Cooper");
        obj.setUrl("pruebas de webclient");
        log.info("✅ return pic with HTTP response ok");
        tMessage.reply(obj);
    }

  private <T> void onMessage2(io.vertx.core.eventbus.Message<T> tMessage) {
    log.info("🚀 onMessage received message : " + tMessage.body());
    //WebClientOptions options = new WebClientOptions()
    //  .setConnectTimeout(5000)
    //  .setUserAgent("My-App/1.2.3");
    //options.setKeepAlive(false);
    //WebClient client = WebClient.create(vertx, options);
    WebClient client = WebClient.create(vertx);

    BigSerializedObject obj = new BigSerializedObject();
    obj.setId(java.util.UUID.randomUUID().toString());
    obj.setTitle("Cooper");
    obj.setUrl("pruebas de webclient");

    client.post(31117, "180.183.170.74", "/api/message2").sendJson(obj, ar -> {
      if (ar.succeeded()) {
        HttpResponse<Buffer> response = ar.result();
        log.info("✅ return pic with HTTP response with status " + response.statusCode());
        JsonObject body = response.body().toJsonObject();
        tMessage.reply(body);
      } else {
        ar.cause().printStackTrace();
        JsonObject body = new JsonObject()
          .put("code", 500)
          .put("message", ar.cause().getMessage());
        tMessage.reply(body);
      }
    });
  }

    private <T> void accountBalance(io.vertx.core.eventbus.Message<T> tMessage) {
      log.info("🚀 AccountBalance PIC received message : " + tMessage.body());
      WebClientOptions options = new WebClientOptions()
        .setConnectTimeout(5000)
        .setUserAgent("My-App/1.2.3");
      options.setKeepAlive(false);
      //WebClient client = WebClient.create(vertx, options);
      WebClient client = WebClient.create(vertx, options);
      JsonObject bodyIn = new JsonObject()
        .put("codigoCuentaCliente", "01020501850009269321")
        .put("divisa", "VES");
      client.post(30813, "180.183.170.77", "/lastmovementsquery/lastMovementsQueryBCMO").sendJson(bodyIn, ar -> {
        if (ar.succeeded()) {
          HttpResponse<Buffer> response = ar.result();
          log.info("✅ return pic with HTTP response with status " + response.statusCode());
          JsonObject body = response.body().toJsonObject();
          tMessage.reply(body);
        } else {
          ar.cause().printStackTrace();
          JsonObject body = new JsonObject()
            .put("code", 500)
            .put("message", ar.cause().getMessage());
          tMessage.reply(body);
        }
      });

  }
}
