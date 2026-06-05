package com.gracaconsultores.messages.verticles.asset.service;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssetChargePic extends AbstractVerticle {

  public static final String ASSET_CHARGE_PIC = "asset.charge.pic";
  private WebClient client;

  @Override
  public void start(Promise<Void> start) {
    eventBusConsumersAccount();

    WebClientOptions options = new WebClientOptions()
      .setConnectTimeout(5000)
      .setUserAgent("My-App/1.2.3")
      .setKeepAlive(false);
    client = WebClient.create(Vertx.vertx(), options);

  }

  void eventBusConsumersAccount() {
    Future.<Void>future(promise -> vertx.eventBus().consumer(ASSET_CHARGE_PIC).handler(this::setAssetChargePic));
  }

  private void setAssetChargePic(Message<Object> msg) {
    log.info("setAssetChargePic : {}", msg.body());
    JsonObject jsonMessage = (JsonObject) msg.body();
    JsonObject jsonIn = jsonMessage.getJsonObject("jsonIn");
    JsonObject bodyInCharge = jsonMessage.getJsonObject("bodyInCharge");
    assetChargePic(jsonIn, bodyInCharge).onComplete(charge -> {
      if (charge.succeeded()) {
        JsonObject responseU087 = charge.result().bodyAsJsonObject();
        log.debug("responseU087 : {}", responseU087);
        JsonObject jsonResponse = new JsonObject();
        jsonResponse.put("responseU087", responseU087);
        JsonObject reponseInternal = new JsonObject();
        reponseInternal.put("creditNumber", bodyInCharge.getString("contratoCredito")).put("idDebt", jsonIn.getInteger("idDebt"));
        jsonResponse.put("reponseInternal", responseU087);
        msg.reply(responseU087);
      } else {
        log.debug("error al realiza la operacion de cobranza activos por : " + charge.cause());
        msg.fail(1010,  charge.cause().getMessage());
      }
    }).onFailure(f -> {
      log.debug("error al realiza la operacion de cobranza activos por : " + f.getMessage());
      msg.fail(1090,  f.getMessage());
    });
  }

  private Future<HttpResponse<Buffer>> assetChargePic(JsonObject jsonIn, JsonObject bodyIn) {
    log.debug("Begin assetChargePic : {} - {}", jsonIn, bodyIn);
    String url = "";
    String ip = "";
    int port = 80;

    url = jsonIn.getString("url");
    ip = jsonIn.getString("ip");
    port = jsonIn.getInteger("port");

    log.info("to : {} - {} - {}", port, ip, url);
    String finalIp = ip;
    String finalUrl = url;
    return client.post(port, ip, url).sendJson(bodyIn)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          log.debug("Successfully webclient to {} - {}", finalIp, finalUrl);
          log.info(String.valueOf(ar.result().bodyAsJsonObject()));
        } else {
          log.info("filed with error webclient : {}", String.valueOf(ar.cause()));
        }
      }).onFailure(ar -> log.error("failed to send webclient" + ar.getCause()));
  }
}
