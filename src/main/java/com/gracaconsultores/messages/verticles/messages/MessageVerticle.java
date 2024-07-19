package com.gracaconsultores.messages.verticles.messages;

import com.gracaconsultores.messages.models.BigSerializedObject;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

import static io.vertx.core.impl.ConversionHelper.fromJsonObject;

public class MessageVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger( MessageVerticle.class );
    public static final String MESSAGE_ADDR = "message.domic.account";
    public static final String DOMIC_CHARGE = "message.domic.charge";
    public static final String ASSET_CHARGE = "message.asset.charge";
    public static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
    private static int bcsaPort;
    private static String bcsaUrl;
    private static String bcsaIp;
    private static int portActivo;
    private static String urlActivo;
    private static String ipActivo;
    private static int portDomic;
    private static String urlDomic;
    private static String ipDomic;

    @Override
    public void start(Promise<Void> start) {
      ConfigRetriever retriever = ConfigRetriever.create(vertx);
      retriever.getConfig().onComplete(json -> {
        if (json.succeeded()) {
          JsonObject env = json.result();
          //log.info("result : " + env);
          Map map = fromJsonObject(env);
          //log.info("map : " + map.toString());
          Map ds = (Map) map.get("cobroActivos");
          urlActivo = (String) ds.get("url");
          ipActivo = (String) ds.get("ip");
          portActivo = (int) ds.get("port");

          Map ds2 = (Map) map.get("cobroDomic");
          ipDomic = (String) ds2.get("ip");
          urlDomic = (String) ds2.get("url");
          portDomic = (int) ds.get("port");

          Map ds3 = (Map) map.get("bcsa");
          bcsaUrl = (String) ds3.get("url");
          bcsaIp = (String) ds3.get("ip");
          bcsaPort = (int) ds.get("port");
        }
      });

      log.info("bcsaUrl : " + bcsaIp+":"+bcsaPort+bcsaUrl);
      log.info("u087Url : " + ipActivo+":"+portActivo+urlActivo);
      log.info("FSICUrl : " + ipDomic+":"+portDomic+urlDomic);

      //configureEventBusConsumers();
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
        vertx.eventBus().<JsonObject>consumer(DOMIC_CHARGE, this::domicCharge);
        vertx.eventBus().<JsonObject>consumer(ASSET_CHARGE, this::assetCharge);
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
      WebClient client = WebClient.create(vertx);

      BigSerializedObject obj = new BigSerializedObject();
      obj.setId(java.util.UUID.randomUUID().toString());
      obj.setTitle("Cooper");
      obj.setUrl("pruebas de webclient");

      client.post(31117, "180.183.170.71", "/api/message2").sendJson(obj, ar -> {
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

    private <T> void domicCharge(Message<T> tMessage) {
      log.info("🚀 domicCharge received message : " + tMessage.body());
      WebClient client = WebClient.create(vertx);
      JsonObject bodyIn = (JsonObject) tMessage.body();
      accountBalance(bodyIn).onComplete(bcsa -> {
        if(bcsa.succeeded()) {
          JsonObject bcsaR = bcsa.result().bodyAsJsonObject();
          log.info("bcsac asset response : " + bcsaR);
          log.info("bcsac code  : " + bcsaR.getInteger("code"));
          if(bcsaR.getInteger("code") == 1000) {
            JsonObject jsonMap = bcsaR.getJsonObject("data").getJsonObject("BGMCSA1");
            log.info("jsonMap : " + jsonMap);
            log.info("building asset  charge U087 and comparing amount balance : " + jsonMap.getFloat("ppalSdoFinal"));
            if (jsonMap.getFloat("ppalSdoFinal") > 0.0) {
              log.info("building asset  charge U087");
              JsonObject bodyInDomic = new JsonObject();
              bodyIn.put("contratoCredito",   bodyIn.getString("CREDIT_NUMBER"));
              bodyIn.put("codDivisa",         bodyIn.getString("CURRENCY"));
              bodyIn.put("recibos",           "1");
              bodyIn.put("importe",           0.0);
              bodyIn.put("fechaValor",        sdf2.format(new Date()));
              bodyIn.put("indFormaDePago",    "1");
              bodyIn.put("cccCargo",          bodyIn.getString("ACCOUNT_NUMBER"));
              bodyIn.put("numeroDeCheque",    "");
              bodyIn.put("importeDelCheque",  "");
              bodyIn.put("tasaDeMora",        "");
              bodyIn.put("importeDeMora",     "");
              bodyIn.put("nioDeCobroLinea",   "");
              bodyIn.put("indicadorDeCobro",  "");
              bodyIn.put("user",              "BDVN001");
              log.info("sending json to charge domic service : " + bodyInDomic);
              chargeDomic(bodyInDomic).onComplete(charge ->{
                if (charge.succeeded()) {
                  JsonObject responseU087 = charge.result().bodyAsJsonObject();
                  log.info("responseU087 : " + responseU087);
                  if(responseU087.getInteger("status") == 1000){
                    log.info("✅ return pic with HTTP response with status " + responseU087.getString("message"));
                    tMessage.reply(responseU087);
                  } else {
                    JsonObject body = new JsonObject()
                      .put("code", 1020)
                      .put("message", "no se realiza la operacion de cobranza activos por : " + responseU087.getString("message"));
                    tMessage.reply(responseU087);
                  }
                } else {
                  charge.cause().printStackTrace();
                  JsonObject body = new JsonObject()
                    .put("code", 500)
                    .put("message", charge.cause().getMessage());
                  tMessage.reply(body);
                }
              });
            } else {
              log.info("no se envia la operacion de cobranza por falta de saldo");
              JsonObject body = new JsonObject()
                .put("code", 1020)
                .put("message", "no se realiza la operacion de cobranza por falta de saldo");
              tMessage.reply(body);
            }
          } else {
            log.info("no se envia la operacion de cobranza por : " + bcsaR.getString("message"));
            JsonObject body = new JsonObject()
              .put("code", 1020)
              .put("message", "no se realiza la operacion de cobranza por : " + bcsaR.getString("message"));
            tMessage.reply(body);

          }
        } else {
          log.info("no se envia la operacion de cobranza por falla : " + bcsa.cause());
          JsonObject body = new JsonObject()
            .put("code", 1020)
            .put("message", "no se realiza la operacion de cobranza por falta de saldo");
          tMessage.reply(body);
        }
      });
    }

    private <T> void assetCharge(Message<T> tMessage) {
      log.info("🚀 assetCharge received message : " + tMessage.body());
      WebClient client = WebClient.create(vertx);
      JsonObject bodyIn = (JsonObject) tMessage.body();
      JsonObject bodyInCharge = new JsonObject();
      bodyInCharge.put("contratoCredito",   bodyIn.getString("CREDIT_NUMBER"));
      bodyInCharge.put("codDivisa",         bodyIn.getString("CURRENCY"));
      bodyInCharge.put("recibos",           "");
      bodyInCharge.put("importe",           0.00);
      bodyInCharge.put("fechaValor",        sdf2.format(new Date()));
      bodyInCharge.put("indFormaDePago",    "1");
      bodyInCharge.put("cccCargo",          bodyIn.getString("ACCOUNT_NUMBER"));
      bodyInCharge.put("numeroDeCheque",    "");
      bodyInCharge.put("importeDelCheque",  "");
      bodyInCharge.put("tasaDeMora",        "");
      bodyInCharge.put("importeDeMora",     "");
      bodyInCharge.put("nioDeCobroLinea",   "");
      bodyInCharge.put("indicadorDeCobro",  "");
      bodyInCharge.put("user",              "BDVN001");
      log.info("sending json to asset charge pic U087 : " + bodyInCharge);
      assetChargePic(bodyInCharge).onComplete(charge ->{
        if (charge.succeeded()) {
          JsonObject responseU087 = charge.result().bodyAsJsonObject();
          log.info("responseU087 : " + responseU087);
          if(responseU087.getInteger("status") == 1000){
            log.info("✅ return pic with HTTP response with status " + responseU087.getString("message"));
            tMessage.reply(responseU087);
          } else {
            JsonObject body = new JsonObject()
              .put("code", 1020)
              .put("message", "no se realiza la operacion de cobranza activos por : " + responseU087.getString("message"));
            tMessage.reply(responseU087);
          }
        } else {
          charge.cause().printStackTrace();
          JsonObject body = new JsonObject()
            .put("code", 500)
            .put("message", charge.cause().getMessage());
          tMessage.reply(body);
        }
      });
    }

    Future<HttpResponse<Buffer>> accountBalance(JsonObject jsonIn) {
      log.info("🚀 AccountBalance PIC received message : " + jsonIn);
      WebClientOptions options = new WebClientOptions()
        .setConnectTimeout(5000)
        .setUserAgent("My-App/1.2.3");
      options.setKeepAlive(false);
      //WebClient client = WebClient.create(vertx, options);
      WebClient client = WebClient.create(vertx, options);
      JsonObject bodyIn = new JsonObject()
        .put("codigoCtaCliente", jsonIn.getValue("ACCOUNT_NUMBER"))
        .put("divisa", "VES");
      log.info("Sending json : " + bodyIn);
      log.info("to : " + bcsaPort + " - " + bcsaIp + " - " + bcsaUrl);

      /*return Future.future(promise ->
        client.post(bcsaPort, bcsaIp, bcsaUrl).sendJson(bodyIn).compose(response -> {return response.bodyAsJson(response.body());})
      );*/
      return client.post(bcsaPort, bcsaIp, bcsaUrl).sendJson(bodyIn);
    }

    private <T> void accountBalance(Message<T> tMessage) {
      log.info("🚀 AccountBalance PIC received message : " + tMessage.body());
      WebClientOptions options = new WebClientOptions()
        .setConnectTimeout(5000)
        .setUserAgent("My-App/1.2.3");
      options.setKeepAlive(false);
      //WebClient client = WebClient.create(vertx, options);
      WebClient client = WebClient.create(vertx, options);
      JsonObject bodyIn = (JsonObject) tMessage.body();
      log.info("Sending json : " + bodyIn);

      client.post(bcsaPort, bcsaIp, bcsaUrl).sendJson(bodyIn, ar -> {
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

    private Future<HttpResponse<Buffer>> assetChargePic(JsonObject jsonIn) {
      Promise<JsonObject> promise = Promise.promise();
      log.info("🚀 assetChargePic : " + jsonIn);
      WebClientOptions options = new WebClientOptions()
        .setConnectTimeout(5000)
        .setUserAgent("Pic-App/1.2.3");
      options.setKeepAlive(false);
      //WebClient client = WebClient.create(vertx, options);
      WebClient client = WebClient.create(vertx, options);
      /*JsonObject bodyIn = new JsonObject()
        .put("codigoCtaCliente", jsonIn.getValue("NRO_CTA_1"))
        .put("divisa", "VES");*/
      log.info("Sending json : " + jsonIn);

      return client.post(portActivo, ipActivo, urlActivo).sendJson(jsonIn);
    }

    private Future<HttpResponse<Buffer>> chargeDomic(JsonObject jsonIn) {
      Promise<JsonObject> promise = Promise.promise();
      log.info("🚀 assetChargePic : " + jsonIn);
      WebClientOptions options = new WebClientOptions()
        .setConnectTimeout(5000)
        .setUserAgent("Pic-App/1.2.3");
      options.setKeepAlive(false);
      //WebClient client = WebClient.create(vertx, options);
      WebClient client = WebClient.create(vertx, options);
        /*JsonObject bodyIn = new JsonObject()
          .put("codigoCtaCliente", jsonIn.getValue("NRO_CTA_1"))
          .put("divisa", "VES");*/
      log.info("Sending json : " + jsonIn);

      return client.post(portDomic, ipDomic, urlDomic).sendJson(jsonIn);
    }
}
