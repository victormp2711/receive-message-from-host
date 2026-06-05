package com.gracaconsultores.messages.verticles.generic.service;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import static com.gracaconsultores.messages.ds.SchedulerConfigs.info;

@Slf4j
public class GenericChargeService {
  public static final DateTimeFormatter formatTimeDate = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS");
  public static final String FREE_ACCOUNT = "free.account";
  private WebClient client;

  public GenericChargeService(Vertx vertx) {
    WebClientOptions options = new WebClientOptions()
      .setConnectTimeout(5000)
      .setIdleTimeout(10000)  // Timeout de inactividad en milisegundos
      .setUserAgent("My-App/1.2.3")
      .setKeepAlive(false);
    this.client = WebClient.create(vertx, options);
  }

  public void processGenericCharge(Vertx vertx, JsonObject jsonIn, JsonObject jsonLog, Handler<JsonObject> resultHandler) {
    log.info("processGenericCharge : " + jsonIn);
    try {
      jsonLog.put("creditNumber", "00000000000").put("collectorId", jsonIn.getInteger("idCollector"));
      String timestampformat = LocalDateTime.now().format(formatTimeDate);
      jsonLog.put("dateSent", timestampformat);

      if (client == null) {
        WebClientOptions options = new WebClientOptions()
          .setConnectTimeout(5000)
          .setUserAgent("My-App/1.2.3")
          .setKeepAlive(false);
        client = WebClient.create(vertx, options);
      }

      JsonObject bodyIn = new JsonObject();
      if(jsonIn.getInteger("idCollector") == 1) {
        bodyIn.put("accountNumber", jsonIn.getValue("account"))
          .put("divisa", "VES")
          .put("origin", jsonIn.getString("origin"))
          .put("originId", jsonIn.getString("originId"));
      }  else {
        bodyIn.put("cuentaCargo", jsonIn.getValue("account"))
          .put("codDivisa", "VES")
          .put("importe", 0)
          .put("IndicadorCobro", " ")
          .put("codigoRespuesta", " ")
          .put("user", "SIC0001");
      }

      log.info("jsonIn : " + jsonIn);
      client.post(jsonIn.getInteger("port"), jsonIn.getString("ip"), jsonIn.getString("url"))
        .timeout(10000)
        .sendJson(bodyIn)// Timeout de respuesta en milisegundos
        .onComplete(charge -> {
          String timestampformat2 = LocalDateTime.now().format(formatTimeDate);
          jsonLog.put("dateReceived", timestampformat2);
          JsonObject result = new JsonObject();
          result.put("freeAccount", jsonIn.getString("accountNumber").substring(0, 20));
          if (charge.succeeded()) {
            JsonObject response = charge.result().bodyAsJsonObject();
            log.info("bodyIn : " + bodyIn + " -- response : " + response);
            if (Objects.equals(response.getString("code"), "1000")) {
              log.info("cuenta procesada correctamente");
              //info.merge("successDomic", 1, (oldVal, newVal) -> (int) oldVal + 1);
              JsonObject responseData = response.getJsonObject("data");
              log.info("responseData : " + responseData);
              jsonLog.put("operationStatus", response.getString("code")).put("operationMessage", response.getString("message"));
            } else if (Objects.equals(response.getString("code"), "1001")) {
              if(response.getString("message").startsWith("No hay registros") ||
                response.getString("message").startsWith("UGE1099")) {
                info.merge("SuccessGenericTotal", 1, (oldVal, newVal) -> (int) oldVal + 1);
                jsonLog.put("operationStatus", response.getString("code")).put("operationMessage", response.getString("message"));
              } else {
                jsonLog.put("operationStatus", "1099").put("operationMessage", response.getString("message"));
              }
            } else {
              jsonLog.put("operationStatus", response.getString("code")).put("operationMessage", response.getString("message"));
            }
            result.put("log", jsonLog);
          } else {
            jsonLog.put("operationStatus", "1099")
              .put("operationMessage", charge.cause().getMessage());
            // info.merge("timeOutDomic", 1, (oldVal, newVal) -> (int) oldVal + 1);
            result.put("log", jsonLog);
          }
          resultHandler.handle(result);
        })
        .onFailure(f -> {
          if (f.getMessage().contains("timeout") || f.getMessage().contains("Timeout")) {
            log.info("Timeout al enviar la solicitud: " + f.getMessage());
            jsonLog.put("operationStatus", 1030).put("operationMessage", "Timeout en la solicitud");
            info.merge("timeOutGeneric", 1, (oldVal, newVal) -> (int) oldVal + 1);
          } else {
            log.info("Fallo en el envío: " + f.getMessage() + " - " + f.getLocalizedMessage());
            jsonLog.put("operationStatus", 1030).put("operationMessage", f.getMessage());
            info.merge("exceptionGeneric", 1, (oldVal, newVal) -> (int) oldVal + 1);
          }
          JsonObject result = new JsonObject();
          result.put("log", jsonLog);
          result.put("freeAccount", jsonIn.getString("accountNumber").substring(0, 20));
          resultHandler.handle(result);
        });
    } catch (Exception e) {
      jsonLog.put("operationStatus", 1099).put("operationMessage", e.getMessage());
      info.merge("exceptionGeneric", 1, (oldVal, newVal) -> (int) oldVal + 1);
      log.info("Falla in GenericCharge : " + e.getMessage());
      JsonObject result = new JsonObject();
      result.put("log", jsonLog);
      result.put("freeAccount", jsonIn.getString("accountNumber").substring(0, 20));
      resultHandler.handle(result);
      e.printStackTrace();
    }
  }
}
