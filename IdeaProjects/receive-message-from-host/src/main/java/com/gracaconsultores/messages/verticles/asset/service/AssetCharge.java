package com.gracaconsultores.messages.verticles.asset.service;

import com.gracaconsultores.messages.ds.HikariConfigs;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

@Slf4j
public class AssetCharge extends AbstractVerticle {
  public static final String WRITE_LOG = "write.log";
  public static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
  public static final DateTimeFormatter formatTimeDate = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS");
  private static final String ASSET_CHARGE = "asset.charge";
  public static final String FREE_ACCOUNT = "free.account";

  private AssetChargeService assetChargeService;
  private AssetChargePicService assetChargePicService;

  @Override
  public void start(Promise<Void> start) {
    DataSource dataSource = HikariConfigs.getDataSource(); // Usar el datasource existente
    assetChargeService = new AssetChargeService(vertx, dataSource);
    assetChargePicService = new AssetChargePicService();
    eventBusConsumersAccount();
  }

  void eventBusConsumersAccount() {
    vertx.eventBus().consumer(ASSET_CHARGE).handler(this::handleAssetCharge);
  }

  private void handleAssetCharge(Message<Object> msg) {
    JsonObject jsonMessage = (JsonObject) msg.body();
    JsonObject jsonIn = jsonMessage.getJsonObject("jsonIn");
    JsonObject jsonLog = jsonMessage.getJsonObject("jsonLog");
    JsonObject jsonData = jsonMessage.getJsonObject("jsonData");
    jsonIn.mergeIn(jsonData);
    log.info("Begin handleAssetCharge : {}", jsonIn);
    assetChargeService.processAssetCharge(jsonIn).onComplete(ar -> {
      if (ar.succeeded()) {
        JsonObject jsonResult = ar.result();
        log.debug("GET_ACCOUNT_BY_COLLECTOR : " + jsonResult);
        if (jsonResult != null && jsonResult.getInteger("code") == 1000) {
          JsonArray arrayJson = jsonResult.getJsonArray("data");
          if (!arrayJson.isEmpty()) {
            log.debug("GET_ACCOUNT_BY_COLLECTOR arrayJson : " + arrayJson.size());
            JsonObject jsonMessagePic = new JsonObject();
            JsonObject jsonResponse = (JsonObject) arrayJson.getJsonObject(0);
            log.debug("procesando registro : " + jsonResponse);
            JsonObject bodyInCharge = new JsonObject();
            bodyInCharge.put("contratoCredito", jsonResponse.getString("creditNumber"));
            bodyInCharge.put("codDivisa", jsonResponse.getString("currency"));
            bodyInCharge.put("recibos", "");
            bodyInCharge.put("importe", 0.00);
            bodyInCharge.put("fechaValor", sdf2.format(new Date()));
            bodyInCharge.put("indFormaDePago", "1");
            bodyInCharge.put("cccCargo", jsonIn.getString("account"));
            bodyInCharge.put("numeroDeCheque", "");
            bodyInCharge.put("importeDelCheque", "");
            bodyInCharge.put("tasaDeMora", "");
            bodyInCharge.put("importeDeMora", "");
            bodyInCharge.put("nioDeCobroLinea", "");
            bodyInCharge.put("indicadorDeCobro", "");
            bodyInCharge.put("user", jsonIn.getString("userPic"));
            bodyInCharge.put("fechaVencimiento", jsonResponse.getInteger("fechaVencimiento"));
            jsonMessagePic.put("jsonIn", jsonIn).put("bodyInCharge", bodyInCharge);
            String timestampformat = LocalDateTime.now().format(formatTimeDate);
            jsonLog.put("dateSent", timestampformat);
            assetChargePic(jsonMessagePic, jsonLog);
          } else {
            handleNoRecords(jsonIn, jsonLog);
          }
        } else {
          handleNoRecords(jsonIn, jsonLog, jsonResult != null ? jsonResult.getString("message") : "Error desconocido");
        }
      } else {
        handleNoRecords(jsonIn, jsonLog, ar.cause().getMessage());
      }
      log.debug("End  assetCharge");
    });
  }

  private void handleNoRecords(JsonObject jsonIn, JsonObject jsonLog) {
    handleNoRecords(jsonIn, jsonLog, "no hay registros para procesar cuenta : collector : " + jsonIn);
  }

  private void handleNoRecords(JsonObject jsonIn, JsonObject jsonLog, String message) {
    vertx.eventBus().send(FREE_ACCOUNT, jsonIn.getString("account").substring(0, 20));
    jsonLog.put("creditNumber", "0").put("collectorId", jsonIn.getString("collector")).put("operationStatus", "1020").put("operationMessage", message);
    log.debug("no hay datos complementarios para activos cuenta : " + jsonIn.getString("account") + " - " + jsonIn);
    writeLog(jsonLog);
  }

  private Connection getConnectionSic() {
    try {
      return HikariConfigs.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void assetChargePic(JsonObject jsonMessagePic, JsonObject jsonLog) {
    assetChargePicService.processAssetChargePic(vertx, jsonMessagePic, jsonLog, result -> {
      JsonObject logObj = result.getJsonObject("log");
      String freeAccount = result.getString("freeAccount");
      writeLog(logObj);
      vertx.eventBus().send(FREE_ACCOUNT, freeAccount);
    });
  }

  private void writeLog(JsonObject jsonLog) {
    vertx.eventBus().send(WRITE_LOG, jsonLog);
  }
}
