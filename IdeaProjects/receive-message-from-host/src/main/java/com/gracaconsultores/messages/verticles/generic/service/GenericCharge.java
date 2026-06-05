package com.gracaconsultores.messages.verticles.generic.service;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.time.format.DateTimeFormatter;

@Slf4j
public class GenericCharge extends AbstractVerticle {
  public static final String WRITE_LOG = "write.log";
  public static final DateTimeFormatter formatTimeDate = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS");
  private static final String GENERIC_CHARGE = "generic.charge";
  public static final String FREE_ACCOUNT = "free.account";
  public static final String FREE_ACCOUNT_IN_PROCESS = "free.account.in.process";

  private GenericChargeService genericChargeService;

  @Override
  public void start(Promise<Void> start) {
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig().onComplete(json -> {
      if (json.succeeded()){
      }
    });
    genericChargeService = new GenericChargeService(vertx);
    eventBusConsumersAccount();
  }

  void eventBusConsumersAccount() {
    Future.<Void>future(promise -> vertx.eventBus().consumer(GENERIC_CHARGE).handler(this::genericCharge));
  }

  private void genericCharge(Message<Object> msg) {
    JsonObject jsonMessage = (JsonObject) msg.body();
    JsonObject jsonIn = jsonMessage.getJsonObject("jsonIn");
    JsonObject jsonLog = jsonMessage.getJsonObject("jsonLog");
    JsonObject jsonData = jsonMessage.getJsonObject("jsonData");
    jsonIn.mergeIn(jsonData);
    log.info("Begin genericCharge : " + jsonIn);
    genericChargeService.processGenericCharge(vertx, jsonIn, jsonLog, result -> {
      JsonObject logObj = result.getJsonObject("log");
      String freeAccount = result.getString("freeAccount");
      log.info("account to free : " + freeAccount);
      vertx.eventBus().send(FREE_ACCOUNT, freeAccount);
      writeLog(logObj);
      log.debug("End  genericCharge");
    });
  }

  private void writeLog(JsonObject jsonLog) {
    vertx.eventBus().send(WRITE_LOG, jsonLog);
  }
}
