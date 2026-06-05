package com.gracaconsultores.messages.verticles.repository;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static com.gracaconsultores.messages.ds.SchedulerConfigs.info;
import static com.gracaconsultores.messages.ds.SchedulerConfigs.objectCollector;
import static com.gracaconsultores.messages.verticles.repository.Repository.WRITE_LOG;
import static com.gracaconsultores.messages.verticles.repository.RepositoryAccounts.LOCK_ACCOUNT;
import static com.gracaconsultores.messages.verticles.services.Service.ORIGIN;

@Slf4j
public class RepositoryService {
    public static final DateTimeFormatter formatTimeDate = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS");
    public static final String GENERIC_CHARGE = "generic.charge";
    public static final String ASSET_CHARGE = "asset.charge";
    public static final String FREE_ACCOUNT = "free.account";

    public void processAccounts(Vertx vertx, JsonArray arrayJson, JsonObject jsonIn, JsonObject jsonLog, Handler<Void> resultHandler) {
        if (!arrayJson.isEmpty()) {
            arrayJson.forEach(i ->{
                processNext(vertx, arrayJson, 0, jsonIn, jsonLog, resultHandler);
            });
        } else {
            jsonLog.put("operationStatus", "1020").put("operationMessage", "no hay registros para procesar cuenta");
            vertx.eventBus().send(FREE_ACCOUNT, jsonIn.getString("account").substring(0, 20));
            resultHandler.handle(null);
        }
    }

    private void processNext(Vertx vertx, JsonArray arrayJson, int index, JsonObject jsonIn, JsonObject jsonLog, Handler<Void> resultHandler) {
        log.info("Begin processNext : " + jsonIn);
        if (index >= arrayJson.size()) {
            resultHandler.handle(null);
            return;
        }
        JsonObject obj = arrayJson.getJsonObject(index);
        String account = jsonIn.getString("account");
        log.info("obj : " + obj);
        JsonObject objectCollectorProcess = objectCollector.get(obj.getString("idCollector"));
        log.info("ObjectCollector : " + objectCollectorProcess);

        if (objectCollectorProcess.getBoolean("isActive")){
            int collectorType = objectCollectorProcess.getInteger("collectionType");
            String customer = obj.getString("customer");
            JsonObject jsonInLock = new JsonObject();
            jsonInLock.put("account", account).put("customer", customer).put("collectorType", collectorType);
            //jsonInLock.put("account", account);
            jsonLog.put("collectorId", obj.getInteger("idCollector"));

            vertx.eventBus().request(LOCK_ACCOUNT, jsonInLock, reply -> {
                try {
                    if (reply.succeeded()) {
                        if (obj.getInteger("idCollector") == 2) {
                            jsonIn.put("collector", obj.getInteger("idCollector"));
                            JsonObject jsonMessage = new JsonObject().put("jsonIn", jsonIn).put("jsonData", obj).put("jsonLog", jsonLog.copy());
                            vertx.eventBus().send(ASSET_CHARGE, jsonMessage);
                        } else if (obj.getInteger("idCollector") == 1) {
                            int domic = (info.get("domic") != null) ? (int) info.get("domic") : 0;
                            domic++;
                            info.put("domic", domic);
                            jsonIn.put("collector", obj.getInteger("idCollector"));
                            JsonObject jsonMessage = new JsonObject().put("jsonIn", jsonIn).put("jsonData", obj).put("jsonLog", jsonLog.copy());
                            vertx.eventBus().send(GENERIC_CHARGE, jsonMessage);
                        } else if (obj.getInteger("idCollector") == 3) {
                            int tdc = (info.get("tdc") != null) ? (int) info.get("tdc") : 0;
                            tdc++;
                            info.put("tdc", tdc);
                            jsonIn.put("collector", obj.getInteger("idCollector"));
                            JsonObject jsonMessage = new JsonObject().put("jsonIn", jsonIn).put("jsonData", obj).put("jsonLog", jsonLog.copy());
                            vertx.eventBus().send(GENERIC_CHARGE, jsonMessage);
                        } else {
                            vertx.eventBus().send(FREE_ACCOUNT, jsonIn.getString("account").substring(0, 20));
                            log.info("collector " + obj.getInteger("idCollector") + " no esta configurado");
                            jsonLog.put("operationStatus", "1080").put("operationMessage", "collector inactivo o no reconocido");
                        }
                    } else {
                        log.info(reply.cause().getMessage());
                        JsonObject jsonLog1 = writeBlockedLog(account, reply.cause().getMessage());
                        jsonLog.mergeIn(jsonLog1);
                        writeLog(vertx, jsonLog);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    log.info(reply.cause().getMessage());
                    JsonObject jsonLog1 = writeBlockedLog(account, reply.cause().getMessage());
                    jsonLog.mergeIn(jsonLog1);
                    writeLog(vertx, jsonLog);
                }
            });
        } else {
            log.info("collector " + obj.getString("idCollector") + " esta inactivo");
            //vertx.eventBus().send(FREE_ACCOUNT, jsonIn.getString("account").substring(0, 20));
        }
        // Usar un timer asíncrono en vez de Thread.sleep
        //vertx.setTimer(500, id -> processNext(vertx, arrayJson, index + 1, jsonIn, jsonLog, resultHandler));
    }

    private JsonObject writeBlockedLog(String key, String operationMessage) {
        JsonObject jsonLog = new JsonObject();
        jsonLog.put("accountNumber", key);
        String timestampformat = LocalDateTime.now().format(formatTimeDate);
        jsonLog.put("dateCreate", timestampformat);
        jsonLog.put("originId", UUID.randomUUID().toString()).put("origin", ORIGIN);
        jsonLog.put("operationStatus", "1098").put("operationMessage", operationMessage);
        return jsonLog;
    }

    private void writeBlockedCustomerLog(String key, String operationMessage) {
        JsonObject jsonLog = new JsonObject();
       // jsonLog.put("customerId", key);
        String timestampformat = LocalDateTime.now().format(formatTimeDate);
        jsonLog.put("dateCreate", timestampformat);
        jsonLog.put("originId", UUID.randomUUID().toString()).put("origin", ORIGIN);
        jsonLog.put("operationStatus", "1098").put("operationMessage", operationMessage);
    }

    private <T> void writeLog(Vertx vertx, JsonObject jsonLog) {
        vertx.eventBus().send(WRITE_LOG, jsonLog);
    }
}
