package com.gracaconsultores.messages.verticles.asset.service;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import static com.gracaconsultores.messages.ds.SchedulerConfigs.info;

public class AssetChargePicService {
    public static final DateTimeFormatter formatTimeDate = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS");
    public static final String FREE_ACCOUNT = "free.account";
  public static final String ASSET_CHARGE_PIC = "asset.charge.pic";

    public void processAssetChargePic(Vertx vertx, JsonObject jsonMessagePic, JsonObject jsonLog, Handler<JsonObject> resultHandler) {
        JsonObject bodyInCharge = jsonMessagePic.getJsonObject("bodyInCharge");
        JsonObject jsonIn = jsonMessagePic.getJsonObject("jsonIn");
        vertx.eventBus().request(ASSET_CHARGE_PIC, jsonMessagePic, reply -> {
            JsonObject result = new JsonObject();
            if (reply.succeeded()) {
                JsonObject responseU087Bus = (JsonObject) reply.result().body();
                String timestampformat2 = LocalDateTime.now().format(formatTimeDate);
                jsonLog.put("dateReceived", timestampformat2);
                jsonLog.put("creditNumber", bodyInCharge.getString("contratoCredito")).put("collectorId", (jsonIn.getInteger("idCollector")));
                jsonLog.put("idDebt", bodyInCharge.getString("idDebt"));
                if (responseU087Bus.getInteger("code") != null) {
                    if (responseU087Bus.getInteger("code") == 1000) {
                        jsonLog.put("operationStatus", responseU087Bus.getInteger("code")).put("operationMessage", "success");
                        JsonObject responseData = responseU087Bus.getJsonObject("data");
                        int accountNoBlocked = (info.get("accountNoBlocked") != null) ? (int) info.get("accountNoBlocked") : 0;
                        JsonArray responseMapUGM0863 = responseData.getJsonArray("UGM0863") != null ? responseData.getJsonArray("UGM0863") : null;
                        JsonObject responseMap = new JsonObject();
                        if(responseData != null && !responseData.isEmpty() && responseMapUGM0863 != null) {
                            responseMap = responseMapUGM0863.getJsonObject(0);
                            if(responseMap.getString("divisaCobro").equals("UVC")){
                                Double amountUVC = (info.get("amountUVC") != null) ? (Double) info.get("amountUVC") : 0;
                                amountUVC =  Double.sum(amountUVC, responseMap.getDouble("importeRecuperado"));
                                info.put("amountUVC", amountUVC);
                            } else {
                                Double amountVES = (info.get("amountVES") != null) ? (Double) info.get("amountVES") : 0;
                                amountVES =  Double.sum(amountVES, responseMap.getDouble("importeRecuperado"));
                                info.put("amountVES", amountVES);
                            }
                        } else {
                            responseMap = new JsonObject().put("UGM0863","Sin respuesta");
                        }
                        int successActivos = (info.get("successActivos") != null) ? (int) info.get("successActivos") : 0;
                        successActivos++;
                        info.put("successActivos", successActivos);
                    } else if (responseU087Bus.getInteger("status") != null && responseU087Bus.getInteger("status") == 500) {
                        jsonLog.put("operationStatus", 1500).put("operationMessage", "timeout Pic");
                        int timeOut = (info.get("timeOut") != null) ? (int) info.get("timeOut") : 0;
                        timeOut++;
                        info.put("timeOut", timeOut);
                    } else {
                        String codeNew;
                        if (responseU087Bus.getInteger("code") == 1001) {
                            if (responseU087Bus.getString("message").startsWith("UGE1099") || responseU087Bus.getString("message").startsWith("UGE0021")) {
                                codeNew = "1001";
                            } else {
                                codeNew = "1002";
                            }
                        } else {
                            codeNew = "1002";
                        }
                        jsonLog.put("operationStatus", codeNew).put("operationMessage", responseU087Bus.getString("message"));
                        int notSuccessActivos = (info.get("notSuccessActivos") != null) ? (int) info.get("notSuccessActivos") : 0;
                        notSuccessActivos++;
                        info.put("notSuccessActivos", notSuccessActivos);
                    }
                } else {
                    jsonLog.put("operationStatus", 1500).put("operationMessage", "timeout Pic");
                    int timeOut = (info.get("timeOut") != null) ? (int) info.get("timeOut") : 0;
                    timeOut++;
                    info.put("timeOut", timeOut);
                }
                result.put("log", jsonLog);
                result.put("freeAccount", jsonIn.getString("accountNumber").substring(0, 20));
                resultHandler.handle(result);
            } else {
                jsonLog.put("operationStatus", 1500).put("operationMessage", "timeout Pic");
                int timeOut = (info.get("timeOut") != null) ? (int) info.get("timeOut") : 0;
                timeOut++;
                info.put("timeOut", timeOut);
                result.put("log", jsonLog);
                result.put("freeAccount", jsonIn.getString("accountNumber").substring(0, 20));
                resultHandler.handle(result);
            }
        });
    }
}
