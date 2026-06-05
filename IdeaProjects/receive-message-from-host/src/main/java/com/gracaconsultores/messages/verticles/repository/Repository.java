package com.gracaconsultores.messages.verticles.repository;

import com.gracaconsultores.messages.ds.HikariConfigs;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static com.gracaconsultores.messages.ds.SchedulerConfigs.info;
import static io.vertx.core.impl.ConversionHelper.fromJsonObject;

@Slf4j
public class Repository extends AbstractVerticle {

  private static List<Map> collectors;
  public static final String GET_ACCOUNT_FROM_COLLECT = "get.account.from.collect";
  public static final String WRITE_LOG = "write.log";
  private static final String ASSET_CHARGE = "asset.charge";
  private static final String ASSET_CHARGE_2 = "asset.charge.2";
  public static final DateTimeFormatter formatTimeDate = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS");
  public static final String FREE_ACCOUNT = "free.account";

  private RepositoryDao repositoryDao;
  private RepositoryService repositoryService;

  @Override
  public void start(Promise<Void> start) {
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig().onComplete(json -> {
      if (json.succeeded()){
        JsonObject env = json.result();
        Map map = fromJsonObject(env);
        collectors = (List<Map>) map.get("collectors");
        log.info("collectors  : " + collectors);
        log.debug("✅ Repository is ready" );
        this.vertx.eventBus().consumer("new-configuration", this::newConfig);
      }
    });
    repositoryDao = new RepositoryDao();
    repositoryService = new RepositoryService();
    eventBusConsumers();
  }

  void eventBusConsumers() {
    Future.<Void>future(promise -> vertx.eventBus().consumer(GET_ACCOUNT_FROM_COLLECT).handler(this::getAccountFromCollect));
  }

  private void newConfig(Message msg) {
    this.log.info("new config recived " + String.valueOf(msg.body()));
    JsonObject jsonConfig = (JsonObject)msg.body();
    Map map = fromJsonObject(jsonConfig);
    collectors = (List<Map>) map.get("collectors");
    log.info("collectors  : " + collectors);
  }

  private void getAccountFromCollect(Message<Object> msg) {
    JsonObject jsonIn = (JsonObject) msg.body();
    log.debug("Begin getAccountFromCollect : " + jsonIn.getString("originId") + " - " +  " - " +  jsonIn);
    JsonObject jsonLog = new JsonObject();
    DataSource dataSource = HikariConfigs.getDataSource();
    vertx.executeBlocking(future -> {
      JsonArray arrayJson = repositoryDao.getAccountsForAllCollectors(jsonIn, dataSource);
      // Enriquecer cada cuenta con los parámetros del collector
      log.info("arrayJson : " + arrayJson);
      for (int i = 0; i < arrayJson.size(); i++) {
        JsonObject obj = arrayJson.getJsonObject(i);
        JsonObject paramsCollector = getCollectorParams(obj.getInteger("idCollector"));
        obj.mergeIn(paramsCollector);
      }
      future.complete(arrayJson);
    }, res -> {
      String timestampformat1 = LocalDateTime.now().format(formatTimeDate);
      jsonLog.put("dateCreate", timestampformat1);
      jsonLog.put("accountNumber", jsonIn.getString("account").substring(0,20));
      jsonLog.put("origin", jsonIn.getString("origin"));
      jsonLog.put("originId", jsonIn.getString("originId"));
      jsonLog.put("customer", jsonIn.getString("customer"));

      if (res.succeeded()) {
        if(res.result() != null) {
          JsonArray arrayJson = (JsonArray) res.result();
          repositoryService.processAccounts(vertx, arrayJson, jsonIn, jsonLog, v -> {
            // Finalización del procesamiento
            log.info("End getAccountFromCollect");
          });
        } else {
          vertx.eventBus().send(FREE_ACCOUNT, jsonIn.getString("account").substring(0,20));
          jsonLog.put("operationStatus", "1095").put("operationMessage", "cuenta no existe entabla de monitoreo");
          log.info("End getAccountFromCollect cuenta no existe entabla de monitoreo");
          handleNoRecords(jsonLog, jsonIn, "cuenta no existe entabla de monitoreo");
        }
      } else {
        jsonLog.put("operationStatus", "1099").put("operationMessage", "no se puede obtener conexion de BD");
        writeLog(jsonLog);
        vertx.eventBus().send(FREE_ACCOUNT, jsonIn.getString("account").substring(0,20));
        int notConnectionDb = (info.get("notConnectionDb") != null) ? (int) info.get("notConnectionDb") : 0;
        notConnectionDb++;
        info.put("notConnectionDb", notConnectionDb);
      }
    });
  }

  private JsonObject getCollectorParams(Integer collectorId) {
    JsonObject bodyIn = new JsonObject();

    for (Map collector : collectors) {
      if (collector.get("idCollector") == collectorId) {
        bodyIn.put("url", collector.get("url"));
        bodyIn.put("ip", collector.get("ip"));
        bodyIn.put("port", collector.get("port"));
        bodyIn.put("userPic", collector.get("userPic"));
        bodyIn.put("channelsPic", collector.get("channelsPic"));
        bodyIn.put("activeFunction", collector.get("activeFunction"));
        break;
      }
    }
    return bodyIn;
  }


  private void handleNoRecords(JsonObject jsonLog, JsonObject jsonIn, String message) {
    vertx.eventBus().send(FREE_ACCOUNT, jsonIn.getString("accountNumber").substring(0, 20));
    jsonLog.put("creditNumber", "0").put("collectorId", 0).put("operationStatus", "1020").put("operationMessage", message);
    log.debug(message + jsonIn.getString("account"));
    writeLog(jsonLog);
  }
  private <T> void writeLog(JsonObject jsonLog) {
    vertx.eventBus().send(WRITE_LOG, jsonLog);
  }
}
