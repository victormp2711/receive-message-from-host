package com.gracaconsultores.messages.verticles.services;

import com.gracaconsultores.messages.ds.HikariConfigs;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import static com.gracaconsultores.messages.ds.SchedulerConfigs.info;


public class Service extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(Service.class);
  public static final String GET_ACCOUNT_FROM_COLLECT = "get.account.from.collect";
  public static final DateTimeFormatter formatTimeDate = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS");
  public static final String WRITE_LOG = "write.log";
  public static final String LOCK_ACCOUNT = "lock.account";
  public static final JsonObject bodyResp = new JsonObject();
  private static JsonObject env = new JsonObject();
  public static final String MESSAGE_FROM_HOST = "message.from.host";
  public static final String ORIGIN = "PIC";


  @Override
  public void start(Promise<Void> start) {
    //vertx.setPeriodic(1000 * 60 * 60, (l) -> { -- cada hora
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig().onComplete(json -> {
      if (json.succeeded()){
        log.debug("✅ Service is ready" );
      }
      env = json.result();
      vertx.eventBus().consumer(MESSAGE_FROM_HOST).handler(this::messageFromHost);
    });
  }

  public void messageFromHost(Message<Object> msg) {
    log.info("Begin messageFromHost : " + msg.body());
    try {
      JsonObject jsonBody = (JsonObject) msg.body();
      String account = jsonBody.getString("account").substring(0,20);
      if (!jsonBody.containsKey("account") || jsonBody.getString("account").length() < 20) {
        bodyResp.put("code", 1010).put("message", "Missing or invalid 'account' field");
        msg.reply(bodyResp);
      } else {
        bodyResp.put("code", 1000).put("message", "success");
        msg.reply(bodyResp); // Respuesta inmediata intencional

        incrementStat("request");
        jsonBody.put("account", account).put("originId", UUID.randomUUID().toString()).put("origin", ORIGIN);
        getAccountFromCollect(jsonBody);
      }
    } catch (Exception e) {
      msg.fail(400, "Invalid JSON body");
    }
    log.info("statistics : " + info.toString());
  }

  private void getAccountFromCollect(JsonObject jsonIn) {
    log.debug("Begin getAccountFromCollect : " + jsonIn);
    JsonObject jsonAccount = new JsonObject();
    jsonAccount.put("account", jsonIn.getString("account").substring(0,20));
    vertx.eventBus().send(GET_ACCOUNT_FROM_COLLECT, jsonIn);
  }

  public void getStatistics(RoutingContext context) {
    log.debug("Begin getStatics");
    context.request().bodyHandler((bodyHandler) -> {
      context.json(info);
      log.debug("End  getStatics");
    });
  }

  public void cleanMemoryAccount(RoutingContext context) {
    log.debug("Begin cleanMemoryAccount");
    context.request().bodyHandler((bodyHandler) -> vertx.eventBus().request("cleanMemoryAccount", "", reply -> {
      if (reply.succeeded()) {
        context.json(reply);
      } else {
        log.debug("Error cleaning memory account");
      }
    }));
  }

  public void getMemoryAccount(RoutingContext context) {
    log.debug("Begin getMemoryAccount");
    context.request().bodyHandler((bodyHandler) -> vertx.eventBus().request("getMemoryAccount", "", reply -> {
      if (reply.succeeded()) {
        context.json(reply);
      } else {
        log.debug("Error getting memory account");
      }
    }));
  }

  public void testDb(RoutingContext context) {
    //log.info("testDb");
    context.request().bodyHandler(bodyHandler -> {
      Connection conn = null;
      try {
        conn = HikariConfigs.getConnection();
        if (conn != null) {
          //log.info("connection ok");
          conn.close();
          Calendar cal = Calendar.getInstance();
          cal.setTime(new Date( ));
          if (cal.get(Calendar.HOUR_OF_DAY) == env.getJsonObject("restart").getInteger("hour")) {
            if (cal.get(Calendar.MINUTE) == env.getJsonObject("restart").getInteger("minute")) {
              if (cal.get(Calendar.SECOND) > env.getJsonObject("restart").getInteger("second")) {
                //log.info("reinicio controlado");
                context.response().setStatusCode(500);
              } else {
                //log.info("todo bien");
                context.json(new JsonObject().put("code", 1000).put("message", "success"));
              }
            } else {
              //log.info("todo bien");
              context.json(new JsonObject().put("code", 1000).put("message", "success"));
            }
          } else {
            //log.info("todo bien");
            context.json(new JsonObject().put("code", 1000).put("message", "success"));
          }
        } else {
          log.info("Connection NOT established");
          context.response().setStatusCode(500);
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private <T> void writeLog(JsonObject jsonLog) {
    vertx.eventBus().send(WRITE_LOG, jsonLog);
  }

  private void respondWithError(RoutingContext context, int status, String message) {
    context.response().setStatusCode(status).putHeader("Content-Type", "application/json")
      .end(new JsonObject().put("code", status).put("message", message).encode());
  }

  private void incrementStat(String key) {
    int value = (info.get(key) != null) ? (int) info.get(key) : 0;
    value++;
    info.put(key, value);
  }

  private void writeEventLog(String key, String operationMessage) {
    JsonObject jsonLog = new JsonObject();
    jsonLog.put("accountNumber", key);
    String timestampformat = LocalDateTime.now().format(formatTimeDate);
    jsonLog.put("dateCreate", timestampformat);
    jsonLog.put("originId", UUID.randomUUID().toString()).put("origin", ORIGIN);
    jsonLog.put("operationStatus", "1098").put("operationMessage", operationMessage);
    writeLog(jsonLog);
  }
}
