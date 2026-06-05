package com.gracaconsultores.messages.verticles.repository;

import com.gracaconsultores.messages.ds.HikariConfigs;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleTypes;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import static com.gracaconsultores.messages.ds.SchedulerConfigs.info;

@Slf4j
public class RepositoryAccounts extends AbstractVerticle {
  public static final String LOCK_ACCOUNT = "lock.account";
  public static final String FREE_ACCOUNT = "free.account";
  @Override
  public void start(Promise<Void> start) {
    //vertx.setPeriodic(1000 * 60 * 60, (l) -> { -- cada hora
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig().onComplete(json -> {
      if (json.succeeded()){
        log.debug("✅ Repository is ready" );
      }
    });
    eventBusConsumers();
  }

  void eventBusConsumers() {
    Future.<Void>future(promise -> {
      vertx.eventBus().consumer(LOCK_ACCOUNT).handler(this::lockAccount);
      vertx.eventBus().consumer(FREE_ACCOUNT).handler(this::freeAccount);
    });
  }

  private Connection getConnectionSic() {
    try {
      return HikariConfigs.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void lockAccount(Message<Object> msg) {
    JsonObject jsonInLock = (JsonObject) msg.body();
    String accountIn = jsonInLock.getString("account");
    String customerIn = jsonInLock.getString("customer");
    int collectorType = jsonInLock.getInteger("collectorType");
    log.info("Begin lockAccount : " + jsonInLock);
    final CallableStatement[] cStmt = {null};
    Connection conn = getConnectionSic();
    try {
      if (conn != null) {
        //log.debug("- llamando al store procedure");
        cStmt[0] = conn.prepareCall("{CALL GIOM.SIC_BLOCKER.BLOCK(?, ?, ?, ?, ?)}");
        cStmt[0].setString("P_ACCOUNT_NUMBER", accountIn.substring(0,20));
        cStmt[0].setString("P_CUSTOMER", customerIn);
        cStmt[0].setInt("P_COLLECTION_TYPE", collectorType);
        cStmt[0].registerOutParameter("COD_RET", OracleTypes.VARCHAR);
        cStmt[0].registerOutParameter("DE_RET", OracleTypes.VARCHAR);
        cStmt[0].setQueryTimeout(2);
        cStmt[0].execute();
        //log.debug("despues de ejecutar el store procedure");
        String status = (String) cStmt[0].getObject("COD_RET");
        String message = (String) cStmt[0].getObject("DE_RET");
        conn.close();
        log.info("status lockAccount : " + status);
        if(status != null && status.equals("1000"))  {
          msg.reply(status);
        } else {
          String messageError;
          if(status.equals("1030")) {
            messageError = "Cuenta ya está en proceso de cobranza";
          } else if (status.equals("1040")) {
            messageError = "Cliente ya está en proceso de cobranza";
          } else {
            messageError = message;
          }
          msg.fail(Integer.parseInt(status), messageError);
        }
      }
    } catch (Exception e) {
      int exception = (info.get("exception") != null) ? (int) info.get("exception") : 0;
      exception++;
      info.put("exception blockeo", exception);
      msg.fail(1098, "blockeo");
      throw new RuntimeException(e);
    } finally {
      log.debug("End lockAccount");
      try {
        if (cStmt[0] != null) cStmt[0].close(); //close CallableStatement
        if (conn != null) conn.close(); // close connection
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  private void freeAccount(Message<Object> msg) {
    String accountIn = (String) msg.body();
    log.debug("Begin freeAccount : " + accountIn);
    final CallableStatement[] cStmt = {null};
    Connection conn = getConnectionSic();
    try {
      if (conn != null) {
        //log.debug("- llamando al store procedure");
        cStmt[0] = conn.prepareCall("{CALL GIOM.SIC_BLOCKER.FREE(?, ?, ?)}");
        cStmt[0].setString("P_ACCOUNT_NUMBER", accountIn.substring(0,20));
        cStmt[0].registerOutParameter("COD_RET", OracleTypes.VARCHAR);
        cStmt[0].registerOutParameter("DE_RET", OracleTypes.VARCHAR);
        cStmt[0].execute();
        //log.debug("despues de ejecutar el store procedure");
        String status = (String) cStmt[0].getObject("COD_RET");
        conn.close();
        log.info("status freeAccount : " + status);
        msg.reply(status);
      }
    } catch (Exception e) {
      int exception = (info.get("exception") != null) ? (int) info.get("exception") : 0;
      exception++;
      info.put("exception", exception);
      throw new RuntimeException(e);
    } finally {
      log.debug("End freeAccount ");
      try {
        if (cStmt[0] != null) cStmt[0].close(); //close CallableStatement
        if (conn != null) conn.close(); // close connection
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
