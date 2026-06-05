package com.gracaconsultores.messages.verticles.repository;

import com.gracaconsultores.messages.ds.HikariConfigs;
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

@Slf4j
public class RepositoryLog extends AbstractVerticle {

  public static final String WRITE_LOG = "write.log";

  @Override
  public void start(Promise<Void> start) {
    log.debug("✅ RepositoryLog is ready" );
    eventBusConsumersAccount();
  }

  void eventBusConsumersAccount() {
    Future.<Void>future(promise -> vertx.eventBus().consumer(WRITE_LOG).handler(this::writeLog));
  }

  private Connection getConnectionSic() {
    try {
      return HikariConfigs.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeLog(Message<Object> msg) {
    JsonObject jsonIn = (JsonObject) msg.body();
    log.debug("Begin writeLog : " + jsonIn);
    final CallableStatement[] cStmt = {null};
    Connection conn = getConnectionSic();
    try {
      if (conn != null) {
        cStmt[0] = conn.prepareCall("{CALL GIOM.PKG_SIC_DEBT_ACCOUNTS_COLLECTOR.SET_LOG(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
        cStmt[0].setString("P_DATECREATED", jsonIn.getString("dateCreate"));
        //log.debug("dateCreate : " + jsonIn.getString("dateCreate"));
        cStmt[0].setString("P_ACCOUNT_NUMBER", jsonIn.getString("accountNumber"));
        cStmt[0].setString("P_CREDIT_NUMBER", jsonIn.getString("creditNumber"));
        cStmt[0].setString("P_COLLECTOR_ID", jsonIn.getString("collectorId"));
        cStmt[0].setString("P_DATE_SENT", jsonIn.getString("dateSent"));
        //log.debug("dateSent : " + jsonIn.getString("dateSent"));
        cStmt[0].setString("P_DATE_RECEIVED", jsonIn.getString("dateReceived"));
        //log.debug("dateReceived : " + jsonIn.getString("dateReceived"));
        cStmt[0].setString("P_OPERATION_STATUS", jsonIn.getString("operationStatus"));
        cStmt[0].setString("P_OPERATION_MESSAGE", jsonIn.getString("operationMessage"));
        cStmt[0].setString("P_ORIGIN", jsonIn.getString("origin"));
        cStmt[0].setString("P_ORIGIN_ID", jsonIn.getString("originId"));
        //cStmt[0].setInt("P_ID_DEBT_ACCOUNT", jsonIn.getInteger("idDebt"));
        cStmt[0].registerOutParameter("P_OUT_STATUS", OracleTypes.VARCHAR);
        cStmt[0].execute();
        //String status = (String) cStmt[0].getObject("P_OUT_STATUS");
        //log.debug("status: " + status);
      } else {
        log.debug("message error : " + "no se obtuvo conexion de BD");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally{
      //log.debug("End  writeLog : " + msg.body());
      try {
        if(cStmt[0] !=null) cStmt[0].close(); //close CallableStatement
        if(conn!=null) conn.close(); // close connection
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }
}
