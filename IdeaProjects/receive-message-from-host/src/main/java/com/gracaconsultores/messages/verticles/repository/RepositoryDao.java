package com.gracaconsultores.messages.verticles.repository;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleTypes;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class RepositoryDao {
    public JsonArray getAccountsForAllCollectors(JsonObject jsonIn, DataSource ds) {
        log.info("Begin getAccountsForAllCollectors : " + jsonIn);
        final CallableStatement[] cStmt = {null};
        Connection conn = null;
        try {
            conn = ds.getConnection();
            if (conn != null) {
                cStmt[0] = conn.prepareCall("{CALL GIOM.PKG_SIC_DEBT_ACCOUNTS_COLLECTOR.GET_ACCOUNT_FOR_ALL_COLLECTORS(?, ?, ?)}");
                cStmt[0].setString("P_IN_ACCOUNT", jsonIn.getString("account"));
                cStmt[0].registerOutParameter("P_OUT_DATA", OracleTypes.REF_CURSOR);
                cStmt[0].registerOutParameter("P_OUT_STATUS", OracleTypes.VARCHAR);
                cStmt[0].execute();
                ResultSet rs = (ResultSet) cStmt[0].getObject("P_OUT_DATA");
                JsonArray arrayJson = new JsonArray();
                while (rs.next()) {
                    JsonObject obj = new JsonObject();
                    obj.put("accountNumber", rs.getString("ACCOUNT_NUMBER"));
                    obj.put("customer", rs.getString("ID_CUSTOMER"));
                    obj.put("idCollector", rs.getInt("ID_DEBT_COLLECTOR"));
                    obj.put("priority", rs.getString("PRIORITY"));
                    arrayJson.add(obj);
                }
                log.info("P_OUT_STATUS : " + (String) cStmt[0].getObject("P_OUT_STATUS"));
                if(arrayJson.isEmpty()){
                  log.info("no existen registros en la tabla de cuentas para la cuenta : " + jsonIn.getString("account"));
                }
                return arrayJson;
            } else {
                return new JsonArray();
            }
        } catch (Exception e) {
            log.info("Exception : " + e.getMessage());
            return new JsonArray();
        } finally {
            try {
                if (cStmt[0] != null) cStmt[0].close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                // Ignorar
            }
          log.info("End  getAccountsForAllCollectors : " + jsonIn);
        }
    }
}
