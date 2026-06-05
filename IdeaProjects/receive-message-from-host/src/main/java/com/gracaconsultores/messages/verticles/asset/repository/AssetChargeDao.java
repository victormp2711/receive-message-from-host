package com.gracaconsultores.messages.verticles.asset.repository;

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
public class AssetChargeDao {
    public JsonObject getAccountsByCollector(JsonObject jsonIn, DataSource ds) {
      log.info("getAccountsByCollector : " + jsonIn);
        final CallableStatement[] cStmt = {null};
        Connection conn = null;
        log.info("d");
        try {
            conn = ds.getConnection();
            if (conn != null) {
                cStmt[0] = conn.prepareCall("{CALL GIOM.PKG_SIC_DEBT_ACCOUNTS_COLLECTOR.GET_ACCOUNT_BY_COLLECTOR(?, ?, ?, ?)}");
                cStmt[0].setString("P_IN_ACCOUNT", jsonIn.getString("account"));
                cStmt[0].setInt("P_IN_COLLECTOR", jsonIn.getInteger("collector"));
                cStmt[0].registerOutParameter("P_OUT_DATA", OracleTypes.REF_CURSOR);
                cStmt[0].registerOutParameter("P_OUT_STATUS", OracleTypes.VARCHAR);
                cStmt[0].execute();
                String status = (String) cStmt[0].getObject("P_OUT_STATUS");
                log.info("getAccountsByCollector : " + status);

                if(status.startsWith("1000")) {
                    final ResultSet rs = (ResultSet) cStmt[0].getObject("P_OUT_DATA");
                    JsonArray arrayJson = new JsonArray();
                    while (rs.next()) {
                        JsonObject obj = new JsonObject();
                        obj.put("accountNumber", rs.getString("ACCOUNT_NUMBER"));
                        obj.put("creditNumber", rs.getString("CREDIT_NUMBER"));
                        obj.put("currency", rs.getString("CURRENCY"));
                        obj.put("fechaVencimiento", rs.getInt("FECHA_VENCIMIENTO"));
                        arrayJson.add(obj);
                    }
                    if(!arrayJson.isEmpty()) {
                        return new JsonObject().put("code", 1000).put("message", "success").put("data", arrayJson).put("status", 200);
                    } else {
                        return new JsonObject().put("code", 1020).put("message", "no hay registros para procesar cuenta").put("status", 200);
                    }
                } else {
                    return new JsonObject().put("code", 1030).put("message", status.substring(3)).put("status", 200);
                }
            } else {
                return new JsonObject().put("code", 1020).put("message", "error : no hay conexion con BD").put("status", 200);
            }
        } catch (Exception e) {
            log.info("error obteniendo conexion con BD : " + e.getMessage());
            return new JsonObject().put("code", 1020).put("message", "error obteniendo conexion con BD ");
        } finally{
            try {
                if(cStmt[0] !=null) cStmt[0].close();
                if(conn!=null) conn.close();
            } catch (SQLException e) {
                // Ignorar
            }
        }
    }
}
