package com.gracaconsultores.messages.ds;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleTypes;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.gracaconsultores.messages.ds.HikariConfigs.getConnection;

@Slf4j
public class SchedulerConfigs extends AbstractVerticle {

  private static int periodic;
  private static long timerID;
  public static  boolean isActive_1 = false;
  public static  boolean isActive_2 = false;
  public static  boolean isActive_3 = false;
  public static  Map<String, Object> info = new LinkedHashMap<>();
  public static  Map<String, JsonObject> objectCollector = new LinkedHashMap<>();
  public static Map<String, String> operationCodes = new ConcurrentHashMap<>();
  public static Map<String, String> accountsPilot = new ConcurrentHashMap<>();
  private static final Locale SPANISH = new Locale("es");

  // NUEVO: Enum para las métricas
  public enum MetricKey {
    REQUEST("request"),
    SUCCESS_ACTIVOS("successActivos"),
    SUCCESS_DOMIC("successDomic"),
    NOT_SUCCESS_ACTIVOS("notSuccessActivos"),
    NOT_SUCCESS_DOMIC("notSuccessDomic"),
    TIMEOUT("timeOut"),
    FAIL("fail"),
    EXCEPTION("exception"),
    EXCEPTION_DOMIC("exceptionDomic"),
    ACTIVOS("activos"),
    DOMIC("domic"),
    AMOUNT_UVC("amountUVC"),
    AMOUNT_VES("amountVES"),
    ACCOUNT_BLOCKED("accountBlocked"),
    ACCOUNT_NO_BLOCKED("accountNoBlocked"),
    NOT_FOUND("notFound"),
    NOT_CONNECTION_DB("notConnectionDb"),
    TIMEOUT_DOMIC("timeOutDomic");
    private final String key;
    MetricKey(String key) { this.key = key; }
    public String key() { return key; }
  }

  // NUEVO: Servicio para lógica de scheduling y métricas
  private SchedulerService schedulerService;
  private final org.joda.time.format.DateTimeFormatter df = DateTimeFormat.forPattern("HH:mm:ss");

  @Override
  public void start(Promise<Void> start) {
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig().onComplete(json -> {
        if (json.succeeded()) {
          JsonObject env = json.result();
          JsonObject ds1 = env.getJsonObject("scheduler");
          periodic = ds1.getInteger("periodic");
          /*try {
            log.info("scheduler : " + CronExpressionDescriptor.getDescription(String.valueOf(ds1.getInteger("periodic")), SPANISH));
          } catch (ParseException e) {
            throw new RuntimeException(e);
          }*/
        }
    });

    vertx.eventBus().<JsonObject>consumer("new-configuration",  this::newConfig);
    schedulerService = new SchedulerService(df);
    processScheduler(periodic);
  }

  /**
   * Inicializa las métricas y programa la tarea periódica.
   */
  private void processScheduler(int periodic) {
    log.info("se cangan por primeravez los parametros de collectores y los codigos de operacion");
    processEvent(); // se cangan por primeravez los parametros de collectores y los codigos de operacion
    //log.info("statistics : " + info);
    log.debug("executing process priodically " + periodic + " ms");
    timerID = vertx.setPeriodic(periodic, (l) -> processEvent());
  }

  /**
   * Ejecuta la actualización de collectors y métricas periódicamente.
   * Maneja errores sin detener el verticle.
   */
  private void processEvent() {
    //log.info("processEvent");
    try {
      schedulerService.updateCollectorsAndMetrics(info);
      schedulerService.getOperationCodes();
      //schedulerService.getAccountsPilot();
    } catch (Exception e) {
      log.info("Error in processEvent: ", e);
      info.put(MetricKey.EXCEPTION.key(), ((int) info.getOrDefault(MetricKey.EXCEPTION.key(), 0)) + 1);
    }
  }

  private  void newConfig(Message msg) {
    JsonObject jsonConfig = (JsonObject) msg.body();
    JsonObject ds = jsonConfig.getJsonObject("scheduler");
    int newPeriodic = ds.getInteger("periodic");
    log.debug("updated new chage cofiguration");
    if(newPeriodic != periodic){
      vertx.cancelTimer(timerID);
      processScheduler(newPeriodic);
    }
  }

  private Connection getConnectionSic() {
    try {
      return getConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  // NUEVO: Servicio interno para lógica de scheduling y métricas
  public static class SchedulerService {
    private final org.joda.time.format.DateTimeFormatter df;
    public SchedulerService(org.joda.time.format.DateTimeFormatter df) {
      this.df = df;
    }
    /** Inicializa las métricas en el mapa info. */
    public void initMetrics(Map<String, Object> info) {
      for (MetricKey key : MetricKey.values()) {
        if (key == MetricKey.AMOUNT_UVC || key == MetricKey.AMOUNT_VES) {
          info.put(key.key(), 0.0D);
        } else {
          info.put(key.key(), 0);
        }
      }
    }

    /**
     * Actualiza el estado de los collectors y métricas desde la base de datos.
     * Maneja errores y no lanza excepciones.
     */
    public void updateCollectorsAndMetrics(Map<String, Object> info) {
      CallableStatement[] cStmt = {null};
      Connection conn = null;
      try {
        conn = getConnection();
        if (conn != null) {
          log.debug("conexion valida");
          cStmt[0] = conn.prepareCall("{CALL GIOM.PKG_SIC_DEBT_ACCOUNTS_COLLECTOR.GET_SCHEDULER(?, ?)}");
          cStmt[0].registerOutParameter("P_OUT_DATA", OracleTypes.REF_CURSOR);
          cStmt[0].registerOutParameter("P_OUT_STATUS", OracleTypes.VARCHAR);
          cStmt[0].execute();
          final ResultSet rs = (ResultSet) cStmt[0].getObject("P_OUT_DATA");
          JsonArray arrayJson = new JsonArray();
          while (rs.next()) {
            JsonObject obj = new JsonObject();
            obj.put("collectorId", rs.getString("COLLECTOR_ID"));
            obj.put("limitStart", rs.getString("START_TIME"));
            obj.put("limitStop", rs.getString("FINAL_TIME"));
            obj.put("status", rs.getString("STATUS"));
            obj.put("collectionType", rs.getObject("COLLECTION_TYPE") != null ? rs.getInt("COLLECTION_TYPE") : 1);
            arrayJson.add(obj);
          }
          if (!arrayJson.isEmpty()) {
            LocalDateTime today = LocalDateTime.now();
            String actual = today.toString().substring(11,11+8);
            for (Object item : arrayJson) {
              JsonObject obj = (JsonObject) item;
              boolean isActive = false;
              String status = obj.getString("status");
              if(status.equalsIgnoreCase("ACTIVO")) {
                DateTime actualTime = df.parseLocalTime(actual).toDateTimeToday();
                DateTime limStart = df.parseLocalTime(obj.getString("limitStart").substring(11)).toDateTimeToday();
                DateTime limStop = df.parseLocalTime(obj.getString("limitStop").substring(11)).toDateTimeToday();
                if (!actualTime.isBefore(limStart) && !actualTime.isAfter(limStop)) {
                  isActive = true;
                }
              } else {
                isActive = false;
              }
              obj.put("isActive", isActive);
              objectCollector.put(obj.getString("collectorId"), obj);
            }
            //log.info("objectCollector : " + objectCollector);
          } else {
            info.put(MetricKey.NOT_FOUND.key(), ((int) info.getOrDefault(MetricKey.NOT_FOUND.key(), 0)) + 1);
          }
        } else {
          log.info("arrayJson vacio");
          info.put(MetricKey.NOT_CONNECTION_DB.key(), ((int) info.getOrDefault(MetricKey.NOT_CONNECTION_DB.key(), 0)) + 1);
        }
      } catch (Exception e) {
        info.put(MetricKey.EXCEPTION.key(), ((int) info.getOrDefault(MetricKey.EXCEPTION.key(), 0)) + 1);
      } finally {
        try {
          if (cStmt[0] != null) cStmt[0].close();
          if (conn != null) conn.close();
        } catch (SQLException e) {
          // Ignorar
        }
      }
    }

    public void getOperationCodes() {
      CallableStatement[] cStmt = {null};
      Connection conn = null;
      try {
        conn = getConnection();
        if (conn != null) {
          cStmt[0] = conn.prepareCall("{CALL GIOM.PKG_SIC_DEBT_ACCOUNTS_COLLECTOR.GET_OPERATION_CODES(?, ?)}");
          cStmt[0].registerOutParameter("P_OUT_DATA", OracleTypes.REF_CURSOR);
          cStmt[0].registerOutParameter("P_OUT_STATUS", OracleTypes.VARCHAR);
          cStmt[0].execute();
          final ResultSet rs = (ResultSet) cStmt[0].getObject("P_OUT_DATA");
          if(rs != null) {
            operationCodes.clear();
            while (rs.next()) {
              //log.info("reg : " + rs.getString(1));
              operationCodes.put(rs.getString("OPERATION_CODE"), rs.getString("OPERATION_CODE"));
            }
          } else {
            log.info("No hay codigos de operacion cargados en tabla");
          }
        } else {
          log.info("no se pudo obtener los conexion con BD");
        }
          //log.info("codigos de operacion cargados correctamente : " + operationCodes);
      } catch (Exception e) {
        info.put(MetricKey.EXCEPTION.key(), ((int) info.getOrDefault(MetricKey.EXCEPTION.key(), 0)) + 1);
        log.info("Error obteniendo codigos de operacion " + e.getMessage());
      } finally {
        try {
          if (cStmt[0] != null) cStmt[0].close();
          if (conn != null) conn.close();
        } catch (SQLException e) {
          // Ignorar
        }
      }
    }

    public void getAccountsPilot() {
      CallableStatement[] cStmt = {null};
      Connection conn = null;
      try {
        conn = getConnection();
        if (conn != null) {
          cStmt[0] = conn.prepareCall("{CALL GIOM.PKG_SIC_DEBT_ACCOUNTS_COLLECTOR.GET_ACCOUNTS_PILOT(?, ?)}");
          cStmt[0].registerOutParameter("P_OUT_DATA", OracleTypes.REF_CURSOR);
          cStmt[0].registerOutParameter("P_OUT_STATUS", OracleTypes.VARCHAR);
          cStmt[0].execute();
          final ResultSet rs = (ResultSet) cStmt[0].getObject("P_OUT_DATA");
          if(rs != null) {
            //accountsPilot.clear();
            while (rs.next()) {
              //log.info("reg : " + rs.getString(1));
              accountsPilot.put(rs.getString("ACCOUNT"), rs.getString("ACCOUNT"));
            }
          } else {
            log.info("No hay cuentas pilotos cargados en tabla");
          }
        } else {
          log.info("no se pudo obtener conexion con BD para las cuentas pilotos");
        }
        //log.info("codigos de operacion cargados correctamente : " + operationCodes);
      } catch (Exception e) {
        info.put(MetricKey.EXCEPTION.key(), ((int) info.getOrDefault(MetricKey.EXCEPTION.key(), 0)) + 1);
        log.info("Error obteniendo cuentas pilotos " + e.getMessage());
      } finally {
        try {
          if (cStmt[0] != null) cStmt[0].close();
          if (conn != null) conn.close();
        } catch (SQLException e) {
          // Ignorar
        }
      }
    }
  }
}
