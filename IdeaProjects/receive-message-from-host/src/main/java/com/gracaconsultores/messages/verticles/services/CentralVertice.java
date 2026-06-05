package com.gracaconsultores.messages.verticles.services;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class CentralVertice  extends AbstractVerticle {
  public static final String VERIFY_ACCOUNT_IN_PROCESS = "verify.account.in.process";
  public static final String FREE_ACCOUNT_IN_PROCESS = "free.account.in.process";
  public static ConcurrentHashMap<String, Object> cacheAccount = new ConcurrentHashMap<>();

  @Override
  public void start(Promise<Void> start) {
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig().onComplete(json -> {
      if (json.succeeded()){
        log.debug("✅ CentralVertice is ready" );
      }
    });

    eventBusConsumersAccount();
  }

  void eventBusConsumersAccount() {
    Future.<Void>future(promise -> {
      vertx.eventBus().consumer(VERIFY_ACCOUNT_IN_PROCESS).handler(this::verifyAccount);
      vertx.eventBus().consumer(FREE_ACCOUNT_IN_PROCESS).handler(this::freeAccount);
    });
  }

  private void verifyAccount(Message<Object> message) {
    log.debug("Verifying account : " + message.body());
    String account = (String) message.body();
    if(cacheAccount.containsKey(account)) {
      String dtaeTimeAccount = cacheAccount.get(account).toString();
      log.debug("cacheAccount contains " + account + " : " + dtaeTimeAccount);
      String[] dates = dtaeTimeAccount.substring(0,10).split("-");
      String[] hours = dtaeTimeAccount.substring(11,11+8).split(":");
      //log.debug("dates : " + dates.toString());
      //log.debug("hours : " + hours.toString());
      DateTime dtAccount = new DateTime(Integer.parseInt(dates[0]), Integer.parseInt(dates[1]), Integer.parseInt(dates[2]), Integer.parseInt(hours[0]), Integer.parseInt(hours[1]), Integer.parseInt(hours[2]));
      DateTime dtNow = new DateTime();
      Period period = new Period(dtAccount, dtNow);
      int millis = period.getMillis();

      //log.debug("dtAccount : " + dtAccount);
      //log.debug("dtNow : " + dtNow);
      //log.debug("period : " + period);
      log.debug("millis : " + millis);
      if(millis > 1500) {
        String time = String.valueOf(new DateTime());
        cacheAccount.putIfAbsent(account, time);
        message.reply("success");
      } else {
        message.fail(1010, "account_already_being_processed");
      }
    } else {
      log.debug("cacheAccount no contains " + account);
      String time = String.valueOf(new DateTime().withMillisOfSecond(0));
      cacheAccount.putIfAbsent(account, time);
      message.reply("success");
    }
  }

  private void freeAccount(Message<Object> message) {
    log.debug("freeAccount account : " + message.body());
    String account = (String) message.body();
    cacheAccount.remove(account);
  }
}
