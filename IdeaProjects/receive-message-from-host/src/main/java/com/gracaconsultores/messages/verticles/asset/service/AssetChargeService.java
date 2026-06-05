package com.gracaconsultores.messages.verticles.asset.service;


import com.gracaconsultores.messages.verticles.asset.repository.AssetChargeDao;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;

@Slf4j
public class AssetChargeService {
    private final AssetChargeDao dao;
    private final DataSource ds;
    private final Vertx vertx;

    public AssetChargeService(Vertx vertx, DataSource ds) {
        this.vertx = vertx;
        this.ds = ds;
        this.dao = new AssetChargeDao();
    }

    public Future<JsonObject> processAssetCharge(JsonObject jsonIn) {
      log.info("processAssetCharge : " + jsonIn);
        Promise<JsonObject> promise = Promise.promise();
        vertx.executeBlocking(fut -> {
            JsonObject result = dao.getAccountsByCollector(jsonIn, ds);
            fut.complete(result);
        }, res -> {
            if (res.succeeded()) {
              promise.complete((JsonObject) res.result());
            } else {
              log.info("AssetChargeService - processAssetCharge : " + res.result());
              promise.complete((JsonObject) res.result());
            }
        });
        return promise.future();
    }
}
