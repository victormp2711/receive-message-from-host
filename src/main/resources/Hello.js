/*
vertx.eventBus().consumer("hello.vertx.addr", function(msg) {
  msg.reply("Hello Vert.x World from JavaScript!");
});*/
console.log("[Worker] Starting in " + Java.type("java.lang.Thread").currentThread().getName());

vertx.eventBus().consumer("WORKER"+instance, function (message) {
  var identifier = message.body();
  var r = {
      id: identifier,
      title: "",
      url: ""
  }
  message.reply(JSON.stringify(r));
});
