/**
 * Врата Судьбы — сервер
 * Раздаёт статику из public/ и держит WebSocket-хаб для синхронизации.
 * Все клиенты получают широковещательные сообщения; сервер хранит
 * последнее состояние (state) в памяти и отдаёт его новым подключениям.
 *
 * ВАЖНО: когда последний участник отключается — состояние полностью сбрасывается,
 * чтобы новая сессия начиналась с чистой доски.
 */

const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.static(path.join(__dirname, "public"), {
  maxAge: "1h",
  setHeaders(res, p){
    if(p.endsWith(".html")) res.setHeader("Cache-Control", "no-cache");
  }
}));

// Health-check для Render
app.get("/healthz", (_,res)=> res.status(200).send("ok"));

// SPA fallback
app.get("*", (_,res)=> res.sendFile(path.join(__dirname, "public", "index.html")));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 50 * 1024 * 1024 });

// Состояние игры хранится в памяти сервера
let gameState = null;

// Ping для отсева мёртвых соединений
function heartbeat(){ this.isAlive = true; }
setInterval(()=>{
  wss.clients.forEach(ws=>{
    if(ws.isAlive === false){
      try{ ws.terminate(); }catch(e){}
      return;
    }
    ws.isAlive = false;
    try{ ws.ping(); }catch(e){}
  });
  // Если после ping не осталось живых клиентов — сбрасываем состояние
  checkAndResetIfEmpty();
}, 15000);

function activeClients(){
  let n = 0;
  wss.clients.forEach(c=>{ if(c.readyState === 1) n++; });
  return n;
}

function resetState(reason){
  if(gameState !== null){
    console.log(`🧹 Сброс состояния (${reason})`);
    gameState = null;
  }
}

let emptySince = null;
function checkAndResetIfEmpty(){
  if(activeClients() === 0){
    if(emptySince === null) emptySince = Date.now();
    // Даём 30 секунд «милости» на переподключение (перезагрузка страницы и т.п.)
    if(Date.now() - emptySince > 30000){
      resetState("нет активных клиентов > 30 сек");
      emptySince = null;
    }
  } else {
    emptySince = null;
  }
}

function broadcast(data, exceptWs){
  const msg = typeof data === "string" ? data : JSON.stringify(data);
  wss.clients.forEach(client=>{
    if(client !== exceptWs && client.readyState === 1){
      try{ client.send(msg); }catch(e){}
    }
  });
}

wss.on("connection", (ws)=>{
  ws.isAlive = true;
  ws.on("pong", heartbeat);
  emptySince = null; // кто-то пришёл — сбрасываем таймер сброса
  console.log(`⚡ Клиент подключился. Всего: ${activeClients()}`);

  // Присылаем последнее состояние новому клиенту (если ещё есть)
  if(gameState){
    try{ ws.send(JSON.stringify({ type: "full-state", state: gameState })); }catch(e){}
  } else {
    // Явно скажем клиенту, что сервер пустой — пусть первый пришедший инициализирует состояние
    try{ ws.send(JSON.stringify({ type: "server-empty" })); }catch(e){}
  }

  ws.on("message", (raw)=>{
    let m;
    try{ m = JSON.parse(raw.toString()); }catch(e){ return; }
    if(!m || !m.type) return;

    // Сервер запоминает актуальный state
    if(m.type === "full-state" && m.state){
      gameState = m.state;
    } else if(m.type === "state-patch" && m.patch){
      if(!gameState) gameState = {};
      // Всегда полностью заменяем — иначе удалённые ключи (например NPC) остаются
      for(const k of Object.keys(m.patch)){
        gameState[k] = m.patch[k];
      }
    } else if(m.type === "chat" && m.msg){
      // Чат-сообщения тоже сохраняем в общем состоянии
      if(!gameState) gameState = {};
      if(!Array.isArray(gameState.chat)) gameState.chat = [];
      gameState.chat.push(m.msg);
      if(gameState.chat.length > 200) gameState.chat = gameState.chat.slice(-200);
    } else if(m.type === "presence" && m.player){
      // Тоже пишем в state
      if(!gameState) gameState = {};
      if(!gameState.players) gameState.players = {};
      gameState.players[m.player.id] = Object.assign(gameState.players[m.player.id]||{}, m.player);
    } else if(m.type === "token-move" && m.id){
      // Живое перемещение — сохраняем актуальную позицию
      if(gameState && gameState.tokens && gameState.tokens[m.id]){
        gameState.tokens[m.id].x = m.x;
        gameState.tokens[m.id].y = m.y;
      }
    } else if(m.type === "reset-state"){
      // Ручной сброс (мастер запросил новую сессию с нуля)
      resetState("запрос reset-state от клиента");
      // не ретранслируем, клиент сам broadcast'ит full-state с чистым состоянием
      return;
    }

    // Ретранслируем всем остальным
    broadcast(raw.toString(), ws);
  });

  ws.on("close", ()=>{
    console.log(`👋 Клиент отключился. Осталось: ${activeClients()}`);
    // Быстрая проверка — вдруг это был последний
    setTimeout(checkAndResetIfEmpty, 100);
  });
  ws.on("error", ()=>{});
});

server.listen(PORT, ()=>{
  console.log(`⚔  Врата Судьбы: слушаю порт ${PORT}`);
});

