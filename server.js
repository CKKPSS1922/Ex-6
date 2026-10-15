/**
 * Врата Судьбы — сервер (v2: сохранения и версии состояния)
 *
 * Что делает:
 *  - раздаёт статику из public/ и держит WebSocket-хаб для синхронизации;
 *  - хранит состояние игры (доска, токены, туман, чат, карточки, музыка) и ПИШЕТ ЕГО В ФАЙЛ,
 *    поэтому сессия не теряется при перезапуске инстанса;
 *  - версионирует состояние: клиент со старой копией не может затереть более свежую;
 *  - отдаёт состояние любому подключившемуся клиенту, а не только мастеру;
 *  - НЕ сбрасывает игру сам: обнуление только по команде reset-state от мастера.
 *
 * Сохранение: SAVE_FILE (по умолчанию savegame.json рядом с сервером).
 * ВАЖНО: на бесплатном Render файловая система эфемерная — файл переживает перезапуск
 * инстанса, но не редеплой. Для «вечных» сейвов подключается внешнее хранилище (Firebase).
 */

const fs = require("fs");
const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const SAVE_FILE = process.env.SAVE_FILE || path.join(__dirname, "savegame.json");
const SAVE_DEBOUNCE_MS = Math.max(200, parseInt(process.env.SAVE_DEBOUNCE_MS || "1500", 10));
const SAVE_EVERY_MS = Math.max(5000, parseInt(process.env.SAVE_EVERY_MS || "60000", 10));

function log(...args){
  console.log(new Date().toISOString().slice(11, 19), ...args);
}

// ---------- Состояние ----------
let gameState = null;      // весь мир игры
let stateVersion = 0;      // растёт при каждом изменении
let stateUpdatedAt = 0;    // когда состояние менялось (мс)
let lastSavedAt = 0;       // когда последний раз писали в файл
let saveTimer = null;
let pendingSave = false;

function loadSave(){
  try{
    if(!fs.existsSync(SAVE_FILE)){
      log("💾 Сохранение не найдено — стартуем с пустой доской");
      return false;
    }
    const raw = fs.readFileSync(SAVE_FILE, "utf8");
    const data = JSON.parse(raw);
    gameState = (data && data.state) ? data.state : null;
    stateVersion = parseInt(data && data.version) || 0;
    stateUpdatedAt = parseInt(data && data.updatedAt) || 0;
    lastSavedAt = Date.now();
    if(gameState){
      log(`💾 Загружено сохранение ${path.basename(SAVE_FILE)}: версия ${stateVersion}, ` +
          `${(raw.length / 1048576).toFixed(2)} МБ, изменено ${new Date(stateUpdatedAt).toLocaleString()}`);
    } else {
      log("💾 В сохранении пусто (доска обнулена мастером)");
    }
    return !!gameState;
  }catch(e){
    log("⚠ Не удалось прочитать сохранение:", e.message);
    return false;
  }
}

function saveNow(force){
  if(!force && !pendingSave && stateUpdatedAt <= lastSavedAt) return;
  try{
    const payload = JSON.stringify({ version: stateVersion, updatedAt: stateUpdatedAt, state: gameState });
    const tmp = SAVE_FILE + ".tmp";
    fs.writeFileSync(tmp, payload);
    fs.renameSync(tmp, SAVE_FILE);   // атомарная замена, чтобы не поймать обрезанный файл
    lastSavedAt = Date.now();
    pendingSave = false;
    log(`💾 Сохранено: версия ${stateVersion}, ${(payload.length / 1048576).toFixed(2)} МБ`);
  }catch(e){
    log("⚠ Ошибка записи сохранения:", e.message);
  }
}

function scheduleSave(){
  pendingSave = true;
  if(saveTimer) return;
  saveTimer = setTimeout(()=>{ saveTimer = null; saveNow(); }, SAVE_DEBOUNCE_MS);
}

/** Отметить изменение состояния (версия ++, запись в файл по таймеру). */
function touchState(saveOnly){
  if(!saveOnly){
    stateVersion++;
    stateUpdatedAt = Date.now();
  }
  scheduleSave();
}

/** Свежее ли клиентское состояние, чем то, что у сервера. */
function isClientStateNewer(clientVersion, clientUpdatedAt){
  const cv = parseInt(clientVersion) || 0;
  const ct = parseInt(clientUpdatedAt) || 0;
  if(!gameState) return true;
  if(cv > stateVersion) return true;
  if(cv === stateVersion && ct > stateUpdatedAt) return true;
  return false;
}

function adoptClientState(state, version, updatedAt, source){
  if(!state || typeof state !== "object") return false;
  if(!isClientStateNewer(version, updatedAt)) return false;
  gameState = state;
  stateVersion = Math.max(stateVersion + 1, parseInt(version) || 0);
  stateUpdatedAt = Math.max(Date.now(), parseInt(updatedAt) || 0);
  scheduleSave();
  log(`↻ Принято состояние от ${source}: версия ${stateVersion}`);
  return true;
}

const REPLACE_KEYS = new Set(["tokens","players","fog","drawings","board","music","settings","chat","characters"]);

function applyPatchToState(patch){
  if(!patch || typeof patch !== "object") return;
  if(!gameState) gameState = {};
  for(const k of Object.keys(patch)){
    if(patch[k] === null) delete gameState[k];
    else if(REPLACE_KEYS.has(k)) gameState[k] = patch[k];
    else if(Array.isArray(patch[k])) gameState[k] = patch[k];
    else if(typeof patch[k] === "object" && gameState[k] && typeof gameState[k] === "object" && !Array.isArray(gameState[k])){
      gameState[k] = Object.assign({}, gameState[k], patch[k]);
    } else gameState[k] = patch[k];
  }
}

// ---------- HTTP ----------
const app = express();
app.use(express.static(path.join(__dirname, "public"), {
  maxAge: "1h",
  setHeaders(res, p){ if(p.endsWith(".html")) res.setHeader("Cache-Control", "no-cache"); }
}));

// Health-check для Render
app.get("/healthz", (_, res)=> res.status(200).send("ok"));

// Состояние сохранения (только для чтения) — удобно для бэкапа и диагностики
app.get("/api/save", (_, res)=>{
  if(!gameState) return res.status(204).end();
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="vrata-save-v${stateVersion}.json"`);
  res.send(JSON.stringify({ version: stateVersion, updatedAt: stateUpdatedAt, state: gameState }));
});

// SPA fallback
app.get("*", (_, res)=> res.sendFile(path.join(__dirname, "public", "index.html")));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 50 * 1024 * 1024 });

// ---------- WebSocket ----------
function stateMessage(){
  return { type: "server-state", state: gameState, version: stateVersion, updatedAt: stateUpdatedAt };
}
function send(ws, obj){ try{ ws.send(JSON.stringify(obj)); }catch(e){} }
function sendState(ws){ send(ws, stateMessage()); }
function broadcast(data, exceptWs){
  const msg = typeof data === "string" ? data : JSON.stringify(data);
  wss.clients.forEach(client=>{
    if(client !== exceptWs && client.readyState === 1){
      try{ client.send(msg); }catch(e){}
    }
  });
}
function broadcastState(exceptWs){ broadcast(stateMessage(), exceptWs); }

function activeClients(){
  let n = 0;
  wss.clients.forEach(c=>{ if(c.readyState === 1) n++; });
  return n;
}

function heartbeat(){ this.isAlive = true; }
setInterval(()=>{
  wss.clients.forEach(ws=>{
    if(ws.isAlive === false){ try{ ws.terminate(); }catch(e){} return; }
    ws.isAlive = false;
    try{ ws.ping(); }catch(e){}
  });
}, 15000);

// Страховка: раз в минуту досохраняем, если были изменения
setInterval(()=>{ saveNow(); }, SAVE_EVERY_MS);

wss.on("connection", (ws)=>{
  ws.isAlive = true;
  ws.on("pong", heartbeat);

  // Новый клиент сразу получает актуальное состояние (мастер для этого не нужен)
  sendState(ws);
  log(`⚡ Клиент подключился (всего ${activeClients()}), версия ${stateVersion}`);

  ws.on("message", (raw)=>{
    let m;
    try{ m = JSON.parse(raw.toString()); }catch(e){ return; }
    if(!m || !m.type) return;

    switch(m.type){
      case "hello":
        sendState(ws);
        return;

      // Новый протокол: клиент предлагает своё состояние
      case "client-state": {
        const adopted = adoptClientState(m.state, m.version, m.updatedAt, "клиентом");
        if(adopted) broadcastState(ws);
        else sendState(ws);   // у сервера свежее — отдаём его
        return;
      }

      // Старый протокол: full-state от мастера (принимаем как предложение состояния)
      case "full-state": {
        const adopted = adoptClientState(m.state, m.version, m.updatedAt, "full-state");
        if(adopted) broadcastState(ws);
        return;
      }

      case "state-patch":
        if(m.patch){
          applyPatchToState(m.patch);
          touchState();
          broadcast(raw.toString(), ws);
        }
        return;

      case "chat":
        if(m.msg){
          if(!gameState) gameState = {};
          if(!Array.isArray(gameState.chat)) gameState.chat = [];
          gameState.chat.push(m.msg);
          if(gameState.chat.length > 200) gameState.chat = gameState.chat.slice(-200);
          touchState();
          broadcast(raw.toString(), ws);
        }
        return;

      case "presence":
        if(m.player && m.player.id){
          if(!gameState) gameState = {};
          if(!gameState.players) gameState.players = {};
          gameState.players[m.player.id] = Object.assign(gameState.players[m.player.id] || {}, m.player);
          touchState();
          broadcast(raw.toString(), ws);
        }
        return;

      case "character":
        // Карточка персонажа: живёт в реестре и переживает перезагрузки
        if(m.character && m.character.id){
          if(!gameState) gameState = {};
          if(!gameState.characters) gameState.characters = {};
          const prev = gameState.characters[m.character.id] || {};
          gameState.characters[m.character.id] = Object.assign({}, prev, m.character, { updatedAt: Date.now() });
          touchState();
          broadcast(raw.toString(), ws);
        }
        return;

      case "token-move":
        if(m.id && gameState && gameState.tokens && gameState.tokens[m.id]){
          gameState.tokens[m.id].x = m.x;
          gameState.tokens[m.id].y = m.y;
          touchState(true);   // позиции не меняют версию, но попадают в сохранение
        }
        broadcast(raw.toString(), ws);
        return;

      case "reset-state":
        // Мастер начал новую сессию — обнуляем и в памяти, и в файле
        gameState = null;
        stateVersion++;
        stateUpdatedAt = Date.now();
        scheduleSave();
        log(`🧹 Состояние сброшено мастером, версия ${stateVersion}`);
        broadcastState();
        return;

      default:
        // Всё остальное (music-cmd, npc-*, kick и т.п.) просто ретранслируем
        broadcast(raw.toString(), ws);
        return;
    }
  });

  ws.on("close", ()=>{
    log(`👋 Клиент отключился (осталось ${activeClients()})`);
  });
  ws.on("error", ()=>{});
});

// ---------- Старт ----------
loadSave();

if(require.main === module){
  server.listen(PORT, ()=>{
    log(`⚔  Врата Судьбы: слушаю порт ${PORT}, сохранение: ${SAVE_FILE}`);
  });
}

function shutdown(signal){
  log(`⏹ Получен ${signal} — сохраняю и выключаюсь`);
  try{ saveNow(true); }catch(e){}
  try{ server.close(()=>process.exit(0)); }catch(e){ process.exit(0); }
  setTimeout(()=>process.exit(0), 3000);
}
process.on("SIGTERM", ()=>shutdown("SIGTERM"));
process.on("SIGINT", ()=>shutdown("SIGINT"));

// Для автотестов
module.exports = {
  app, server, wss,
  __test: {
    loadSave, saveNow, scheduleSave,
    getState: ()=> gameState,
    setState: (s)=>{ gameState = s; },
    getVersion: ()=> stateVersion,
    setVersion: (v)=>{ stateVersion = v; },
    resetMemory: ()=>{ gameState = null; stateVersion = 0; stateUpdatedAt = 0; lastSavedAt = 0; },
    isClientStateNewer, adoptClientState, applyPatchToState
  }
};
