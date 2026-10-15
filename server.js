/**
 * Врата Судьбы — сервер (v3: сохранения + внешнее хранилище)
 *
 * Что делает:
 *  - раздаёт статику из public/ и держит WebSocket-хаб для синхронизации;
 *  - хранит состояние игры (доска, токены, туман, чат, карточки, музыка);
 *  - сохраняет его в файл (переживает перезапуск процесса) и, если задан ключ Firebase,
 *    в Realtime Database — тогда сейв переживает и редеплой/сон бесплатного инстанса;
 *  - версионирует состояние: клиент со старой копией не может затереть более свежую;
 *  - отдаёт состояние любому подключившемуся клиенту, а не только мастеру;
 *  - НЕ сбрасывает игру сам: обнуление только по команде reset-state от мастера.
 *
 * Хранилище:
 *   файл:      SAVE_FILE (по умолчанию savegame.json рядом с сервером)
 *   Firebase:  FIREBASE_SERVICE_ACCOUNT (JSON сервисного аккаунта одной строкой)
 *              или FIREBASE_SERVICE_ACCOUNT_FILE (путь к JSON)
 *              FIREBASE_DB_URL — URL базы (иначе соберём из project_id и FIREBASE_REGION)
 *              FIREBASE_REGION — регион базы, по умолчанию europe-west1
 *              FIREBASE_PATH — путь в базе, по умолчанию games/vrata-sudby
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
const CLOUD_DEBOUNCE_MS = Math.max(2000, parseInt(process.env.CLOUD_DEBOUNCE_MS || "8000", 10));
const CLOUD_MIN_INTERVAL_MS = Math.max(5000, parseInt(process.env.CLOUD_MIN_INTERVAL_MS || "30000", 10));

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
      log("💾 Файл сохранения не найден — стартуем с пустой доской");
      return false;
    }
    const raw = fs.readFileSync(SAVE_FILE, "utf8");
    const data = JSON.parse(raw);
    gameState = (data && data.state) ? data.state : null;
    stateVersion = parseInt(data && data.version) || 0;
    stateUpdatedAt = parseInt(data && data.updatedAt) || 0;
    lastSavedAt = Date.now();
    if(gameState){
      log(`💾 Из файла: версия ${stateVersion}, ${(raw.length / 1048576).toFixed(2)} МБ, ` +
          `изменено ${new Date(stateUpdatedAt).toLocaleString()}`);
    } else {
      log("💾 В файле пусто (доска обнулена мастером)");
    }
    return !!gameState;
  }catch(e){
    log("⚠ Не удалось прочитать файл сохранения:", e.message);
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
    log(`💾 В файл: версия ${stateVersion}, ${(payload.length / 1048576).toFixed(2)} МБ`);
  }catch(e){
    log("⚠ Ошибка записи файла сохранения:", e.message);
  }
}

function scheduleSave(){
  pendingSave = true;
  if(saveTimer) return;
  saveTimer = setTimeout(()=>{ saveTimer = null; saveNow(); }, SAVE_DEBOUNCE_MS);
}

// ---------- Внешнее хранилище (Firebase Realtime Database) ----------
let fbRef = null;
let fbReady = false;
let fbError = null;
let cloudTimer = null;
let lastCloudWriteAt = 0;
let dirtyKeys = new Set();

function markDirty(keys){
  (keys || []).forEach(k=> dirtyKeys.add(k));
  scheduleCloudSave();
}

function scheduleCloudSave(){
  if(!fbReady) return;
  if(cloudTimer) return;
  cloudTimer = setTimeout(()=>{ cloudTimer = null; flushCloud(); }, CLOUD_DEBOUNCE_MS);
}

/**
 * Разобрать JSON сервисного аккаунта.
 * Частая беда: значение переменной окружения обрезается при вставке — не хватает
 * закрывающих скобок в конце. Тогда аккуратно дописываем их и пробуем снова.
 */
function missingClosers(text){
  let brace = 0, bracket = 0, inStr = false, esc = false;
  for(const ch of text){
    if(inStr){
      if(esc) esc = false;
      else if(ch === "\\") esc = true;
      else if(ch === '"') inStr = false;
      continue;
    }
    if(ch === '"'){ inStr = true; continue; }
    if(ch === "{") brace++;
    else if(ch === "}") brace--;
    else if(ch === "[") bracket++;
    else if(ch === "]") bracket--;
  }
  let out = inStr ? '"' : "";
  out += "]".repeat(Math.max(0, bracket));
  out += "}".repeat(Math.max(0, brace));
  return out;
}

function parseServiceAccount(raw){
  const text = String(raw || "").replace(/^\uFEFF/, "").trim();
  try{
    return JSON.parse(text);
  }catch(e1){
    const closers = missingClosers(text);
    if(closers){
      try{
        const obj = JSON.parse(text + closers);
        log(`☁ Ключ Firebase был обрезан — дописал «${closers}» автоматически`);
        return obj;
      }catch(e2){ /* ниже отдадим первую ошибку */ }
    }
    throw new Error("не разобрать JSON сервисного аккаунта (" + e1.message + ")");
  }
}

function initFirebase(){
  const inline = process.env.FIREBASE_SERVICE_ACCOUNT;
  const file = process.env.FIREBASE_SERVICE_ACCOUNT_FILE;
  if(!inline && !file){
    log("☁ Firebase не настроен — работаю только с файлом и клиентами");
    return false;
  }
  try{
    const admin = require("firebase-admin");
    const cred = inline ? parseServiceAccount(inline) : parseServiceAccount(fs.readFileSync(file, "utf8"));
    const region = process.env.FIREBASE_REGION || "europe-west1";
    const dbUrl = process.env.FIREBASE_DB_URL ||
      (region === "us-central1"
        ? `https://${cred.project_id}-default-rtdb.firebaseio.com`
        : `https://${cred.project_id}-default-rtdb.${region}.firebasedatabase.app`);
    if(admin.apps.length === 0){
      admin.initializeApp({ credential: admin.credential.cert(cred), databaseURL: dbUrl });
    }
    fbRef = admin.database().ref(process.env.FIREBASE_PATH || "games/vrata-sudby");
    fbReady = true;
    fbError = null;
    log(`☁ Firebase подключён: ${dbUrl} / ${fbRef.path.toString()}`);
    return true;
  }catch(e){
    fbError = String((e && e.message) || e);
    log("⚠ Firebase не подключился:", fbError);
    fbReady = false;
    fbRef = null;
    return false;
  }
}

/** Записать в облако только изменившиеся ключи (экономим трафик бесплатного тарифа). */
async function flushCloud(){
  if(!fbReady || !fbRef) return;
  if(!dirtyKeys.size) return;
  const since = Date.now() - lastCloudWriteAt;
  if(since < CLOUD_MIN_INTERVAL_MS){
    scheduleCloudSave();
    return;
  }
  const keys = Array.from(dirtyKeys);
  dirtyKeys.clear();
  try{
    if(!gameState){
      await fbRef.set(null);
      lastCloudWriteAt = Date.now();
      log("☁ В облаке очищено (новая сессия)");
      return;
    }
    const patch = { _meta: { version: stateVersion, updatedAt: stateUpdatedAt } };
    let bytes = 0;
    keys.forEach(k=>{
      const v = (k in gameState) ? gameState[k] : null;
      patch[k] = (v === undefined) ? null : v;
      try{ bytes += JSON.stringify(patch[k]).length; }catch(e){}
    });
    await fbRef.update(patch);
    lastCloudWriteAt = Date.now();
    log(`☁ В облако: ${keys.join(", ")} (${(bytes / 1048576).toFixed(2)} МБ)`);
  }catch(e){
    log("⚠ Ошибка записи в облако:", e.message);
    keys.forEach(k=> dirtyKeys.add(k));   // повторим позже
  }
}

/** Прочитать состояние из облака при старте. Возвращает true, если облако оказалось свежее файла. */
async function loadFromCloud(){
  if(!fbReady || !fbRef) return false;
  try{
    const snap = await fbRef.once("value");
    const val = snap.val();
    if(!val){
      if(gameState){
        log("☁ В облаке пусто, а в файле есть игра — заливаю её в облако");
        markDirty(Object.keys(gameState));
      }
      return false;
    }
    const meta = val._meta || {};
    delete val._meta;
    const cloudUpdatedAt = parseInt(meta.updatedAt) || 0;
    if(gameState && cloudUpdatedAt < stateUpdatedAt){
      log(`☁ В облаке старее (${new Date(cloudUpdatedAt).toLocaleString()}), чем в файле — оставляю файл`);
      markDirty(Object.keys(gameState));
      return false;
    }
    gameState = val;
    stateVersion = parseInt(meta.version) || 0;
    stateUpdatedAt = cloudUpdatedAt;
    log(`☁ Из облака: версия ${stateVersion}, изменено ${cloudUpdatedAt ? new Date(cloudUpdatedAt).toLocaleString() : "—"}`);
    return true;
  }catch(e){
    log("⚠ Не удалось прочитать облако:", e.message);
    return false;
  }
}

// ---------- Изменения состояния ----------
/** Отметить изменение состояния (версия ++, запись в файл и облако). */
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
  markDirty(Object.keys(gameState));
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

// Информация о хранилищах (без секретов)
app.get("/api/storage", (_, res)=>{
  res.json({
    file: path.basename(SAVE_FILE),
    fileBytes: fs.existsSync(SAVE_FILE) ? fs.statSync(SAVE_FILE).size : 0,
    cloud: fbReady ? (process.env.FIREBASE_PATH || "games/vrata-sudby") : null,
    cloudError: fbReady ? null : fbError,
    cloudConfigured: !!(process.env.FIREBASE_SERVICE_ACCOUNT || process.env.FIREBASE_SERVICE_ACCOUNT_FILE),
    version: stateVersion,
    updatedAt: stateUpdatedAt,
    hasState: !!gameState
  });
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
setInterval(()=>{ saveNow(); scheduleCloudSave(); }, SAVE_EVERY_MS);

wss.on("connection", async (ws)=>{
  ws.isAlive = true;
  ws.on("pong", heartbeat);
  try{ await bootPromise; }catch(e){}

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
          markDirty(Object.keys(m.patch));
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
          markDirty(["chat"]);
          broadcast(raw.toString(), ws);
        }
        return;

      case "presence":
        if(m.player && m.player.id){
          if(!gameState) gameState = {};
          if(!gameState.players) gameState.players = {};
          gameState.players[m.player.id] = Object.assign(gameState.players[m.player.id] || {}, m.player);
          touchState();
          markDirty(["players"]);
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
          markDirty(["characters"]);
          broadcast(raw.toString(), ws);
        }
        return;

      case "token-move":
        if(m.id && gameState && gameState.tokens && gameState.tokens[m.id]){
          gameState.tokens[m.id].x = m.x;
          gameState.tokens[m.id].y = m.y;
          touchState(true);   // позиции не меняют версию, но попадают в сохранение
          markDirty(["tokens"]);
        }
        broadcast(raw.toString(), ws);
        return;

      case "reset-state":
        // Мастер начал новую сессию — обнуляем и в памяти, и в файле, и в облаке
        gameState = null;
        stateVersion++;
        stateUpdatedAt = Date.now();
        scheduleSave();
        markDirty(["__reset"]);
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
if(require.main === module){
  server.listen(PORT, ()=>{
    log(`⚔  Врата Судьбы: слушаю порт ${PORT}, файл сохранения: ${SAVE_FILE}`);
  });
}

// Загрузка сохранений до первого клиента: сначала файл, потом облако (если оно свежее)
const bootPromise = (async ()=>{
  loadSave();
  initFirebase();
  if(fbReady) await loadFromCloud();
  return true;
})();

function shutdown(signal){
  log(`⏹ Получен ${signal} — сохраняю и выключаюсь`);
  try{ saveNow(true); }catch(e){}
  Promise.resolve(fbReady && fbRef ? flushCloud() : null).finally(()=>{
    try{ server.close(()=>process.exit(0)); }catch(e){ process.exit(0); }
    setTimeout(()=>process.exit(0), 3000);
  });
}
process.on("SIGTERM", ()=>shutdown("SIGTERM"));
process.on("SIGINT", ()=>shutdown("SIGINT"));

// Для автотестов
module.exports = {
  app, server, wss, bootPromise,
  __test: {
    loadSave, saveNow, scheduleSave, flushCloud, loadFromCloud, initFirebase, markDirty,
    parseServiceAccount, missingClosers,
    getDirtyKeys: ()=> Array.from(dirtyKeys),
    clearDirty: ()=> dirtyKeys.clear(),
    setCloudRef: (ref)=>{ fbRef = ref; fbReady = !!ref; },
    resetCloudThrottle: ()=>{ lastCloudWriteAt = 0; },
    isCloudReady: ()=> fbReady,
    getState: ()=> gameState,
    setState: (s)=>{ gameState = s; },
    getVersion: ()=> stateVersion,
    setVersion: (v)=>{ stateVersion = v; },
    setUpdatedAt: (v)=>{ stateUpdatedAt = v; },
    resetMemory: ()=>{ gameState = null; stateVersion = 0; stateUpdatedAt = 0; lastSavedAt = 0; },
    isClientStateNewer, adoptClientState, applyPatchToState
  }
};
