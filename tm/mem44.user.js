// ==UserScript==
// @name         MEM44 Auto-Reply AI Assistant
// @namespace    tamper-datingops
// @version      2.114
// @description  mem44 個別送信用のAIパネル（元のDatingOps Panelと同等機能）
// @author       coogee2033
// @match        https://mem44.com/*
// @downloadURL  https://raw.githubusercontent.com/coogee2033-blip/n8n-chat-ops-tm-public/main/tm/mem44.user.js
// @updateURL    https://raw.githubusercontent.com/coogee2033-blip/n8n-chat-ops-tm-public/main/tm/mem44.user.js
// @grant        GM_xmlhttpRequest
// @grant        GM_getValue
// @grant        GM_setValue
// @grant        GM_addStyle
// @grant        unsafeWindow
// @connect      localhost
// @connect      127.0.0.1
// @connect      raw.githubusercontent.com
// @connect      githubusercontent.com
// @connect      192.168.*.*
// @run-at       document-idle
// ==/UserScript==

// NOTE: このスクリプトは GitHub raw からインストール・更新される想定です。
// Tampermonkey 上で直接編集せず、このリポジトリのファイルを変更してからバージョンを上げてください。

/*
  === mem44 専用 Tampermonkey スクリプト ===
  元の DatingOps Panel（n8n-chat-ops/datingops.user.js）から分離。
  現在は `coogee2033-blip/n8n-chat-ops` リポジトリの `tm/` フォルダで管理されており、
  このファイルが正本です。
  OLV29 用バージョンは同じフォルダの `tm/olv29.user.js` が担当します。
*/

// v2.114 - n8n cross-tab lock (empty body fix)

(() => {
  "use strict";

  // iframe 内では動かさない
  if (window.top !== window.self) {
    console.debug("[DatingOps] skip: in iframe");
    return;
  }

  // 二重実行ガード（IIFE レベル）
  const g = typeof unsafeWindow !== "undefined" ? unsafeWindow : window;
  if (g.__datingOpsInitDone) {
    console.log("[DatingOps] already initialized, skip duplicate load");
    return;
  }
  g.__datingOpsInitDone = true;

  /** ===== 設定 ===== */
  const WEBHOOKS = [
    "http://localhost:5678/webhook/chat-v2",
    "http://127.0.0.1:5678/webhook/chat-v2",
  ];
  // メモ更新専用 Webhook （ホストは WEBHOOKS を流用、パスだけ差し替え）
  // 注意: n8n側は /memo-extract パスで FreeMemoExtractor v3 が動いている
  const MEMO_WEBHOOKS = WEBHOOKS.map((u) =>
    u.replace(/\/chat-v2$/, "/memo-extract")
  );
  const PANEL_ID = "datingops-ai-panel";
  const AUTO_SEND_ON_LOAD = false;  // open-check押下でのみ自動送信
  const AUTO_SEND_ON_NEW_MALE = false;
  const SHOW_QUEUE_STATUS = false; // C4: キュー表示を抑止（true にすると表示）
  const LOAD_DELAY_MS = 600;
  const RETRY_READ_CHAT = 3;
  const RETRY_READ_GAP = 250;
  const DUP_WINDOW_MS = 10_000;
  const REQUEST_TIMEOUT = 45_000; // webhook送信の最大待ち時間
  const diagState = {
    lastRequestAt: "-",
    lastResult: "-",
    errType: "",
    status: "",
    snippet: "",
    url: "",
  };

  // 自由メモ dirty フラグ（送信時自動保存用）
  let pairMemoDirty = false;
  let pairMemoInitialValue = null;
  let panelUserDragged = false;
  let inFlight = false;
  let inFlightAt = 0;

  /** ===== タブ間ロック機構（n8n 空ボディ対策） ===== */
  const N8N_LOCK_KEY = "mem44_n8n_send_lock";
  const N8N_LOCK_TTL_MS = 90000;    // ロックTTL: 90秒（クラッシュ時の自然回復用）
  const N8N_LOCK_WAIT_MS = 60000;   // 最大待ち時間: 60秒
  const N8N_LOCK_POLL_MS = 500;     // ポーリング間隔: 500ms
  const n8nLockId = (() => {
    try { return crypto.randomUUID(); }
    catch { return `tab_${Date.now()}_${Math.random().toString(36).slice(2)}`; }
  })();

  function getN8nLock() {
    try {
      const raw = localStorage.getItem(N8N_LOCK_KEY);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch { return null; }
  }

  function setN8nLock(owner, until) {
    try {
      localStorage.setItem(N8N_LOCK_KEY, JSON.stringify({ owner, until }));
      return true;
    } catch { return false; }
  }

  function clearN8nLock() {
    try { localStorage.removeItem(N8N_LOCK_KEY); } catch {}
  }

  async function acquireN8nLock() {
    const startWait = Date.now();
    while (Date.now() - startWait < N8N_LOCK_WAIT_MS) {
      const lock = getN8nLock();
      const now = Date.now();
      // ロックなし or 期限切れ or 自分のロック → 取得/更新OK
      if (!lock || lock.until < now || lock.owner === n8nLockId) {
        if (setN8nLock(n8nLockId, now + N8N_LOCK_TTL_MS)) {
          console.log("[MEM44] n8n lock acquired:", n8nLockId);
          return true;
        }
      }
      // 他タブがロック中 → 待機
      setStatus("他タブ送信中…", "#a78bfa");
      await new Promise(r => setTimeout(r, N8N_LOCK_POLL_MS));
    }
    console.warn("[MEM44] n8n lock timeout after", N8N_LOCK_WAIT_MS, "ms");
    return false;
  }

  function releaseN8nLock() {
    const lock = getN8nLock();
    if (lock && lock.owner === n8nLockId) {
      clearN8nLock();
      console.log("[MEM44] n8n lock released:", n8nLockId);
    }
  }

  /** ===== util ===== */
  const qs = (s, r = document) => r.querySelector(s);
  const qsa = (s, r = document) => [...r.querySelectorAll(s)];
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const nowMs = () => Date.now();
  const hash = (s) =>
    String(
      (s || "")
        .split("")
        .reduce((a, c) => ((a << 5) - a + c.charCodeAt(0)) | 0, 0)
    );

  const log = (...a) => console.debug("[DatingOps]", ...a);

  // ========== Global Queue System (Cross-Tab Coordination) ==========
  const QUEUE_KEY = "chatops.queue.v1";
  const LOCK_KEY = "chatops.queue.lock.v1";
  const PROGRESS_KEY = "chatops.queue.progress.v1";
  const LOCK_TTL_MS = 60_000;        // ロック TTL 60秒
  const HEARTBEAT_INTERVAL_MS = 15_000; // ハートビート 15秒
  const DISPATCH_INTERVAL_MS = 1_000;   // ディスパッチ間隔 1秒
  const MAX_RETRIES = 2;               // 最大リトライ回数
  const RETRY_DELAYS = [1000, 3000];   // 指数バックオフ
  const AUTO_FIRED_PREFIX = "autoFired.v2.86";
  const QUEUE_LIMIT = 5;
  const MAX_JOB_ATTEMPTS = 5;
  const BACKOFF_BASE_MS = 1000;
  const BACKOFF_MAX_MS = 60000;
  const OPEN_CHECK_TTL_MS = 10_000;

  // タブ固有ID（セッション単位）
  const TAB_ID = (() => {
    let id = sessionStorage.getItem("chatops.tabId");
    if (!id) {
      id = `tab_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      sessionStorage.setItem("chatops.tabId", id);
    }
    return id;
  })();

  // このタブの jobId（URL から安定キー生成）
  // URLパラメータを複数候補から取得するヘルパー
  // member_id1 / char_member_id1 など、サイトによって異なるパラメータ名に対応
  function getParamAny(params, keys) {
    for (const k of keys) {
      const v = params.get(k);
      if (v) return v;
    }
    return "";
  }

  // mid候補: mid, member_id, member_id1
  const MID_KEYS = ["mid", "member_id", "member_id1"];
  // cid候補: cid, char_id, char_member_id, char_member_id1
  const CID_KEYS = ["cid", "char_id", "char_member_id", "char_member_id1"];

  function getMyJobId() {
    // mem44 のURLは box_id / member_id / char_member_id / checknumber を使うことがある
    // member_id1 / char_member_id1 のケースもあるため、複数候補から取得
    const params = new URLSearchParams(location.search);
    const mid = getParamAny(params, MID_KEYS);
    const cid = getParamAny(params, CID_KEYS);
    const box = params.get("box_id") || "";
    const chk = params.get("checknumber") || "";

    // ★重要: checknumber を jobId に含める
    // 同一スレ (mid/cid/box) でも checknumber が変わると別ジョブとして扱えるため、
    // 1通目処理中に2通目タブが開いたときに "job already exists" で弾かれる問題を避ける。
    if (mid && cid) {
      const parts = ["mem44", mid, cid];
      if (box) parts.push(box);
      if (chk) parts.push(chk);
      return parts.join("_");
    }

    // フォールバック：pathname + search（checknumber が付いていれば自然に含まれる）
    return `mem44_${hash(location.pathname + location.search)}`;
  }

  function getAutoKey() {
    const params = new URLSearchParams(location.search);
    const mid = getParamAny(params, MID_KEYS);
    const cid = getParamAny(params, CID_KEYS);
    const box = params.get("box_id") || "";
    const chk = params.get("checknumber") || "";

    // できるだけURLの識別子を入れて、同一スレ誤爆や衝突を避ける
    return `mem44_${mid}_${cid}_${box}_${chk || location.pathname}`;
  }

  function getOpenCheckKey() {
    const params = new URLSearchParams(location.search);
    const mid = getParamAny(params, MID_KEYS);
    const cid = getParamAny(params, CID_KEYS);
    const chk = params.get("checknumber") || "";
    // mid も含めて衝突回避
    return `autoFired::openCheckWindow::${location.host}::${mid}::${cid}::${chk}`;
  }

  function getCheckWindowLoadKey() {
    const params = new URLSearchParams(location.search);
    const box = params.get("box_id") || "";
    const mid = getParamAny(params, MID_KEYS);
    const cid = getParamAny(params, CID_KEYS);
    const chk = params.get("checknumber") || "";
    return `autoFired::checkWindowLoad::${location.host}::${box}::${mid}::${cid}::${chk}`;
  }

  let openCheckListenerAdded = false;
  function isOpenCheckTrigger(btn) {
    if (!btn) return false;
    const txt = (
      btn.innerText ||
      btn.value ||
      btn.getAttribute("aria-label") ||
      btn.title ||
      ""
    ).toLowerCase();
    if (!txt) return false;
    const hasCheck = txt.includes("チェック会話");
    const hasWindow = txt.includes("別ウィンド") || txt.includes("別ウインド") || txt.includes("別ウィンドウ");
    const full = txt.includes("チェック会話を別ウ");
    return (hasCheck && hasWindow) || full;
  }

  function triggerOpenCheckEnqueue() {
    const key = getOpenCheckKey();
    const now = Date.now();

    // TTL抑止は「同一タブの連打」だけを対象にする。
    // openCheckクリック元タブと、開いた別ウィンドウ（TAB_IDが変わる）は抑止しない。
    try {
      const raw = localStorage.getItem(key);
      const prev = raw ? JSON.parse(raw) : null;
      const prevTs = Number(prev?.ts || 0);
      const prevTab = String(prev?.tabId || "");
      if (prevTs && (now - prevTs) < OPEN_CHECK_TTL_MS && prevTab === TAB_ID) {
        console.log("[AutoTrigger] suppressed by TTL (same tab)", { key, ageMs: now - prevTs, TAB_ID });
        return;
      }
    } catch (e) {
      // ignore parse errors
    }
    localStorage.setItem(key, JSON.stringify({ ts: now, tabId: TAB_ID }));
    const myJobId = getMyJobId();
    const enqueued = enqueueJob(myJobId, location.href);
    if (enqueued) {
      console.log("[AutoTrigger] enqueue requested", { jobId: myJobId });
      setDiagStatus("auto: start", "#c084fc");
      updateProgressFromQueue();
      checkAndProcessMyJob();
    } else {
      console.log("[AutoTrigger] enqueue skipped", { reason: "exists or blocked", jobId: myJobId });
    }
  }

  function initOpenCheckClickListener() {
    if (openCheckListenerAdded) return;
    openCheckListenerAdded = true;
    document.addEventListener("click", (e) => {
      const btn = e.target.closest("button,a,input,[role=\"button\"]");
      if (!btn) return;
      if (!isOpenCheckTrigger(btn)) return;
      console.log("[AutoTrigger] openCheckWindow click detected", {
        url: location.href,
      });
      setTimeout(triggerOpenCheckEnqueue, 100);
    }, true);
  }

  function checkWindowLoadAutoTrigger() {
    const params = new URLSearchParams(location.search);
    const chk = params.get("checknumber") || "";
    if (!chk) return;
    if (!window.opener) return;
    const key = getCheckWindowLoadKey();
    const now = Date.now();

    // TTL抑止は「同一ウィンドウ（同一TAB_ID）のリロード連打」だけを対象にする。
    // 別ウィンドウで開いたチェック会話はTAB_IDが異なるため抑止しない。
    try {
      const raw = localStorage.getItem(key);
      const prev = raw ? JSON.parse(raw) : null;
      const prevTs = Number(prev?.ts || 0);
      const prevTab = String(prev?.tabId || "");
      if (prevTs && (now - prevTs) < OPEN_CHECK_TTL_MS && prevTab === TAB_ID) {
        console.log("[AutoTrigger] suppressed by TTL (same tab)", { key, ageMs: now - prevTs, TAB_ID });
        return;
      }
    } catch (e) {
      // ignore parse errors
    }
    localStorage.setItem(key, JSON.stringify({ ts: now, tabId: TAB_ID }));
    console.log("[AutoTrigger] checkWindow load detected", {
      url: location.href,
      params: {
        box_id: params.get("box_id") || "",
        mid: params.get("mid") || params.get("member_id") || "",
        cid: params.get("cid") || params.get("char_id") || params.get("char_member_id") || "",
        checknumber: chk,
      },
    });
    const myJobId = getMyJobId();
    const enqueued = enqueueJob(myJobId, location.href);
    if (enqueued) {
      console.log("[AutoTrigger] enqueue requested", { jobId: myJobId });
      setDiagStatus("auto: start", "#c084fc");
      updateProgressFromQueue();
      checkAndProcessMyJob();
    } else {
      console.log("[AutoTrigger] enqueue skipped", { reason: "exists or blocked", jobId: myJobId });
    }
  }

  // キュー読み取り
  function getQueue() {
    try {
      const raw = localStorage.getItem(QUEUE_KEY);
      if (!raw) return { version: 1, items: [], createdAt: Date.now() };
      const q = JSON.parse(raw);
      if (q.version !== 1) return { version: 1, items: [], createdAt: Date.now() };
      return q;
    } catch (e) {
      console.warn("[Queue] getQueue error:", e);
      return { version: 1, items: [], createdAt: Date.now() };
    }
  }

  // キュー書き込み
  function setQueue(queue) {
    try {
      localStorage.setItem(QUEUE_KEY, JSON.stringify(queue));
    } catch (e) {
      console.warn("[Queue] setQueue error:", e);
    }
  }

  // 異常に膨らんだキューを自動回復（旧バージョンの残骸対策）
  function pruneQueueIfTooLarge() {
    try {
      const q = getQueue();
      const len = (q.items || []).length;
      // 通常運用では 0〜数件のはず。50超は壊れた状態とみなして初期化。
      if (len > 50) {
        console.warn("[Queue] queue too large, resetting", { len });
        setQueue({ version: 1, items: [], createdAt: Date.now() });
        // progress/lock も一緒に初期化して復旧させる
        try { localStorage.removeItem(LOCK_KEY); } catch {}
        try { localStorage.removeItem(PROGRESS_KEY); } catch {}
        setProgress({ currentJobId: null, total: 0, done: 0, running: 0, failed: 0, remaining: 0 });
        updateQueueUI();
      }
    } catch (e) {
      console.warn("[Queue] pruneQueueIfTooLarge failed", e);
    }
  }

  // ジョブ登録（二重登録防止）
  function enqueueJob(jobId, url) {
    const queue = getQueue();
    if (queue.items.length >= QUEUE_LIMIT) {
      console.log("[ChatOps] enqueue blocked: queue too large", { queueLen: queue.items.length });
      return false;
    }
    const exists = queue.items.find(it => it.jobId === jobId);
    if (exists) {
      console.log("[ChatOps] job already exists:", jobId, exists.status);
      return false;
    }
    queue.items.push({
      jobId,
      url,
      status: "pending",
      tries: 0,
      attempt: 0,
      nextAt: Date.now(),
      lastError: null,
      updatedAt: Date.now(),
    });
    setQueue(queue);
    console.log("[ChatOps] enqueued:", jobId);
    updateProgressFromQueue();
    return true;
  }

  // ジョブ状態更新
  // FIX B: tries更新はincrementJobTries()に統一し、ここでは触らない
  function updateJobStatus(jobId, status, errorMsg = null) {
    const queue = getQueue();
    const item = queue.items.find(it => it.jobId === jobId);
    if (!item) return false;
    item.status = status;
    item.updatedAt = Date.now();
    if (errorMsg) item.lastError = errorMsg;
    // FIX B: 元のコードは常に +0 だったバグ。tries更新はincrementJobTries()で行う
    setQueue(queue);
    updateProgressFromQueue();
    return true;
  }

  // ジョブの tries を増やす
  function incrementJobTries(jobId) {
    const queue = getQueue();
    const item = queue.items.find(it => it.jobId === jobId);
    if (!item) return 0;
    item.tries = (item.tries || 0) + 1;
    item.updatedAt = Date.now();
    setQueue(queue);
    return item.tries;
  }

  // プログレス読み取り
  function getProgress() {
    try {
      const raw = localStorage.getItem(PROGRESS_KEY);
      if (!raw) return { currentJobId: null, total: 0, done: 0, running: 0, failed: 0 };
      return JSON.parse(raw);
    } catch (e) {
      return { currentJobId: null, total: 0, done: 0, running: 0, failed: 0 };
    }
  }

  // プログレス書き込み
  function setProgress(progress) {
    try {
      localStorage.setItem(PROGRESS_KEY, JSON.stringify(progress));
    } catch (e) {
      console.warn("[Queue] setProgress error:", e);
    }
  }

  // キューからプログレスを計算して更新
  function updateProgressFromQueue() {
    const queue = getQueue();
    const items = queue.items || [];
    const progress = getProgress();
    progress.total = items.length;
    progress.done = items.filter(it => it.status === "done").length;
    progress.running = items.filter(it => it.status === "running").length;
    progress.failed = items.filter(it => it.status === "failed").length;
    progress.remaining = progress.total - progress.done - progress.failed;
    setProgress(progress);
    updateQueueUI();
    clearQueueIfAllDone();
  }

  // ロック取得
  function tryAcquireLock() {
    try {
      const raw = localStorage.getItem(LOCK_KEY);
      const now = Date.now();
      if (raw) {
        const lock = JSON.parse(raw);
        // 自分が既に持っている
        if (lock.ownerId === TAB_ID) {
          lock.acquiredAt = now; // 延長
          localStorage.setItem(LOCK_KEY, JSON.stringify(lock));
          return true;
        }
        // 他タブが持っていてTTL内
        if (now - lock.acquiredAt < LOCK_TTL_MS) {
          return false;
        }
        // TTL超過 → 奪取
        console.log("[Queue] lock expired, taking over from:", lock.ownerId);
      }
      // ロック取得
      localStorage.setItem(LOCK_KEY, JSON.stringify({ ownerId: TAB_ID, acquiredAt: now }));
      console.log("[Queue] lock acquired by:", TAB_ID);
      return true;
    } catch (e) {
      console.warn("[Queue] tryAcquireLock error:", e);
      return false;
    }
  }

  // ロック解放
  function releaseLock() {
    try {
      const raw = localStorage.getItem(LOCK_KEY);
      if (raw) {
        const lock = JSON.parse(raw);
        if (lock.ownerId === TAB_ID) {
          localStorage.removeItem(LOCK_KEY);
          console.log("[Queue] lock released by:", TAB_ID);
        }
      }
    } catch (e) {
      console.warn("[Queue] releaseLock error:", e);
    }
  }

  // 自分がロックオーナーか
  function isLockOwner() {
    try {
      const raw = localStorage.getItem(LOCK_KEY);
      if (!raw) return false;
      const lock = JSON.parse(raw);
      return lock.ownerId === TAB_ID && (Date.now() - lock.acquiredAt < LOCK_TTL_MS);
    } catch (e) {
      return false;
    }
  }

  // ディスパッチャ: 次の pending ジョブを currentJobId にセット
  let dispatcherTimer = null;
  function startDispatcher() {
    if (dispatcherTimer) return;
    console.log("[Queue] dispatcher started");

    const dispatch = () => {
      if (!tryAcquireLock()) {
        // ロック取れない → 他タブがディスパッチャ
        return;
      }
      const progress = getProgress();
      // currentJobId が設定済みでまだ running なら待つ
      if (progress.currentJobId) {
        const queue = getQueue();
        const current = queue.items.find(it => it.jobId === progress.currentJobId);
        if (current && current.status === "running") {
          return; // 処理中
        }
        // done/failed なら次へ
        progress.currentJobId = null;
      }
      // 次の pending を探す
      const queue = getQueue();
      const now = Date.now();
      const next = queue.items.find(it => it.status === "pending" && (!it.nextAt || it.nextAt <= now));
      if (next) {
        progress.currentJobId = next.jobId;
        setProgress(progress);
        console.log("[Queue] dispatching job:", next.jobId);

        // NOTE: 同一タブ内で setProgress しても storage イベントは発火しないため
        // dispatcher 自身がワーカー処理を起動する
        setTimeout(() => {
          try {
            checkAndProcessMyJob();
          } catch (e) {
            console.warn("[Queue] checkAndProcessMyJob failed after dispatch", e);
          }
        }, 0);
      } else {
        // 全部終わり
        if (progress.currentJobId) {
          progress.currentJobId = null;
          setProgress(progress);
        }
      }
    };

    dispatch();
    dispatcherTimer = setInterval(dispatch, DISPATCH_INTERVAL_MS);
  }

  // ワーカー: currentJobId が自分の jobId なら処理
  let workerActive = false;
  async function checkAndProcessMyJob() {
    if (workerActive) return;
    const myJobId = getMyJobId();
    const progress = getProgress();

    if (progress.currentJobId !== myJobId) {
      return; // 自分の番じゃない
    }

    const queue = getQueue();
    const job = queue.items.find(it => it.jobId === myJobId);
    if (!job || job.status !== "pending") {
      return; // ジョブがないか既に処理済み
    }

    workerActive = true;
    console.log("[Queue] processing my job:", myJobId, "workerActive=", workerActive);
    updateJobStatus(myJobId, "running");
    setStatus("処理中…", "#ffa94d");

    let success = false;
    let lastError = null;
    try {
      for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
        try {
          if (attempt > 0) {
            const delay = RETRY_DELAYS[attempt - 1] || 3000;
            console.log(`[Queue] retry ${attempt}/${MAX_RETRIES} after ${delay}ms`);
            setStatus(`リトライ ${attempt}/${MAX_RETRIES}…`, "#f59e0b");
            await sleep(delay);
          }

          const payload = await buildWebhookPayload(false);  // 初回生成（temperature省略）
          setDiagStatus("queue: sending", "#38bdf8");
          const res = await sendToN8n(payload, "queue_auto");

          if (!res) {
            throw new Error("Empty response");
          }

          const reply = res?.reply_formatted || res?.reply || res?.text || res?.message || res?.choices?.[0]?.message?.content || "";
          const memoBlock = res?.memo_block ?? res?.memo ?? res?.memo_candidate ?? "";
          if (res?.reply_formatted) console.debug("[Queue] using reply_formatted");

          if (reply && reply.trim().length >= 12) {
            const ok = insertReply(reply);
            updateMemoCandidateBox(memoBlock);
            setStatus(ok ? "挿入OK" : "挿入NG", ok ? "#4ade80" : "#f87171");
            success = true;
            break;
          } else {
            throw new Error("Reply too short or empty");
          }
        } catch (e) {
          lastError = e?.message || String(e);
          console.warn(`[Queue] attempt ${attempt + 1} failed:`, lastError);
          incrementJobTries(myJobId);
        }
      }

      if (success) {
        const q = getQueue();
        q.items = q.items.filter(it => it.jobId !== myJobId);
        setQueue(q);
        updateProgressFromQueue();
        console.log("[Queue] job done:", myJobId);
      } else {
        const q = getQueue();
        const jobRef = q.items.find(it => it.jobId === myJobId);
        if (jobRef) {
          jobRef.status = "pending";
          jobRef.attempt = (jobRef.attempt || 0) + 1;
          jobRef.lastError = lastError;
          if (jobRef.attempt >= MAX_JOB_ATTEMPTS) {
            q.items = q.items.filter(it => it.jobId !== myJobId);
            console.log("[Queue] job dropped after max attempts:", myJobId, lastError);
          } else {
            const delay = Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * Math.pow(2, jobRef.attempt));
            jobRef.nextAt = Date.now() + delay;
            console.log("[Queue] job backoff:", { jobId: myJobId, attempt: jobRef.attempt, nextAt: jobRef.nextAt, err: lastError });
          }
          setQueue(q);
          updateProgressFromQueue();
        }
        setStatus("処理失敗", "#f87171");
        console.error("[Queue] job failed:", myJobId, lastError);
      }
    } finally {
      const prog = getProgress();
      prog.currentJobId = null;
      setProgress(prog);
      updateProgressFromQueue();
      workerActive = false;
    }
  }

  // storage イベントで他タブの変更を監視
  function setupStorageListener() {
    window.addEventListener("storage", (e) => {
      if (e.key === PROGRESS_KEY || e.key === QUEUE_KEY) {
        updateQueueUI();
        // currentJobId が自分なら処理開始
        checkAndProcessMyJob();
      }
    });
  }

  // UI 更新: パネルに進捗表示
  function updateQueueUI() {
    const progress = getProgress();
    const statusEl = qs("#n8n_queue_status");
    if (!statusEl) return;

    const { total, done, running, failed, remaining } = progress;

    // C4: SHOW_QUEUE_STATUS が false ならキュー表示を常に非表示
    if (!SHOW_QUEUE_STATUS || total === 0) {
      statusEl.textContent = "";
      statusEl.style.display = "none";
      return;
    }

    statusEl.style.display = "block";
    let text = `Queue: ${done}/${total}`;
    if (running > 0) text += ` (処理中)`;
    if (failed > 0) text += ` 失敗:${failed}`;
    if (remaining > 0) text += ` 残:${remaining}`;

    statusEl.textContent = text;

    // 色
    if (running > 0) {
      statusEl.style.color = "#ffa94d";
    } else if (failed > 0 && remaining === 0) {
      statusEl.style.color = "#f87171";
    } else if (done === total) {
      statusEl.style.color = "#4ade80";
    } else {
      statusEl.style.color = "#9aa";
    }
  }

  // キューをクリア（全完了後）
  function clearQueueIfAllDone() {
    const queue = getQueue();
    const pending = queue.items.filter(it => it.status === "pending" || it.status === "running");
    if (pending.length === 0 && queue.items.length > 0) {
      // 1分後にクリア
      setTimeout(() => {
        const q2 = getQueue();
        const stillPending = q2.items.filter(it => it.status === "pending" || it.status === "running");
        if (stillPending.length === 0) {
          setQueue({ version: 1, items: [], createdAt: Date.now() });
          setProgress({ currentJobId: null, total: 0, done: 0, running: 0, failed: 0, remaining: 0 });
          updateQueueUI();
          console.log("[Queue] cleared completed queue");
        }
      }, 60_000);
    }
  }

  // 手動送信がキュー処理中は抑止されるかチェック
  function isQueueBusy() {
    const progress = getProgress();
    return progress.currentJobId != null && progress.running > 0;
  }

  // ========== Timestamp Debug ==========
  const DEBUG_TS = true; // デバッグログを出す（うるさければ false に）
  let _tsDbgCount = 0;
  const TS_DBG_LIMIT = 20; // 最初の N 件だけログ

  // 日時文字列をなるべく ISO っぽく整形する（失敗したら null）
  function normalizeTimestamp(raw) {
    if (!raw) return null;
    let t = String(raw).trim();
    if (!t) return null;

    // 前処理: 全角スペース→半角、改行/タブ→空白、連続空白→1つ
    t = t.replace(/\u3000/g, " ");
    t = t.replace(/[\r\n\t]+/g, " ");
    t = t.replace(/\s+/g, " ").trim();

    // "12/1202:10" のように空白なしで結合されている場合に備えて間に空白を挿入
    // MM/DD の直後に HH:MM が続く場合
    t = t.replace(/(\d{1,2}\/\d{1,2})(\d{1,2}:\d{2})/, "$1 $2");

    const now = new Date();
    const currentYear = now.getFullYear();

    let y, mo, d, h = 0, mi = 0, s = 0;
    let matched = false;

    // 1) 年を含む完全な日付: 2025/12/11 09:21, 2025-12-11 09:21:37, 2025.12.11 09:21
    let m = t.match(/(20\d{2})[\/.\-](\d{1,2})[\/.\-](\d{1,2})(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?/);
    if (m) {
      y = parseInt(m[1], 10);
      mo = parseInt(m[2], 10) - 1;
      d = parseInt(m[3], 10);
      if (m[4] != null) h = parseInt(m[4], 10);
      if (m[5] != null) mi = parseInt(m[5], 10);
      if (m[6] != null) s = parseInt(m[6], 10);
      matched = true;
    }

    if (!matched) {
      // 2) 年なし「12/12 02:10」形式（MM/DD HH:MM）- 空白0個以上でもOK
      m = t.match(/(\d{1,2})[\/.\-](\d{1,2})\s*(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?/);
      if (m) {
        y = currentYear;
        mo = parseInt(m[1], 10) - 1;
        d = parseInt(m[2], 10);
        h = parseInt(m[3], 10);
        mi = parseInt(m[4], 10);
        if (m[5] != null) s = parseInt(m[5], 10);
        matched = true;
      }
    }

    if (!matched) {
      // 3) 保険: 時刻だけ "02:10" 形式のとき → 今日の日付で補完
      m = t.match(/^(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$/);
      if (m) {
        y = currentYear;
        mo = now.getMonth();
        d = now.getDate();
        h = parseInt(m[1], 10);
        mi = parseInt(m[2], 10);
        if (m[3] != null) s = parseInt(m[3], 10);
        matched = true;
      }
    }

    if (!matched) {
      return null;
    }

    const date = new Date(y, mo, d, h, mi, s);
    if (Number.isNaN(date.getTime())) {
      return null;
    }

    return {
      timestamp: date.toISOString(),
      timestampMs: date.getTime(),
    };
  }

  // メッセージノード付近から日時文字列を抽出（デバッグログ付き）
  function extractTimestampFromNode(el, idx, role, text) {
    try {
      if (!el) return null;

      const elTag = el.tagName || "";
      const elClass = el.className || "";
      const elText80 = (el.textContent || "").replace(/\s+/g, " ").trim().slice(0, 80);

      // A. 吹き出しコンテナを特定
      let bubble = el.closest(".mb_M, .mb_B");
      if (!bubble) {
        const member = el.closest(".mmsg_member");
        if (member) {
          bubble = member.closest(".mb_M, .mb_B") || member.parentElement;
        }
      }

      const containerTag = bubble ? bubble.tagName : "";
      const containerClass = bubble ? bubble.className : "";

      // B. コンテナ内の .mmsgdt からテキストを集める
      const texts = [];
      const seen = new Set();

      const collectFromNode = (node) => {
        if (!node) return;
        const dts = node.querySelectorAll(".mmsgdt");
        dts.forEach((dt) => {
          let t = dt.textContent || "";
          t = t.replace(/\s+/g, " ").trim();
          // 空文字や空白だけは除外
          if (t && t.length > 0 && !/^[\s\u00A0]*$/.test(t) && !seen.has(t)) {
            seen.add(t);
            texts.push(t);
          }
        });
      };

      if (bubble) {
        collectFromNode(bubble);
      }

      // C. bubble で取れなければ row を探索
      if (texts.length === 0) {
        const row = el.closest("tr");
        if (row) collectFromNode(row);
      }

      // D. さらに取れなければ直前の兄弟を探索
      if (texts.length === 0 && bubble) {
        let prev = bubble.previousElementSibling;
        for (let i = 0; i < 3 && prev && texts.length === 0; i++) {
          if (prev.classList && prev.classList.contains("mmsgdt")) {
            const t = (prev.textContent || "").replace(/\s+/g, " ").trim();
            if (t && !seen.has(t)) {
              seen.add(t);
              texts.push(t);
            }
          }
          collectFromNode(prev);
          prev = prev.previousElementSibling;
        }
      }

      // E. texts を結合して raw を作成
      const raw = texts.join(" ");
      const norm = raw ? normalizeTimestamp(raw) : null;

      // F. デバッグログ（最初の N 件だけ）
      if (DEBUG_TS && _tsDbgCount < TS_DBG_LIMIT) {
        _tsDbgCount++;
        console.debug("[TSDBG]", {
          idx,
          role,
          elTag,
          elClass: elClass.slice(0, 50),
          elText80,
          containerTag,
          containerClass: containerClass.slice(0, 50),
          dtCount: texts.length,
          dtTexts: texts,
          raw,
          norm,
        });
      }

      if (norm && norm.timestamp) {
        return norm;
      }

      // G. フォールバック: text の先頭から日時を拾う試み
      if (text) {
        const head = String(text).trim().slice(0, 20);
        const headNorm = normalizeTimestamp(head);
        if (headNorm && headNorm.timestamp) {
          if (DEBUG_TS && _tsDbgCount < TS_DBG_LIMIT) {
            console.debug("[TSDBG] fallback from text head:", { head, headNorm });
          }
          return headNorm;
        }
      }

      return null;
    } catch (e) {
      return null;
    }
  }

  /** ===== 個別送信ページ判定（URL + 返信欄の存在） ===== */
  function isPersonalSendPage() {
    const urlOk = /\/staff\/personalbox/i.test(location.pathname);
    if (!urlOk) return false;
    const sendBtn = qsa('input[type="submit"],button').find((b) =>
      /送信して閉じる/.test(b.value || b.textContent || "")
    );
    const anyTextarea = qsa("textarea").length > 0;
    return !!(sendBtn && anyTextarea);
  }

  /** ===== パネル（ドラッグ可） ===== */
  function forceAiPanelLayout(panel) {
    const el = panel || qs("#" + PANEL_ID) || qs(".datingops-ai-panel");
    if (!el) return;
    if (panelUserDragged) return;

    const isPersonalBox = location.pathname.includes("/staff/personalbox");
    el.classList.add("datingops-ai-panel");
    el.style.setProperty("position", "fixed", "important");
    el.style.setProperty("box-sizing", "border-box", "important");
    el.style.setProperty("z-index", "999999", "important");

    if (isPersonalBox) {
      el.style.setProperty("left", "8px", "important");
      el.style.removeProperty("right");
      el.style.setProperty("bottom", "16px", "important");
      el.style.setProperty("width", "280px", "important");
      el.style.setProperty("min-width", "280px", "important");
      el.style.setProperty("max-width", "280px", "important");
    } else {
      el.style.setProperty("right", "16px", "important");
      el.style.removeProperty("left");
      el.style.setProperty("bottom", "16px", "important");
      el.style.setProperty("width", "320px", "important");
      el.style.setProperty("min-width", "320px", "important");
      el.style.setProperty("max-width", "320px", "important");
    }
  }

  function ensurePanel() {
    if (qs("#" + PANEL_ID)) return;
    const wrap = document.createElement("div");
    wrap.id = PANEL_ID;
    wrap.classList.add("datingops-ai-panel");
    wrap.style.cssText = `
      position:fixed; z-index:999999;
      background:#111; color:#eee; border-radius:12px;
      box-shadow:0 12px 30px rgba(0,0,0,.35); font-family:system-ui,-apple-system,Segoe UI,sans-serif;
      max-height:70vh; overflow-y:auto; box-sizing:border-box; padding-bottom:8px;
    `;
    wrap.innerHTML = `
      <div id="n8n_drag_handle" style="cursor:move;padding:10px 12px; display:flex; align-items:center; gap:8px; border-bottom:1px solid #333;">
        <div style="font-weight:700;">MEM44 自動返信</div>
        <button id="n8n_close_btn" style="margin-left:auto; background:transparent; border:none; color:#888; font-size:14px; cursor:pointer; padding:0 4px;">✕</button>
        <div id="n8n_status" style="font-size:12px; color:#9aa; margin-left:4px;">起動</div>
        <div id="mem44_diag_summary" style="font-size:11px; color:#9aa; margin-left:8px;">Diag: -</div>
      </div>
      <div id="n8n_queue_status" style="display:none; padding:4px 12px; font-size:11px; color:#9aa; background:#1a1a1a; border-bottom:1px solid #333;"></div>
      <div style="padding:10px 12px; display:flex; flex-direction:column; gap:8px;">
        <label style="font-size:12px;color:#aaa;">一言プロンプト（任意）</label>
        <textarea id="n8n_prompt" rows="3" placeholder="例）もう少し丁寧に" style="width:100%;resize:vertical;border-radius:8px;border:1px solid #444;background:#1b1b1b;color:#eee;padding:8px;"></textarea>
        <div style="display:flex;align-items:center;gap:8px;">
          <span style="font-size:12px;color:#aaa;">温度</span>
          <input id="n8n_temp" type="range" min="0" max="2.0" step="0.1" value="1.0" style="flex:1;">
          <span id="n8n_temp_val" style="width:40px;text-align:right;font-size:12px;color:#ccc;">1.0</span>
        </div>
        <div style="display:flex;gap:8px;">
          <button id="n8n_send" style="flex:1;background:#16a34a;border:none;color:#fff;border-radius:8px;padding:10px 12px;font-weight:700;cursor:pointer;">再生成</button>
          <button id="n8n_copy" style="width:84px;background:#333;border:1px solid #444;color:#eee;border-radius:8px;cursor:pointer;">コピー</button>
        </div>
        <div style="margin-top:4px; padding-top:6px; border-top:1px solid #333; display:flex; flex-direction:column; gap:4px;">
          <label style="font-size:11px;color:#aaa;">メモ候補（男性の事実メモ）</label>
          <textarea id="n8n_memo_candidate" class="datingops-memo-candidate" rows="3" readonly style="width:100%;resize:vertical;border-radius:8px;border:1px solid #444;background:#181818;color:#9ef;padding:6px;font-size:11px;"></textarea>
          <div style="display:flex;justify-content:flex-end;gap:6px;">
            <button id="n8n_copy_memo" style="background:#334155;border:1px solid #475569;color:#e5e7eb;border-radius:8px;padding:4px 8px;font-size:11px;cursor:pointer;">メモにコピー</button>
            <button id="n8n_memo_update" style="background:#7c3aed;border:1px solid #a855f7;color:#f9fafb;border-radius:8px;padding:4px 8px;font-size:11px;cursor:pointer;">メモ更新</button>
          </div>
        </div>
        <div id="mem44_diag_root"></div>
      </div>`;
    document.body.appendChild(wrap);
    forceAiPanelLayout(wrap);
    ensureDiagElements();

    let layoutTimer = null;
    const scheduleLayout = () => {
      if (panelUserDragged) return;
      if (layoutTimer) clearTimeout(layoutTimer);
      layoutTimer = setTimeout(() => forceAiPanelLayout(wrap), 120);
    };
    window.addEventListener("resize", scheduleLayout);
    window.addEventListener("scroll", scheduleLayout);

    // Drag
    (function dragify(box, handle) {
      let sx = 0,
        sy = 0,
        ox = 0,
        oy = 0,
        dragging = false;
      const onDown = (e) => {
        dragging = true;
        panelUserDragged = true;
        sx = e.clientX;
        sy = e.clientY;
        const r = box.getBoundingClientRect();
        ox = r.left;
        oy = r.top;
        document.addEventListener("mousemove", onMove);
        document.addEventListener("mouseup", onUp);
        e.preventDefault();
      };
      const onMove = (e) => {
        if (!dragging) return;
        const dx = e.clientX - sx,
          dy = e.clientY - sy;
        box.style.left = ox + dx + "px";
        box.style.top = oy + dy + "px";
        box.style.right = "auto";
        box.style.bottom = "auto";
      };
      const onUp = () => {
        dragging = false;
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
      };
      box.style.position = "fixed";
      handle.addEventListener("mousedown", onDown);
    })(wrap, qs("#n8n_drag_handle", wrap));

    // ✕ ボタンでパネルを閉じる
    const closeBtn = qs("#n8n_close_btn", wrap);
    if (closeBtn) {
      closeBtn.addEventListener("click", () => {
        wrap.style.display = "none";
      });
    }

    qs("#n8n_temp", wrap).addEventListener("input", () => {
      qs("#n8n_temp_val", wrap).textContent = (+qs("#n8n_temp", wrap)
        .value).toFixed(1);
    });
    qs("#n8n_copy", wrap).addEventListener("click", async () => {
      const convo20 = await getConversation20();
      const clip = `PROMPT:\n${(
        qs("#n8n_prompt")?.value || ""
      ).trim()}\n\nCONVO20:\n${convo20}\n\nPROFILE:\n${getSideInfoText()}`;
      await navigator.clipboard.writeText(clip);
      setStatus("コピーOK", "#4ade80");
    });
    qs("#n8n_send", wrap).addEventListener("click", sendManual);

    // メモコピーボタン
    qs("#n8n_copy_memo", wrap)?.addEventListener("click", async () => {
      const ta = qs("#n8n_memo_candidate", wrap);
      const text = (ta?.value || "").trim();
      if (!text) {
        setStatus("メモ候補なし", "#f59e0b");
        return;
      }
      try {
        await navigator.clipboard.writeText(text);
      } catch (e) {
        console.warn("[DatingOps] clipboard error:", e);
      }
      const memoTa = getPairMemoTextarea();
      if (memoTa) {
        const cur = memoTa.value.trim();
        memoTa.value = cur ? cur + "\n" + text : text;
        ["input", "change", "keyup"].forEach((ev) =>
          memoTa.dispatchEvent(new Event(ev, { bubbles: true }))
        );
        savePairMemo("copy-memo");
        setStatus("メモ追記OK", "#4ade80");
      } else {
        setStatus("メモ欄見つからず", "#f59e0b");
      }
    });

    // メモ更新ボタン
    qs("#n8n_memo_update", wrap)?.addEventListener("click", () => {
      sendMemoUpdate();
    });
    createDiagBlock();
    renderDiag();
  }

  function setStatus(msg, color = "#9aa") {
    const s = qs("#n8n_status");
    if (s) {
      s.textContent = msg;
      s.style.color = color;
    }
  }

  function setDiagStatus(text, color = "#ccc") {
    ensureDiagElements();
    const r = qs("#mem44_diag_result");
    const summary = qs("#mem44_diag_summary");
    if (r) {
      r.textContent = "result: " + text;
      r.style.color = color;
    }
    if (summary) {
      summary.textContent = "Diag: " + text;
      summary.style.color = color;
    }
  }

  function renderDiag(res, urlUsed) {
    if (!ensureDiagElements()) {
      console.log("[DiagFallback] diag elements missing");
    }
    const now = new Date();
    diagState.lastRequestAt = now.toLocaleTimeString();
    if (res && res.ok) {
      diagState.lastResult = "OK";
      diagState.errType = "";
      diagState.status = String(res.status || "");
      diagState.snippet = "";
    } else {
      diagState.lastResult = "NG";
      diagState.errType = res?.errType || "unknown";
      diagState.status = String(res?.status ?? "");
      diagState.snippet = (res?.snippet || res?.text || "").slice(0, 200);
    }
    diagState.url = urlUsed || diagState.url || "";

    const lastEl = qs("#mem44_diag_last");
    const resultEl = qs("#mem44_diag_result");
    const detailEl = qs("#mem44_diag_detail");
    const summary = qs("#mem44_diag_summary");
    if (lastEl) lastEl.textContent = `last: ${diagState.lastRequestAt}`;
    if (resultEl) {
      const ok = diagState.lastResult === "OK";
      resultEl.textContent = `result: ${diagState.lastResult} ${diagState.status ? `(status ${diagState.status})` : ""}`;
      resultEl.style.color = ok ? "#4ade80" : "#f87171";
    }
    if (summary) {
      const ok = diagState.lastResult === "OK";
      summary.textContent = `Diag: ${diagState.lastResult} ${diagState.status ? `(status ${diagState.status})` : ""}`;
      summary.style.color = ok ? "#4ade80" : "#f87171";
    }
    if (detailEl) {
      if (diagState.lastResult === "OK") {
        detailEl.textContent = diagState.url ? diagState.url : "";
        detailEl.style.color = "#9cd8a5";
      } else {
        detailEl.textContent = `${diagState.errType || ""} ${diagState.url || ""}\n${diagState.snippet || ""}`;
        detailEl.style.color = "#fca5a5";
      }
    }
  }

  function ensureDiagElements() {
    const wrap = qs("#" + PANEL_ID);
    if (!wrap) return false;
    let diagRoot = qs("#mem44_diag_root", wrap);
    if (!diagRoot) {
      diagRoot = document.createElement("div");
      diagRoot.id = "mem44_diag_root";
    }
    // move to tail to ensure visibility
    if (wrap && diagRoot) wrap.appendChild(diagRoot);
    diagRoot.style.display = "block";
    diagRoot.style.marginTop = "8px";
    diagRoot.style.borderTop = "1px solid rgba(255,255,255,0.2)";
    diagRoot.style.position = "relative";
    diagRoot.style.zIndex = "99999";
    if (!qs("#mem44_diag_wrap", diagRoot)) {
      for (let i = 0; i < 2; i++) {
        try {
          createDiagBlock();
          if (qs("#mem44_diag_wrap", diagRoot)) break;
        } catch (e) {
          console.log("[DiagFallback] create diag failed", e);
        }
      }
    }
    if (!qs("#mem44_diag_wrap", diagRoot)) {
      console.log("[DiagFallback] diag wrap missing even after retry");
      return false;
    }
    return true;
  }

  function createDiagBlock() {
    const wrap = qs("#" + PANEL_ID);
    const root = qs("#mem44_diag_root", wrap);
    if (!wrap || !root) return;
    root.innerHTML = `
      <div id="mem44_diag_wrap" style="margin-top:6px; padding-top:6px; border-top:1px solid #333; font-size:11px; color:#aaa; display:flex; flex-direction:column; gap:4px;">
        <div id="mem44_diag_header" style="display:flex; align-items:center; gap:8px; cursor:pointer; user-select:none;">
          <span>診断 / Diag</span>
          <span id="mem44_diag_toggle" style="font-size:12px;">▼</span>
          <button id="mem44_diag_test" style="margin-left:auto; background:#0ea5e9;border:1px solid #38bdf8;color:#e0f2fe;border-radius:8px;padding:4px 8px;font-size:11px;cursor:pointer;">疎通テスト</button>
        </div>
        <div id="mem44_diag_body" style="display:flex; flex-direction:column; gap:4px;">
          <div id="mem44_diag_last" style="color:#ddd;">last: -</div>
          <div id="mem44_diag_result" style="color:#ddd;">result: -</div>
          <div id="mem44_diag_detail" style="color:#ccc; white-space:pre-wrap; word-break:break-all; max-height:120px; overflow:auto;"></div>
        </div>
      </div>
    `;
    qs("#mem44_diag_test", root)?.addEventListener("click", async () => {
      const url = WEBHOOKS[0];
      setDiagStatus("診断中…", "#fbbf24");
      const payload = { diag: true, ping: true, ts: Date.now() };
      const res = await sendToN8nDiagnostic(payload, url);
      renderDiag(res, url);
    });
    qs("#mem44_diag_header", root)?.addEventListener("click", () => {
      const body = qs("#mem44_diag_body", root);
      const tg = qs("#mem44_diag_toggle", root);
      if (!body || !tg) return;
      const shown = body.style.display !== "none";
      body.style.display = shown ? "none" : "flex";
      tg.textContent = shown ? "▶" : "▼";
    });
  }

  /** ===== メモ候補欄の更新（毎回必ず上書き） ===== */
  function updateMemoCandidateBox(memoBlockRaw) {
    const box = qs("#n8n_memo_candidate");
    if (!box) {
      log("updateMemoCandidateBox: box not found");
      return;
    }
    const text = (memoBlockRaw ?? "").trim();
    if (!text) {
      console.log("[DatingOps] memo_block empty, keep existing memo candidate");
      return;
    }
    console.log("[DatingOps] memo_block from n8n:", text.slice(0, 120) + (text.length > 120 ? "..." : ""));
    box.value = text;
  }

  /** ===== 会話ルート推定（mem44専用） ===== */
  function getChatRoot() {
    // mem44 専用: 中央の会話カラムを強制的に root とする
    const direct =
      qs("#mm1.inbox.inbox_chat") ||
      qs(".inbox.inbox_chat");
    if (direct) {
      console.debug("[DatingOps] getChatRoot: mem44 direct inbox", direct);
      return direct;
    }

    const sendBtn = qsa('input[type="submit"],button').find((b) =>
      /送信して閉じる/.test(b.value || b.textContent || "")
    );
    if (sendBtn) {
      const host =
        (sendBtn.closest("form") || document).parentElement || document;
      const candidates = [
        qs("#mm1.inbox.inbox_chat", host),
        qs(".inbox.inbox_chat", host),
        host.previousElementSibling,
        host.parentElement?.querySelector(".inbox"),
        document.body,
      ].filter(Boolean);

      let best = null,
        score = -1;
      for (const c of candidates) {
        const cnt = qsa(
          ".mmmsg_char, .mmmsg_member, .mb_M, .mb_L, .message, .talk_row, .msg",
          c
        ).length;
        if (cnt > score) {
          score = cnt;
          best = c;
        }
      }
      return best || document.body;
    }
    return (
      qs("#mm1.inbox.inbox_chat") || qs(".inbox.inbox_chat") || document.body
    );
  }

  /** ===== MEM44 speaker 判定ヘルパー ===== */
  function detectSpeaker(el, chatMidX) {
    if (!el) return { speaker: "male", method: "no element" };

    // 1. クラス名による最優先判定（mmsg_char / mmsg_member）
    //    ※ classList.contains を使って正確にマッチ
    if (el.classList && el.classList.contains("mmsg_char")) {
      return { speaker: "female", method: "class mmsg_char" };
    }
    if (el.classList && el.classList.contains("mmsg_member")) {
      return { speaker: "male", method: "class mmsg_member" };
    }

    // 2. フォールバック: className 文字列での判定（旧形式互換）
    const classNames = (el.className || "") + " " +
      Array.from(el.classList || []).join(" ");
    const classLower = classNames.toLowerCase();

    if (classLower.includes("mmsg_char") || classLower.includes("mb_l")) {
      return { speaker: "female", method: "class mmsg_char/mb_L (string)" };
    }
    if (classLower.includes("mmsg_member") || classLower.includes("mb_m")) {
      return { speaker: "male", method: "class mmsg_member/mb_M (string)" };
    }

    // 3. MEM44 固有のクラス（mbR / mbL など）
    if (classLower.includes("mbr") || classLower.includes("msg_r") || classLower.includes("right")) {
      return { speaker: "male", method: "class mbR/msg_r/right" };
    }
    if (classLower.includes("mbl") || classLower.includes("msg_l") || classLower.includes("left")) {
      return { speaker: "female", method: "class mbL/msg_l/left" };
    }

    // 4. 親要素の td の align / text-align で左右判定
    const td = el.closest("td");
    if (td) {
      const align = (td.getAttribute("align") || "").toLowerCase();
      const textAlign = (td.style?.textAlign || "").toLowerCase();
      if (align === "right" || textAlign === "right") {
        return { speaker: "male", method: "td align=right" };
      }
      if (align === "left" || textAlign === "left") {
        return { speaker: "female", method: "td align=left" };
      }
    }

    // 5. 親要素の tr/table の背景色で判定（MEM44特有）
    const tr = el.closest("tr");
    if (tr) {
      const bgColor = (tr.getAttribute("bgcolor") || tr.style?.backgroundColor || "").toLowerCase();
      if (bgColor.includes("e6f") || bgColor.includes("fce") || bgColor.includes("pink")) {
        return { speaker: "female", method: "tr bgcolor pink/fce" };
      }
      if (bgColor.includes("e6e") || bgColor.includes("eef") || bgColor.includes("blue")) {
        return { speaker: "male", method: "tr bgcolor blue/eef" };
      }
    }

    // 6. 座標 fallback（chatMidX より右なら男性）
    if (chatMidX !== undefined) {
      const rect = el.getBoundingClientRect?.() || { left: 0, width: 0 };
      const centerX = rect.left + rect.width / 2;
      if (centerX > chatMidX) {
        return { speaker: "male", method: `position centerX=${Math.round(centerX)} > midX=${Math.round(chatMidX)}` };
      } else {
        return { speaker: "female", method: `position centerX=${Math.round(centerX)} <= midX=${Math.round(chatMidX)}` };
      }
    }

    // 7. 最終 fallback
    return { speaker: "male", method: "default fallback" };
  }

  /**
   * ===== 会話抽出（MEM44 専用: シンプル版） =====
   * mmsg_char / mmsg_member クラスだけで男女判定
   * 返り値: { all, last6, last20 }
   */
  function scrapeConversationStructured(rootOverride) {
    const root = rootOverride || getChatRoot() || document;

    // mmsg_char（キャラ=女性）と mmsg_member（メンバー=男性）を取得
    const selectors = "div.mmsg_char, div.mmsg_member";
    const nodes = Array.from(root.querySelectorAll(selectors));

    log("[MEM44] scrapeConversationStructured: found", nodes.length, "nodes");

    // 各ノードから { speaker, text, timestamp? } を抽出
    const all = [];
    for (const el of nodes) {
      // クラス名だけで speaker を決定（100% 確実）
      let speaker = "unknown";
      if (el.classList.contains("mmsg_char")) {
        speaker = "female";
      } else if (el.classList.contains("mmsg_member")) {
        speaker = "male";
      }

      // テキスト取得（空白正規化）
      const text = (el.innerText || "").replace(/\s+/g, " ").trim();

      // 空テキストは除外
      if (!text) continue;

      // 管理テキスト類を除外
      const isAdminMeta =
        /(管理者メモ|自己紹介文|使用絵文字・顔文字|残り\s*\d+\s*pt|入金|本登録|最終アクセス|累計送信数|返信文グループ|自由メモ|ジャンル|エロ・セフレ募集系|ポイント残高|ふたりメモ|キャラ情報|ユーザー情報)/.test(text);
      if (isAdminMeta) continue;

      // プロフィールヘッダー除外
      if (/^\d{6}\s/.test(text)) continue;

      // 「開封済み」削除
      const cleanText = text.replace(/開封済み/g, "").trim();
      if (!cleanText) continue;

      const tsInfo = extractTimestampFromNode(el, all.length, speaker, cleanText);

      all.push({
        speaker,
        text: cleanText,
        timestamp: tsInfo?.timestamp ?? null,
        timestampMs: tsInfo?.timestampMs ?? null,
      });
    }

    const last20 = all.slice(-20);
    const last6 = all.slice(-6);

    // デバッグログ
    const maleCount = all.filter((m) => m.speaker === "male").length;
    const femaleCount = all.filter((m) => m.speaker === "female").length;
    console.debug("[MEM44 v2.95] scrapeConversationStructured:", {
      total: all.length,
      male: maleCount,
      female: femaleCount,
    });
    console.log(
      "[MEM44] sample (last 6):",
      last6.map((m, i) => ({ idx: i, speaker: m.speaker, text: m.text.slice(0, 40) }))
    );

    return { all, last6, last20 };
  }

  // DEBUG helper (keep as comment for manual console testing):
  // [...document.querySelectorAll('div.mmsg_char, div.mmsg_member')]
  //   .slice(-10)
  //   .map((el, i) => ({
  //     idx: i,
  //     role: el.classList.contains('mmsg_char') ? 'female(char)' : 'male(member)',
  //     text: (el.innerText || '').trim().slice(0, 50),
  //   }));

  /**
   * ===== 青ログステージ算出 =====
   * 直近 male より後に female が連続何通送っているかをカウント
   * - 0 = 未返信（直近 male 以降に female なし）
   * - 1 = 青1（female 1通）
   * - 2 = 青2（female 2通）
   * - 3 = 青3（female 3通）
   * - 4 = 青4（female 4通以上）
   */
  function computeBlueStageFromEntries(entries) {
    if (!entries || !entries.length) return 0;

    const len = entries.length;

    // 末尾から直近 male を探す
    let lastMaleIndex = -1;
    for (let i = len - 1; i >= 0; i--) {
      const role = entries[i]?.role || entries[i]?.speaker;
      if (role === "male") {
        lastMaleIndex = i;
        break;
      }
    }

    // 直近 male が見つからない場合 → 全て female 連投とみなし、青1を返す
    if (lastMaleIndex === -1) {
      return 1;
    }

    // 直近 male より後ろの female 連続数をカウント
    let consecutiveFemale = 0;
    for (let i = lastMaleIndex + 1; i < len; i++) {
      const role = entries[i]?.role || entries[i]?.speaker;
      if (role === "female") {
        consecutiveFemale++;
      } else if (role === "male") {
        // 再度 male が来たらカウント終了
        break;
      } else {
        // unknown 等は連続性を切る扱いで break
        break;
      }
    }

    if (consecutiveFemale <= 0) return 0; // 未返信
    if (consecutiveFemale === 1) return 1;
    if (consecutiveFemale === 2) return 2;
    if (consecutiveFemale === 3) return 3;
    return 4; // 4通以上は青4固定
  }

  // getConversation20: 互換用ラッパー（コピーボタン用）
  function getConversation20() {
    const conv = scrapeConversationStructured();
    return conv.last20.map(m => `${m.speaker === 'male' ? '彼' : '私'}: ${m.text}`).join('\n');
  }

  function getSiteId() {
    const host = location.hostname || "";
    if (!host) return "";
    return host.split(".")[0] || host;
  }

  function getThreadId() {
    const selectors = [
      'input[name*="thread" i]',
      'input[id*="thread" i]',
      'input[name*="messageid" i]',
      'input[id*="messageid" i]',
    ];
    for (const sel of selectors) {
      const el = qs(sel);
      const val = el?.value?.trim();
      if (val) return val;
    }
    const searchMatch = location.search.match(
      /(?:thread|msg|id)=([A-Za-z0-9_-]{4,})/i
    );
    if (searchMatch) return searchMatch[1];
    const pathMatch = location.pathname.match(/(\d{4,})/);
    if (pathMatch) return pathMatch[1];
    return null;
  }

  function getToneSetting() {
    const select = qs('select[name*="tone" i], select[id*="tone" i]');
    const btn = qs('[data-tone]');
    const input = qs('input[name*="tone" i]:checked');
    const val =
      select?.value?.trim() ||
      input?.value?.trim() ||
      btn?.dataset?.tone?.trim() ||
      "";
    return val || null;
  }

  function getBlueStage() {
    const el =
      qs("[data-blue-stage]") ||
      qs('[name*="blue" i]:checked') ||
      qs('[id*="blue" i][data-stage]');
    const val =
      el?.dataset?.blueStage ||
      el?.dataset?.stage ||
      el?.value ||
      el?.textContent ||
      "";
    return val ? val.trim().toLowerCase() : null;
  }

  function getLastUtteranceSync() {
    const { all } = scrapeConversationStructured();
    const last = all[all.length - 1] || { speaker: "", text: "" };
    const who = last.speaker === "male" ? "M" : last.speaker === "female" ? "F" : "";
    return { who, text: last.text, fp: hash(last.text) };
  }

  /** ===== ふたりメモ alert パッチ ===== */
  function patchPairMemoAlertOnce() {
    try {
      const w = typeof unsafeWindow !== "undefined" ? unsafeWindow : window;
      if (!w || w.__datingOpsAlertPatched) return;
      const originalAlert = w.alert;
      if (typeof originalAlert !== "function") return;
      w.alert = function patchedAlert(message) {
        try {
          if (typeof message === "string" && message.indexOf("ふたりメモを更新しました") !== -1) {
            console.log("[DatingOps] auto-skip pair memo alert:", message);
            return;
          }
        } catch (e) {
          console.warn("[DatingOps] patchedAlert error", e);
        }
        return originalAlert.call(this, message);
      };
      w.__datingOpsAlertPatched = true;
      console.log("[DatingOps] patched window.alert for pair memo");
    } catch (e) {
      console.warn("[DatingOps] patchPairMemoAlertOnce failed", e);
    }
  }

  /** ===== ふたりメモ「更新」ボタンを探す ===== */
  function findPairMemoUpdateButton(textarea) {
    if (!textarea) return null;
    let cell = textarea.closest("td");
    if (cell) {
      const btnInCell = cell.querySelector('input[type="button"][value="更新"], input[type="submit"][value="更新"]');
      if (btnInCell) return btnInCell;
    }
    let col = textarea.closest("td.freemmobg_gray, td.freememobg_gray, td.freememo_bg_gray, td.freememo_bg_gray_pd0");
    if (col) {
      const btnInCol = col.querySelector('input[type="button"][value="更新"], input[type="submit"][value="更新"]');
      if (btnInCol) return btnInCol;
    }
    const allButtons = document.querySelectorAll('input[type="button"][value="更新"], input[type="submit"][value="更新"]');
    if (!allButtons.length) return null;
    let best = null;
    let bestScore = -Infinity;
    for (const btn of allButtons) {
      let score = 0;
      const parentTd = btn.closest("td");
      if (parentTd) {
        const txt = parentTd.textContent || "";
        if (txt.indexOf("ふたりメモ") !== -1 || txt.indexOf("ユーザー通算数") !== -1) score += 3;
        if (parentTd.className && parentTd.className.indexOf("freememo") !== -1) score += 5;
      }
      if (textarea && textarea.parentElement && btn.parentElement) {
        const taRect = textarea.getBoundingClientRect();
        const btnRect = btn.getBoundingClientRect();
        const dy = Math.abs(taRect.top - btnRect.top);
        if (dy < 200) score += 2;
        if (dy < 100) score += 2;
      }
      if (score > bestScore) {
        bestScore = score;
        best = btn;
      }
    }
    return best;
  }

  /** ===== ふたりメモを自動保存 ===== */
  function savePairMemo(reason, onDone) {
    try {
      const textarea = getPairMemoTextarea && getPairMemoTextarea();
      if (!textarea) {
        console.log("[DatingOps] savePairMemo: textarea not found", reason);
        onDone && onDone();
        return;
      }
      const btn = findPairMemoUpdateButton(textarea);
      if (!btn) {
        console.log("[DatingOps] savePairMemo: update button not found", reason);
        onDone && onDone();
        return;
      }
      console.log("[DatingOps] auto-click pair memo update button:", reason);
      btn.click();
      pairMemoInitialValue = textarea.value;
      pairMemoDirty = false;
      if (onDone) {
        setTimeout(onDone, 100);
      }
    } catch (e) {
      console.warn("[DatingOps] savePairMemo error", e);
      onDone && onDone();
    }
  }

  /** ===== 自由メモ変更監視 ===== */
  function watchPairMemoChanges() {
    const ta = getPairMemoTextarea();
    if (!ta) {
      console.log('[DatingOps] watchPairMemoChanges: textarea not found');
      return;
    }
    if (ta.dataset.n8nWatched === '1') return;
    ta.dataset.n8nWatched = '1';

    pairMemoInitialValue = ta.value;
    pairMemoDirty = false;

    ta.addEventListener('input', () => {
      pairMemoDirty = (ta.value !== pairMemoInitialValue);
    });
    console.log('[DatingOps] watchPairMemoChanges: watching textarea for changes');
  }

  /** ===== 「送信して閉じる」ボタンを探す ===== */
  function getSendAndCloseButton() {
    const selectors = [
      'input[type="submit"][value="送信して閉じる"]',
      'input[type="button"][value="送信して閉じる"]',
      'button[value="送信して閉じる"]',
    ];
    for (const sel of selectors) {
      const btn = document.querySelector(sel);
      if (btn) return btn;
    }
    return null;
  }

  /** ===== 送信ボタン押下時に自動保存するフック ===== */
  function hookSendButtonAutoSave() {
    const sendBtn = getSendAndCloseButton();
    if (!sendBtn) {
      console.log('[DatingOps] hookSendButtonAutoSave: send button not found');
      return;
    }
    if (sendBtn.dataset.n8nMemoHooked === '1') return;
    sendBtn.dataset.n8nMemoHooked = '1';

    sendBtn.addEventListener('click', (ev) => {
      if (!pairMemoDirty) return;

      console.log('[DatingOps] hookSendButtonAutoSave: memo dirty, auto-saving before send');

      ev.preventDefault();
      ev.stopPropagation();

      savePairMemo('before-send', () => {
        console.log('[DatingOps] hookSendButtonAutoSave: memo saved, now send');
        pairMemoDirty = false;
        sendBtn.click();
      });
    }, true);

    console.log('[DatingOps] hookSendButtonAutoSave: hooked send button');
  }

  /** ===== ふたりメモ欄を探す（mem44専用） ===== */
  function getPairMemoTextarea() {
    const direct = document.querySelector(
      'td.freememo_bg_gray textarea[name="memo_free_memo1"]'
    );
    if (direct) {
      console.log('[DatingOps] getPairMemoTextarea: found via freememo_bg_gray/name=memo_free_memo1');
      return direct;
    }

    const byName = document.querySelector('textarea[name="memo_free_memo1"]');
    if (byName) {
      console.log('[DatingOps] getPairMemoTextarea: found via name=memo_free_memo1');
      return byName;
    }

    const candidates = Array.from(
      document.querySelectorAll('textarea[name*="memo"], textarea[id*="memo"], textarea[name*="free"], textarea[id*="free"]')
    );
    if (candidates.length > 0) {
      const sorted = candidates
        .map(el => ({ el, x: el.getBoundingClientRect().left }))
        .sort((a, b) => a.x - b.x);
      const picked = sorted[sorted.length - 1].el;
      console.log('[DatingOps] getPairMemoTextarea: picked right-most memo-like textarea as fallback', picked);
      return picked;
    }

    console.warn('[DatingOps] getPairMemoTextarea: textarea not found');
    return null;
  }

  /** ===== 自由メモテンプレート自動挿入 ===== */
  function ensurePairMemoTemplate() {
    try {
      const ta = getPairMemoTextarea();
      if (!ta) {
        console.warn('[DatingOps] ensurePairMemoTemplate: textarea not found, skip');
        return;
      }
      const current = (ta.value || '').trim();
      if (current.length > 0) {
        console.log('[DatingOps] ensurePairMemoTemplate: already has content, skip');
        return;
      }
      const template = [
        '■アポ■',
        '',
        '',
        '--------------',
        '♂：',
        '',
        '',
        '♀：',
        ''
      ].join('\n');
      ta.value = template;
      ['input', 'change'].forEach(ev =>
        ta.dispatchEvent(new Event(ev, { bubbles: true }))
      );
      console.log('[DatingOps] inserted default pair memo template');
      savePairMemo("auto-template");
    } catch (e) {
      console.error('[DatingOps] ensurePairMemoTemplate error', e);
    }
  }

  /** ===== サイド情報（mem44専用: 右カラム「ユーザー情報」のみ） ===== */
  function getSideInfoText() {
    let userInfoContent = "";

    const allElements = qsa("td, th, div, span, h2, h3, b, strong");
    for (const el of allElements) {
      const labelText = (el.textContent || "").trim();
      if (labelText === "ユーザー情報" || labelText === "会員情報") {
        log("ユーザー情報ラベル発見:", el.tagName, labelText);

        const parentRow = el.closest("tr");
        if (parentRow) {
          const cells = qsa("td", parentRow);
          for (const cell of cells) {
            if (cell !== el && !cell.contains(el)) {
              const cellText = (cell.innerText || "").trim();
              if (cellText && cellText !== labelText && cellText.length > 10) {
                userInfoContent = cellText;
                log("同行セルからプロフィール取得:", cellText.slice(0, 50));
                break;
              }
            }
          }
        }

        if (!userInfoContent) {
          let sibling = el.nextElementSibling;
          while (sibling && !userInfoContent) {
            const sibText = (sibling.innerText || "").trim();
            if (sibText && sibText.length > 10) {
              userInfoContent = sibText;
              log("兄弟要素からプロフィール取得:", sibText.slice(0, 50));
            }
            sibling = sibling.nextElementSibling;
          }
        }

        if (!userInfoContent) {
          const parentBlock = el.closest("td") || el.closest("div");
          if (parentBlock) {
            const fullText = (parentBlock.innerText || "").trim();
            userInfoContent = fullText.replace(/^(ユーザー情報|会員情報)\s*/i, "").trim();
            if (userInfoContent.length > 10) {
              log("親要素からプロフィール取得:", userInfoContent.slice(0, 50));
            } else {
              userInfoContent = "";
            }
          }
        }

        if (userInfoContent) break;
      }
    }

    if (!userInfoContent) {
      const userInfoBlock = qs(".right_col") || qs(".user_info");
      if (userInfoBlock) {
        userInfoContent = (userInfoBlock.innerText || "").trim();
        log("クラス名からプロフィール取得:", userInfoContent.slice(0, 50));
      }
    }

    if (!userInfoContent) {
      const tables = qsa("table");
      for (const table of tables) {
        const rows = qsa("tr", table);
        for (const row of rows) {
          const cells = qsa("td", row);
          if (cells.length >= 3) {
            const rightCell = cells[cells.length - 1];
            const txt = (rightCell.innerText || "").trim();
            if (
              (txt.includes("年齢") || txt.includes("都道府県") || txt.includes("職業") || txt.includes("無職")) &&
              txt.length > 20
            ) {
              userInfoContent = txt;
              log("テーブル右端からプロフィール取得:", txt.slice(0, 50));
              break;
            }
          }
        }
        if (userInfoContent) break;
      }
    }

    if (userInfoContent) {
      userInfoContent = userInfoContent
        .replace(/^(ユーザー情報|会員情報|管理者メモ)\s*/gi, "")
        .replace(/\s+/g, " ")
        .trim();
    }

    if (!userInfoContent || userInfoContent.length < 10) {
      log("ユーザー情報ブロックが見つかりません、または内容が少なすぎます");
      return "(ユーザー情報なし)";
    }

    console.log("[DatingOps] sideInfoText:", userInfoContent.slice(0, 200));

    return userInfoContent;
  }

  function getCityFromSide() {
    const t = getSideInfoText();
    const m = t.match(
      /(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)[^\n ]*/
    );
    return m ? m[0].trim() : "";
  }

  function getPartnerNameFromSide() {
    const t = getSideInfoText();
    const m = t.match(
      /\b([A-Za-zぁ-んァ-ヶ一-龠々ー][A-Za-zぁ-んァ-ヶ一-龠々ー0-9._-]{1,24})\b/
    );
    return m ? m[1] : "";
  }

  function getOperatorIdFromSide() {
    const t = getSideInfoText();
    const m = t.match(/\b(\d{5,7})\b/);
    return m ? m[1] : "";
  }

  /** ===== 返信欄 ===== */
  function pickReplyTextarea() {
    const sendBtn = qsa('input[type="submit"],button').find((b) =>
      /送信して閉じる/.test(b.value || b.textContent || "")
    );
    const form = sendBtn ? sendBtn.closest("form") || null : null;

    if (form) {
      const direct = form.querySelector(
        'textarea[name*="message" i], textarea[id*="message" i]'
      );
      if (direct) return direct;
      const t2 = [...form.querySelectorAll("textarea")][0];
      if (t2) return t2;
    }
    const all = [...document.querySelectorAll("textarea")];
    if (!all.length) return null;
    if (!sendBtn) return all[0];
    const sb = sendBtn.getBoundingClientRect();
    return (
      all.reduce((best, ta) => {
        const r = ta.getBoundingClientRect();
        const d = Math.hypot(
          (r.left + r.right) / 2 - (sb.left + sb.right) / 2,
          r.bottom - sb.top
        );
        return !best || d < best.d ? { el: ta, d } : best;
      }, null)?.el || null
    );
  }

  function insertReply(text) {
    if (!text) return false;
    const ta = pickReplyTextarea();
    if (!ta) return false;

    // C3: 改行保持のデバッグ（挿入前後で \n\n を確認）
    const hasDoubleNewlineBefore = text.includes('\n\n');
    console.debug('[MEM44] insertReply: hasDoubleNewline(before)=', hasDoubleNewlineBefore, ', len=', text.length);

    ta.focus();
    // textarea.value に直接代入（innerText/textContent/innerHTML は使わない）
    ta.value = text;
    ["input", "change", "keyup"].forEach((ev) =>
      ta.dispatchEvent(new Event(ev, { bubbles: true }))
    );

    // C3: 挿入後の確認
    const hasDoubleNewlineAfter = ta.value.includes('\n\n');
    console.debug('[MEM44] insertReply: hasDoubleNewline(after)=', hasDoubleNewlineAfter, ', textarea.value.len=', ta.value.length);

    try {
      ta.selectionStart = ta.selectionEnd = ta.value.length;
    } catch {}
    return true;
  }

  /** ===== 送信（フェイルオーバ） ===== */
  async function sendToN8nDiagnostic(payload, url) {
    const data = JSON.stringify(payload || {});
    console.log("[ChatOps] POST start (diag)", { url, keys: Object.keys(payload || {}), ts: Date.now() });
    return new Promise((resolve) => {
      GM_xmlhttpRequest({
        method: "POST",
        url,
        data,
        headers: { "Content-Type": "application/json" },
        timeout: 20000,
        onload: (res) => {
          const text = res.responseText || "";
          let parsed = null;
          try {
            parsed = text ? JSON.parse(text) : {};
          } catch (e) {
            parsed = null;
          }
          const ok = res.status >= 200 && res.status < 300;
          if (ok) {
            console.log("[ChatOps] POST ok", { status: res.status, snippet: text.slice(0, 120) });
          } else {
            console.log("[ChatOps] POST fail", { errType: "http", status: res.status, statusText: res.statusText, snippet: text.slice(0, 120), url });
          }
          resolve({
            ok,
            status: res.status,
            statusText: res.statusText,
            data: parsed,
            text,
            snippet: text.slice(0, 400),
            url,
          });
        },
        onerror: (err) => {
          console.log("[ChatOps] POST fail", { errType: "error", status: err?.status, statusText: err?.statusText, url });
          resolve({
            ok: false,
            errType: "error",
            status: err?.status,
            statusText: err?.statusText,
            snippet: "",
            url,
          });
        },
        ontimeout: () => {
          console.log("[ChatOps] POST fail", { errType: "timeout", url });
          resolve({
            ok: false,
            errType: "timeout",
            status: 0,
            statusText: "timeout",
            snippet: "",
            url,
          });
        },
      });
    }).catch((e) => ({
      ok: false,
      errType: "exception",
      status: 0,
      statusText: e?.message || String(e),
      snippet: "",
      url,
    }));
  }

  function postJSONWithFallback(payload) {
    const data = JSON.stringify(payload);
    const tryOne = (url) =>
      new Promise((resolve, reject) => {
        GM_xmlhttpRequest({
          method: "POST",
          url,
          data,
          headers: { "Content-Type": "application/json" },
          timeout: REQUEST_TIMEOUT,
          onload: (res) => {
            const text = res.responseText || "";
            console.log("[MEM44] RAW n8n response text:", text);
            let parsed = null;
            try {
              parsed = text ? JSON.parse(text) : {};
              console.log("[MEM44] PARSED n8n response JSON:", parsed);
            } catch (e) {
              console.warn("[MEM44] FAILED to parse JSON from n8n:", e, "raw:", text);
              parsed = { ok: true, raw: text };
            }
            const hasReply = !!parsed?.reply;
            const hasMemoBlock = !!parsed?.memo_block;
            console.log("[MEM44] hasReply/hasMemoBlock check:", {
              hasReply,
              hasMemoBlock,
              reply: parsed?.reply,
              memo_block: parsed?.memo_block,
            });
            // FIX D: reply_formatted の有無と改行数をデバッグログ
            const chosen = parsed?.reply_formatted || parsed?.reply || "";
            console.debug("[MEM44] reply_formatted?", !!parsed?.reply_formatted, "newlineCount", (chosen.match(/\n/g) || []).length);

            console.log("[MEM44] n8n response:", url, res.status, text?.slice(0, 200));
            if (res.status >= 200 && res.status < 300) {
              resolve(parsed);
            } else
              reject(
                new Error("HTTP " + res.status + " " + (res.responseText || ""))
              );
          },
          onerror: (err) => {
            console.error("[DatingOps] n8n request error:", url, err);
            reject(new Error("GM_xhr onerror"));
          },
          ontimeout: () => {
            console.error("[DatingOps] n8n request timeout:", url);
            reject(new Error("GM_xhr timeout"));
          },
        });
      });

    let p = Promise.reject(new Error("init"));
    WEBHOOKS.forEach((u, i) => {
      p = p.catch(() => tryOne(u));
    });
    return p;
  }

  async function sendToN8n(payload, reason = "") {
    // 同一タブ内ガード
    if (inFlight) {
      setStatus("送信中のため待機", "#f59e0b");
      return null;
    }
    // タブ間ロック取得（n8n 空ボディ対策）
    if (!await acquireN8nLock()) {
      setStatus("他タブ送信中: タイムアウト", "#f87171");
      return null;
    }
    inFlight = true;
    inFlightAt = Date.now();
    setStatus("送信中…", "#ffa94d");
    try {
      const enriched = { ...(payload || {}) };
      if (reason) {
        enriched.meta = enriched.meta || {};
        if (!enriched.meta.reason) enriched.meta.reason = reason;
      }
      const url = WEBHOOKS[0];
      if (!url || String(url).includes("undefined")) {
        console.error("[ChatOps] invalid webhook url", url);
        return null;
      }
      const queueLen = getQueue().items.length;
      console.log("[ChatOps] POST start", {
        url,
        reason,
        jobId: getMyJobId(),
        queueLen,
        keys: Object.keys(enriched || {}),
        convLen: enriched?.conversation?.length,
        long20Len: enriched?.conversation_long20?.length,
        ts: Date.now(),
      });
      const res = await sendToN8nDiagnostic(enriched, url);
      renderDiag(res, WEBHOOKS[0]);
      if (!res?.ok) {
        throw new Error(`HTTP ${res?.status || "?"} ${res?.statusText || ""}`.trim());
      }
      console.log("[ChatOps] POST ok", { status: res?.status, snippet: (res?.text || "").slice(0, 120) });
      setStatus("ok (200)", "#4ade80");
      return res.data || {};
    } catch (e) {
      console.log("[ChatOps] POST fail", { errType: "exception", status: 0, statusText: e?.message, snippet: "", url: WEBHOOKS[0] });
      setStatus("送信失敗: 再生成/再送できます", "#f87171");
      console.warn("[DatingOps] send error:", e);
      return null;
    } finally {
      inFlight = false;
      inFlightAt = 0;
      releaseN8nLock();  // タブ間ロック解放
    }
  }

  // メモ更新用：/memo-update に POST（Featherless memo-flow）
  function postMemoJSONWithFallback(payload) {
    const data = JSON.stringify(payload);
    const tryOne = (url) =>
      new Promise((resolve, reject) => {
        GM_xmlhttpRequest({
          method: "POST",
          url,
          data,
          headers: { "Content-Type": "application/json" },
          timeout: 20000,
          onload: (res) => {
            console.log("[DatingOps] memo n8n response:", url, res.status, res.responseText);
            if (res.status >= 200 && res.status < 300) {
              try {
                resolve(JSON.parse(res.responseText || "{}"));
              } catch {
                resolve({ ok: true, raw: res.responseText });
              }
            } else {
              reject(
                new Error("HTTP " + res.status + " " + (res.responseText || ""))
              );
            }
          },
          onerror: (err) => {
            console.error("[DatingOps] memo n8n request error:", url, err);
            reject(new Error("GM_xhr onerror"));
          },
          ontimeout: () => {
            console.error("[DatingOps] memo n8n request timeout:", url);
            reject(new Error("GM_xhr timeout"));
          },
        });
      });
    let p = Promise.reject(new Error("init"));
    MEMO_WEBHOOKS.forEach((u) => {
      p = p.catch(() => tryOne(u));
    });
    return p;
  }

  async function buildWebhookPayload(isRegenerate = false) {
    // 新しい構造化会話取得（クラス名だけで男女判定）
    const conv = scrapeConversationStructured();

    const profileText = getSideInfoText() || "";

    // speaker -> role に変換して payload 用配列を作成
    const conv6 = conv.last6.map(m => ({
      role: m.speaker,
      text: m.text,
      timestamp: m.timestamp ?? null,
      timestampMs: m.timestampMs ?? null,
    }));
    const conv20 = conv.last20.map(m => ({
      role: m.speaker,
      text: m.text,
      timestamp: m.timestamp ?? null,
      timestampMs: m.timestampMs ?? null,
    }));

    // 青ログステージを会話から自動算出
    const blueStage = computeBlueStageFromEntries(conv20);

    // タイムスタンプを付与
    const now = new Date();
    const timestamp = now.toISOString();
    const timestampMs = now.getTime();

    // デバッグ用ログ
    console.debug("[MEM44 v2.95] buildWebhookPayload:", {
      blueStage,
      conv6Count: conv6.length,
      conv20Count: conv20.length,
      male: conv.all.filter(m => m.speaker === "male").length,
      female: conv.all.filter(m => m.speaker === "female").length,
      timestamp,
      timestampMs,
    });
    console.debug("[MEM44 v2.5] conversation sample (last 6):",
      conv6.map((m, idx) => ({ idx, role: m.role, text: m.text.slice(0, 50) }))
    );

    // UIから温度・一言プロンプトを取得
    const tempEl = qs("#n8n_temp");
    const promptEl = qs("#n8n_prompt");
    const tempVal = tempEl ? parseFloat(tempEl.value) : NaN;
    const promptVal = promptEl ? promptEl.value.trim() : "";
    // 初回生成: temperatureはpayloadに含めない（n8n側でデフォルト1.0を使用）
    // 再生成: UIスライダーの値をpayloadに含める
    // 再生成時のみtemperatureを含める（それ以外はundefined → n8nでデフォルト1.0）
    const temperature = isRegenerate && Number.isFinite(tempVal) ? tempVal : undefined;
    const oneLinerPrompt = promptVal || null;
    // regenerateフラグ
    const regenerate = isRegenerate;

    // デバッグ
    console.debug("[MEM44] payload extras: regenerate=", regenerate, "temp=", temperature, "oneLiner=", oneLinerPrompt?.slice(0, 30));

    // FIX C: tzOffsetMin + localHour を追加（時間帯ズレ対策）
    // getTimezoneOffset() は「UTCとの差(分)で西が+」なので符号反転
    const tzOffsetMin = -new Date().getTimezoneOffset();
    const localHour = new Date().getHours();

    // allowedEmojis（暫定で空配列、将来プロフから抽出）
    const allowedEmojis = [];

    return {
      site: getSiteId(),
      threadId: getThreadId(),
      tone: getToneSetting(),
      blueStage,
      conversation: conv6,
      conversation_long20: conv20,
      profileText,
      timestamp,
      timestampMs,
      tzOffsetMin,
      localHour,
      allowedEmojis,
      temperature,
      oneLinerPrompt,
      regenerate,
    };
  }

  async function sendManual() {
    // キュー処理中は手動送信を抑止（競合回避）
    if (workerActive) {
      setStatus("処理中…", "#f59e0b");
      return;
    }
    if (inFlight) {
      setStatus("送信中のため待機", "#f59e0b");
      return;
    }
    try {
      const payload = await buildWebhookPayload(true);  // 再生成（UIスライダーのtemperatureを使用）
      console.log("[MEM44 v2.95] sending payload to n8n:", payload.timestamp, payload);
      const res = await sendToN8n(payload);
      if (!res) return;
      // reply_formatted を優先使用（文頭制御・疑問文化・改行整形済み）
      const reply =
        res?.reply_formatted ||
        res?.reply ||
        res?.text ||
        res?.message ||
        res?.choices?.[0]?.message?.content ||
        "";
      const memoBlock = res?.memo_block ?? res?.memo ?? res?.memo_candidate ?? "";
      if (res?.reply_formatted) console.debug("[MEM44] using reply_formatted");
      if (reply) {
        const ok = insertReply(reply);
        setStatus(ok ? "挿入OK" : "挿入NG", ok ? "#4ade80" : "#f87171");
        console.debug("[MEM44] manual-send: got reply:", reply.slice(0, 120) + (reply.length > 120 ? "..." : ""));
      } else {
        setStatus("応答空", "#f59e0b");
      }

      // memo_block を左下パネルのメモ候補欄に表示
      updateMemoCandidateBox(memoBlock);
      console.debug("[MEM44] memo_block from n8n:", memoBlock ? memoBlock.slice(0, 120) : "(empty)");
    } catch (e) {
      setStatus("送信失敗", "#f87171");
      console.warn("[DatingOps] send error:", e);
      alert("n8n送信エラー：" + (e?.message || e));
    }
  }

  // ===== メモ更新ボタン処理（/memo-updateを叩く） =====
  async function sendMemoUpdate() {
    setStatus("メモ更新中…", "#a855f7");
    try {
      // 新しい構造化会話取得
      const conv = scrapeConversationStructured();
      const conversation_long20 = conv.last20.map(m => ({ role: m.speaker, text: m.text }));

      // プロフィール
      const profileText = getSideInfoText() || "";

      // 既存ふたりメモ
      const memoTa = getPairMemoTextarea();
      const existingPairMemo = memoTa ? memoTa.value || "" : "";

      const payload = {
        profileText,
        conversation_long20,
        existingPairMemo,
      };

      console.log("[DatingOps] sending memo payload to n8n:", payload);
      const res = await postMemoJSONWithFallback(payload);
      const memoText = (res && res.memo_candidate) ? String(res.memo_candidate).trim() : "";

      if (memoText) {
        if (memoTa) {
          // ふたりメモ欄に直接反映
          memoTa.value = memoText;
          ["input", "change", "keyup"].forEach((ev) =>
            memoTa.dispatchEvent(new Event(ev, { bubbles: true }))
          );
          // 自動保存フラグと連携
          pairMemoDirty = true;
          savePairMemo("memo-update");
          setStatus("メモ更新OK", "#4ade80");
        } else {
          // メモ欄が取れない場合は、候補欄にだけ反映
          updateMemoCandidateBox(memoText);
          setStatus("メモ候補更新OK", "#4ade80");
        }
      } else {
        setStatus("メモ候補空", "#f59e0b");
      }
    } catch (e) {
      console.warn("[DatingOps] memo update error:", e);
      setStatus("メモ更新失敗", "#f97316");
      alert("メモ更新エラー：" + (e?.message || e));
    }
  }

  /** ===== 自動送信：ページロード時に 1 回だけ実行（キュー登録版） ===== */
  function mountAuto() {
    pruneQueueIfTooLarge();

    // AutoGuard: 起動時コンテキストログ（1回だけ）
    (() => {
      const params = new URLSearchParams(location.search);
      const ctx = {
        domain: location.host || "",
        box_id: params.get("box_id") || "",
        mid: getParamAny(params, MID_KEYS),
        cid: getParamAny(params, CID_KEYS),
        checknumber: params.get("checknumber") || "",
        tabId: TAB_ID || "",
      };
      const missing = Object.entries(ctx).filter(([, v]) => !v).map(([k]) => k);
      if (missing.length > 0) {
        console.warn("[AutoGuard] context WARN", ctx, { missing });
      } else {
        console.log("[AutoGuard] context OK", ctx);
      }
    })();

    // AUTO_SEND_ON_LOAD が false でも、キューシステムは常に有効
    // false の場合は enqueue しないが、他タブからの dispatch は受け付ける

    const myJobId = getMyJobId();
    log("mountAuto: myJobId =", myJobId, "AUTO_SEND_ON_LOAD =", AUTO_SEND_ON_LOAD);
    setDiagStatus("auto: start", "#c084fc");
    initOpenCheckClickListener();
    checkWindowLoadAutoTrigger();

    // storage イベント監視を開始
    setupStorageListener();

    // ディスパッチャを開始（全タブで試みるが、1つだけがロック取得）
    startDispatcher();

    // UI更新
    updateQueueUI();

    // 既にキューにある自分のジョブをチェック
    checkAndProcessMyJob();

    if (!AUTO_SEND_ON_LOAD) {
      log("auto-send disabled: not enqueueing (manual send only)");
      setStatus("待機中", "#9aa");
      return;
    }

    // autoFired dedupe
    const autoKey = getAutoKey();
    const firedKey = `${AUTO_FIRED_PREFIX}:${autoKey}`;
    if (GM_getValue(firedKey, false)) {
      console.log("[ChatOps] auto skipped: already fired", autoKey);
      return;
    }
    GM_setValue(firedKey, true);
    console.log("[ChatOps] auto fired", autoKey);

    // キューに登録
    const url = location.href;
    const enqueued = enqueueJob(myJobId, url);
    if (enqueued) {
      // C4: SHOW_QUEUE_STATUS が false ならキュー表示を抑止
      if (SHOW_QUEUE_STATUS) {
        setStatus("キュー登録", "#60a5fa");
      } else {
        setStatus("処理中…", "#ffa94d");
      }
      log("[Queue] enqueue:", myJobId, "queue.length=", getQueue().items.length);
    } else {
      // 既に登録済み（ページリロード等）
      if (SHOW_QUEUE_STATUS) {
        setStatus("キュー待ち", "#60a5fa");
      }
      log("[Queue] already in queue:", myJobId);
    }

    // C4: enqueue 直後に即座に処理開始を試みる
    log("[Queue] trying immediate checkAndProcessMyJob after enqueue");
    checkAndProcessMyJob();

    // 少し待ってからもう一度試行（ディスパッチャがロック取得完了後に対応）
    setTimeout(() => {
      log("[Queue] delayed checkAndProcessMyJob");
      checkAndProcessMyJob();
    }, 500);
  }

  /** ===== Main ===== */
  (async function init() {
    if (!isPersonalSendPage()) {
      log("skip: not personalbox");
      return;
    }

    // 同一ページでの二重初期化防止
    if (window.__datingOpsInitialized) {
      console.debug("[DatingOps] skip: already initialized in init()");
      return;
    }
    window.__datingOpsInitialized = true;

    // ふたりメモ更新アラートをパッチ
    patchPairMemoAlertOnce();

    // レイアウト安定待ち
    for (let i = 0; i < 5; i++) await sleep(150);
    ensurePanel();
    const t = qs("#n8n_temp"),
      tv = qs("#n8n_temp_val");
    if (t && tv) tv.textContent = (+t.value).toFixed(1);

    // 自由メモが空のときはテンプレートを挿入
    ensurePairMemoTemplate();

    // 自由メモ変更監視＆送信ボタンフック
    watchPairMemoChanges();
    hookSendButtonAutoSave();

    mountAuto();
    log("ready.");
  })();
})();
