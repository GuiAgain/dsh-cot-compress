/**
 * dsh-cot-compress 0.5.0 —— 浏览器半。
 *
 * 三处界面：
 *   1. 输入框工具栏（conversation.hero.options）：压缩范围开关，紧贴「标准模式」；
 *   2. 侧栏入口（sidebar.panellist）：推理压缩；
 *   3. 整页（main，key=cot-compress）：压缩可视化。
 *
 * 只通过 SPEC §3 的 HTTP 接口读写后端：
 *   /cot-compress/api/state、summary、sessions、entries、entry、usage。
 *
 * 打包格式是手写经典脚本：window.__ModuleLoader__.load({id, factory}).
 * 只 require react / react/jsx-runtime / @deepseek-ai/dsh-client-ui-primitives。
 * 样式全部用 --dsw-* token，类名统一 cotc- 前缀。
 */
window.__ModuleLoader__.load({
  id: "dsh-cot-compress",
  factory: (require) => {
    var module = { exports: {} };
    var exports = module.exports;

    var React = require("react");
    var jsxRuntime = require("react/jsx-runtime");
    var primitives = require("@deepseek-ai/dsh-client-ui-primitives") || {};
    var jsx = jsxRuntime.jsx;
    var jsxs = jsxRuntime.jsxs;

    var PANEL_ID = "cot-compress";
    var API_BASE = "/cot-compress/api/";
    var PAGE_SIZE = 50;
    var POLL_MS = 10000;
    var TOAST_HOLD_MS = 4000;

    // ------------------------------------------------------------------
    // 纯函数（__test 暴露给 node --test）
    // ------------------------------------------------------------------

    var SCOPES = ["lead", "subagent", "all", "off"];

    var SCOPE_CHIP_TEXT = {
      lead: "压缩主智能体",
      subagent: "压缩子智能体",
      all: "全部压缩",
      off: "压缩已关闭",
    };

    var SCOPE_MENU = {
      lead: { name: "只压缩主智能体", description: "子智能体会话保持原文" },
      subagent: { name: "只压缩子智能体", description: "主会话保持原文" },
      all: { name: "全部压缩", description: "主会话和子智能体都压缩" },
      off: { name: "关闭", description: "只停止新的压缩，已经替换的段保持不变" },
    };

    var REASON_TEXT = {
      "not-fewer-tokens": "省得不够（按 token）",
      "not-shorter": "没有变短",
      "all-chunks-failed": "所有分块都失败",
      quote: "引文对不上原文",
      timeout: "整理超时",
      "tool-markup": "混入工具调用标记",
      "below-min-ratio": "压得过短，疑似失真",
      empty: "结果为空",
      "no-finish": "整理没有正常结束",
    };

    function reasonLabel(reason) {
      if (typeof reason !== "string" || reason.length === 0) return "未知原因";
      if (Object.prototype.hasOwnProperty.call(REASON_TEXT, reason)) return REASON_TEXT[reason];
      if (reason.indexOf("schema:") === 0) return "格式不合格";
      if (reason.indexOf("error") === 0 || reason.indexOf("failed") === 0) return "请求出错";
      return reason;
    }

    function normalizeScope(value) {
      return SCOPES.indexOf(value) >= 0 ? value : "lead";
    }

    function isFiniteNumber(value) {
      return typeof value === "number" && Number.isFinite(value);
    }

    function numberOr(value, fallback) {
      if (isFiniteNumber(value)) return value;
      var parsed = Number(value);
      return Number.isFinite(parsed) && value !== null && value !== "" ? parsed : fallback;
    }

    function formatNumber(value) {
      if (!isFiniteNumber(value)) return "—";
      var text = String(Math.round(value));
      var negative = text.charAt(0) === "-";
      if (negative) text = text.slice(1);
      text = text.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      return negative ? "-" + text : text;
    }

    function formatSeconds(ms) {
      if (!isFiniteNumber(ms) || ms < 0) return "—";
      return (ms / 1000).toFixed(1) + " 秒";
    }

    function pad2(value) {
      return value < 10 ? "0" + String(value) : String(value);
    }

    function asDate(value) {
      // 日志里的 ts 是 ISO 字符串，也接受毫秒数。
      if (typeof value === "string" && value !== "") value = Date.parse(value);
      if (!isFiniteNumber(value)) return null;
      var date = new Date(value);
      return Number.isNaN(date.getTime()) ? null : date;
    }

    function formatTime(value) {
      var date = asDate(value);
      if (date === null) return "—";
      return pad2(date.getMonth() + 1) + "-" + pad2(date.getDate()) + " " + pad2(date.getHours()) + ":" + pad2(date.getMinutes());
    }

    function formatClock(value) {
      var date = asDate(value);
      if (date === null) return "—";
      return pad2(date.getHours()) + ":" + pad2(date.getMinutes()) + ":" + pad2(date.getSeconds());
    }

    function formatRatio(value) {
      if (value === null || value === undefined || value === "") return "—";
      var parsed = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(parsed)) return "—";
      return parsed.toFixed(2);
    }

    function kindLabel(kind) {
      if (kind === "lead") return "主";
      if (kind === "subagent") return "子";
      return "未知";
    }

    function kindName(kind) {
      if (kind === "lead") return "主智能体";
      if (kind === "subagent") return "子智能体";
      return "未知";
    }

    var UNKNOWN_KIND_TITLE = "这条记录来自 0.5.0 之前，且没找到对应的会话文件";

    function bareSessionId(value) {
      if (typeof value !== "string" || value.length === 0) return null;
      return value.replace(/^session-/, "");
    }

    /** 「主会话 xxxxxxxx」：显示父会话 id 的前 8 位；没有父会话则为 null。 */
    function parentSessionLabel(parentSessionId) {
      var bare = bareSessionId(parentSessionId);
      if (bare === null) return null;
      return "主会话 " + bare.slice(0, 8);
    }

    /** 在会话列表里找父会话的原始 id（忽略可选的 session- 前缀）。 */
    function matchSessionId(parentSessionId, sessionIds) {
      var bare = bareSessionId(parentSessionId);
      if (bare === null || !Array.isArray(sessionIds)) return null;
      for (var index = 0; index < sessionIds.length; index += 1) {
        var candidate = sessionIds[index];
        if (typeof candidate !== "string") continue;
        if (candidate === parentSessionId || bareSessionId(candidate) === bare) return candidate;
      }
      return null;
    }

    function sourceLabel(source) {
      if (source === "tool-wait") return "工具等待时";
      if (source === "turn-end") return "回合结束时";
      if (typeof source === "string" && source.length > 0) return source;
      return "未知来源";
    }

    function formatTurnLabel(hostTurn, turn) {
      if (isFiniteNumber(hostTurn)) return "第 " + formatNumber(hostTurn) + " 回合";
      if (isFiniteNumber(turn)) return "≈ 第 " + formatNumber(turn) + " 回合";
      return "回合未知";
    }

    function failureCount(value) {
      if (Array.isArray(value)) return value.length;
      if (isFiniteNumber(value)) return value;
      return 0;
    }

    function classifyLogLine(line) {
      var row = line;
      if (typeof line === "string") {
        try {
          row = JSON.parse(line);
        } catch (_error) {
          return null;
        }
      }
      if (row === null || typeof row !== "object" || Array.isArray(row)) return null;
      if (row.type === "session") return "session";
      if (Object.prototype.hasOwnProperty.call(row, "origChars")) return "event";
      if (Object.prototype.hasOwnProperty.call(row, "usage")) return "usage";
      return null;
    }

    function buildSummaryView(summary) {
      var source = summary !== null && typeof summary === "object" ? summary : {};
      var attempted = numberOr(source.attempted, 0);
      var replaced = numberOr(source.replaced, 0);
      var superseded = numberOr(source.superseded, 0);
      var origTokensEst = numberOr(source.origTokensEst, 0);
      var compTokensEst = numberOr(source.compTokensEst, 0);
      var savedTokensEst = numberOr(source.savedTokensEst, 0);
      var avgRatio = source.avgRatio === undefined || source.avgRatio === null ? null : source.avgRatio;
      var avgCompressMs = source.avgCompressMs === undefined || source.avgCompressMs === null ? null : numberOr(source.avgCompressMs, null);
      var maxCompressMs = source.maxCompressMs === undefined || source.maxCompressMs === null ? null : numberOr(source.maxCompressMs, null);
      var cards = [
        {
          id: "replaced",
          title: "已替换",
          value: formatNumber(replaced) + " 段",
          note: "共尝试 " + formatNumber(attempted) + " 段 · 迟到作废 " + formatNumber(superseded),
        },
        {
          id: "saved",
          title: "估算节省",
          value: formatNumber(savedTokensEst) + " token",
          note: "原 " + formatNumber(origTokensEst) + " → 现 " + formatNumber(compTokensEst) + "（本地估算）",
        },
        {
          id: "ratio",
          title: "平均压缩比",
          value: formatRatio(avgRatio),
          note: "按 token 估算，只算已替换的段",
        },
        {
          id: "duration",
          title: "平均整理耗时",
          value: formatSeconds(avgCompressMs),
          note: "最长 " + (maxCompressMs === null ? "—" : formatSeconds(maxCompressMs)),
        },
      ];
      var rawReasons = source.reasons !== null && typeof source.reasons === "object" ? source.reasons : {};
      var reasonRows = Object.keys(rawReasons).map(function (reason) {
        return {
          reason: reason,
          label: reasonLabel(reason),
          count: numberOr(rawReasons[reason], 0),
        };
      }).filter(function (row) {
        return row.count > 0;
      });
      reasonRows.sort(function (left, right) {
        if (right.count !== left.count) return right.count - left.count;
        return left.label.localeCompare(right.label, "zh-CN");
      });
      var maxCount = reasonRows.length > 0 ? reasonRows[0].count : 0;
      var reasons = reasonRows.map(function (row) {
        return {
          reason: row.reason,
          label: row.label,
          count: row.count,
          pct: maxCount > 0 ? Math.round((row.count / maxCount) * 100) : 0,
        };
      });
      return { cards: cards, reasons: reasons };
    }

    function buildSessionList(items) {
      var source = Array.isArray(items) ? items : [];
      return source.map(function (item) {
        var sessionId = typeof item.sessionId === "string" ? item.sessionId : "";
        var label = typeof item.label === "string" && item.label.length > 0 ? item.label : null;
        var kind = item.agentKind === "lead" || item.agentKind === "subagent" ? item.agentKind : "unknown";
        var kindSource = item.kindSource === "log" || item.kindSource === "session-file" ? item.kindSource : null;
        var parentSessionId = typeof item.parentSessionId === "string" && item.parentSessionId.length > 0 ? item.parentSessionId : null;
        return {
          id: sessionId,
          title: label !== null ? label : "会话 " + sessionId.replace(/^session-/, "").slice(0, 8),
          kind: kind,
          kindText: kindLabel(kind),
          kindTitle: kind === "unknown" ? UNKNOWN_KIND_TITLE : null,
          kindSource: kindSource,
          parentSessionId: parentSessionId,
          parentLabel: parentSessionLabel(parentSessionId),
          time: formatTime(item.lastTs),
          replaceText: "替换 " + formatNumber(numberOr(item.replaced, 0)) + "/" + formatNumber(numberOr(item.attempted, 0)) + " 段",
          ratioText: "压缩比 " + formatRatio(item.ratio),
          cwd: typeof item.cwd === "string" ? item.cwd : null,
        };
      });
    }

    // ------------------------------------------------------------------
    // 错误与接口
    // ------------------------------------------------------------------

    function makeError(message, status, code) {
      var error = new Error(message);
      error.status = status;
      error.code = code;
      return error;
    }

    function errorMessage(error) {
      if (error && typeof error.message === "string" && error.message.length > 0) return error.message;
      if (typeof error === "string" && error.length > 0) return error;
      return "未知错误";
    }

    function parseResponse(response) {
      return response.json().then(function (body) {
        if (!response.ok) {
          var message = body && typeof body.message === "string" && body.message.length > 0
            ? body.message
            : "接口返回 " + String(response.status);
          throw makeError(message, response.status, body && body.error);
        }
        return body;
      }, function () {
        if (!response.ok) throw makeError("接口返回 " + String(response.status), response.status, null);
        return null;
      });
    }

    function apiRequest(path, init) {
      var options = init || {};
      if (typeof fetch !== "function") {
        return Promise.reject(makeError("当前环境没有 fetch，读不到压缩插件的数据接口。", 0, "no-fetch"));
      }
      return fetch(API_BASE + path, options).then(parseResponse, function (error) {
        throw makeError("读不到压缩插件的数据接口（插件后端可能还没重新加载）。", 0, "network");
      });
    }

    function apiGet(path) {
      return apiRequest(path, {
        method: "GET",
        headers: { accept: "application/json" },
        cache: "no-store",
      });
    }

    function apiPostState(scope) {
      return apiRequest("state", {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify({ scope: scope }),
      });
    }

    function getSummary(kind) {
      return apiGet("summary?kind=" + encodeURIComponent(kind || "all"));
    }

    function getSessions(kind, cursor) {
      var query = "sessions?kind=" + encodeURIComponent(kind || "all") + "&limit=" + String(PAGE_SIZE);
      if (cursor !== null && cursor !== undefined && cursor !== "") query += "&cursor=" + encodeURIComponent(String(cursor));
      return apiGet(query);
    }

    function getEntries(sessionId, cursor) {
      var query = "entries?sessionId=" + encodeURIComponent(sessionId) + "&limit=" + String(PAGE_SIZE);
      if (cursor !== null && cursor !== undefined && cursor !== "") query += "&cursor=" + encodeURIComponent(String(cursor));
      return apiGet(query);
    }

    function getEntry(line) {
      return apiGet("entry?line=" + encodeURIComponent(String(line)));
    }

    function getUsage(sessionId) {
      return apiGet("usage?sessionId=" + encodeURIComponent(sessionId));
    }

    // ------------------------------------------------------------------
    // 模块级共享 store（开关与可视化页共用）
    // ------------------------------------------------------------------

    var scopeSnapshot = {
      status: "idle", // idle | loading | ready | unavailable
      scope: "lead",
      saving: false,
      error: null,
      info: null,
    };
    var scopeListeners = new Set();

    var scopeStore = {
      get: function () {
        return scopeSnapshot;
      },
      set: function (patch) {
        scopeSnapshot = Object.assign({}, scopeSnapshot, patch);
        scopeListeners.forEach(function (listener) {
          try {
            listener();
          } catch (_error) {
            // 订阅者异常不影响其它界面。
          }
        });
      },
      subscribe: function (listener) {
        scopeListeners.add(listener);
        return function () {
          scopeListeners.delete(listener);
        };
      },
    };

    var scopeLoad = null;

    function ensureScopeLoaded(force) {
      if (force === true) scopeLoad = null;
      if (scopeLoad !== null) return scopeLoad;
      scopeLoad = apiGet("state").then(function (info) {
        var scope = normalizeScope(info && info.scope);
        scopeStore.set({ status: "ready", scope: scope, info: info || null, error: null, saving: false });
        return info;
      }, function (error) {
        scopeStore.set({ status: "unavailable", info: null, error: errorMessage(error), saving: false });
        return null;
      });
      return scopeLoad;
    }

    function changeScope(next) {
      if (SCOPES.indexOf(next) < 0) {
        return Promise.resolve({ ok: false, error: "范围取值非法" });
      }
      var snapshot = scopeSnapshot;
      if (snapshot.status !== "ready") {
        return Promise.resolve({ ok: false, error: "压缩插件的数据接口不可用" });
      }
      if (snapshot.scope === next) return Promise.resolve({ ok: true });
      var previous = snapshot.scope;
      scopeStore.set({ scope: next, saving: true });
      return apiPostState(next).then(function (body) {
        var applied = normalizeScope(body && body.scope);
        scopeStore.set({ scope: applied, saving: false, error: null });
        return { ok: true };
      }, function (error) {
        scopeStore.set({ scope: previous, saving: false });
        return { ok: false, error: errorMessage(error) };
      });
    }

    var useScopeStore = typeof React.useSyncExternalStore === "function"
      ? function useScopeStore() {
          return React.useSyncExternalStore(scopeStore.subscribe, scopeStore.get, scopeStore.get);
        }
      : function useScopeStore() {
          var pair = React.useState(0);
          var force = pair[1];
          React.useEffect(function () {
            return scopeStore.subscribe(function () {
              force(function (value) {
                return value + 1;
              });
            });
          }, []);
          return scopeStore.get();
        };

    // ------------------------------------------------------------------
    // 样式（类名前缀 cotc-，颜色/圆角/字体只用 --dsw-* token）
    // ------------------------------------------------------------------

    var CSS = [
      ".cotc-toolbarRoot{display:inline-flex;align-items:center;min-width:0}",
      ".cotc-menuAnchor{min-width:54px;max-width:100%}",
      ".cotc-seat{border-radius:var(--dsw-radius-sm);min-width:0;max-width:min(100%,240px);min-height:28px;color:var(--dsw-alias-label-primary);white-space:nowrap;cursor:pointer;background:0 0;border:none;align-items:center;gap:4px;padding:0 8px;font-size:13px;font-weight:500;line-height:20px;display:inline-flex;font-family:var(--dsw-font-family)}",
      ".cotc-seat[aria-expanded=true]{background:var(--dsw-alias-interactive-bg-hover)}",
      "@media (hover:hover){.cotc-seat:not(:disabled):hover{background:var(--dsw-alias-interactive-bg-hover)}}",
      ".cotc-seat:disabled{cursor:default}",
      ".cotc-seatOff{color:var(--dsw-alias-label-caption)}",
      ".cotc-seatIcon{color:var(--dsw-alias-label-primary);flex:none}",
      ".cotc-seatOff .cotc-seatIcon{color:var(--dsw-alias-label-caption)}",
      ".cotc-seatLabel{text-overflow:ellipsis;white-space:nowrap;min-width:0;overflow:hidden}",
      ".cotc-chevron{color:var(--dsw-alias-label-caption);flex:none}",
      ".cotc-item{flex-direction:column;gap:2px;max-width:280px;display:flex}",
      ".cotc-itemName{color:var(--dsw-alias-label-primary);font-size:13px;line-height:20px}",
      ".cotc-itemDesc{color:var(--dsw-alias-label-caption);white-space:normal;font-size:12px;line-height:16px}",
      ".cotc-fallbackAnchor{position:relative;display:inline-flex}",
      ".cotc-fallbackMenu{position:absolute;top:calc(100% + 4px);left:0;z-index:100;min-width:180px;max-width:320px;padding:4px;border-radius:var(--dsw-radius-lg);border:1px solid var(--dsw-alias-border-l1);background:var(--dsw-alias-bg-base);box-shadow:var(--dsw-elevation-prominent);display:flex;flex-direction:column}",
      ".cotc-fallbackItem{border:none;background:transparent;color:var(--dsw-alias-label-primary);text-align:left;border-radius:var(--dsw-radius-md);padding:6px 8px;min-height:34px;font-size:13px;line-height:20px;cursor:pointer}",
      ".cotc-fallbackItem:hover{background:var(--dsw-alias-interactive-bg-hover)}",
      ".cotc-fallbackSeparator{height:.5px;margin:3px 8px;background:var(--dsw-alias-border-l2)}",
      ".cotc-page{height:100%;overflow:auto;background:var(--dsw-alias-bg-base);color:var(--dsw-alias-label-primary);font-family:var(--dsw-font-family);font-size:14px;line-height:1.6;box-sizing:border-box;padding:24px 32px}",
      ".cotc-page *{box-sizing:border-box}",
      ".cotc-pageInner{max-width:1200px;margin:0 auto}",
      ".cotc-titleRow{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}",
      ".cotc-title{font-size:20px;font-weight:600;line-height:28px;margin:0}",
      ".cotc-titleActions{display:flex;align-items:center;gap:12px;flex-wrap:wrap}",
      ".cotc-updated{font-size:12px;color:var(--dsw-alias-label-caption);white-space:nowrap}",
      ".cotc-iconButton{width:32px;height:32px;min-height:32px;padding:0;display:inline-flex;align-items:center;justify-content:center;border:none;background:transparent;border-radius:var(--dsw-radius-sm);color:var(--dsw-alias-label-secondary);cursor:pointer}",
      ".cotc-iconButton:hover{background:var(--dsw-alias-interactive-bg-hover)}",
      ".cotc-controlGroup{display:inline-flex;align-items:center;gap:8px;min-width:0}",
      ".cotc-controlLabel{font-size:12px;line-height:18px;color:var(--dsw-alias-label-caption);white-space:nowrap}",
      ".cotc-controlWrap{display:inline-flex;min-width:0}",
      ".cotc-filterWithLabel{display:flex;align-items:center;gap:8px;margin:0 0 12px;min-width:0}",
      ".cotc-filterWithLabel .cotc-filterRow{margin:0}",
      ".cotc-filterRow{display:flex;align-items:center;gap:8px;margin:0 0 12px}",
      ".cotc-filterButton{border:none;background:transparent;color:var(--dsw-alias-label-secondary);font-family:var(--dsw-font-family);font-size:12px;line-height:20px;height:26px;padding:0 10px;border-radius:var(--dsw-radius-sm);cursor:pointer}",
      ".cotc-filterButton[data-active=true]{background:var(--dsw-alias-interactive-bg-hover);color:var(--dsw-alias-label-primary)}",
      ".cotc-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px}",
      ".cotc-card{border:1px solid var(--dsw-alias-border-l1);border-radius:var(--dsw-radius-md);padding:14px 16px;min-width:0}",
      ".cotc-cardTitle{font-size:12px;color:var(--dsw-alias-label-caption)}",
      ".cotc-cardValue{font-size:22px;font-weight:600;line-height:30px;margin-top:2px}",
      ".cotc-cardNote{font-size:12px;color:var(--dsw-alias-label-caption);line-height:18px;margin-top:2px}",
      ".cotc-section{margin-top:24px}",
      ".cotc-sectionTitle{font-size:14px;font-weight:600;line-height:22px;margin:0 0 10px}",
      ".cotc-reasonRow{display:flex;align-items:center;gap:12px;margin:6px 0;font-size:12px;line-height:18px}",
      ".cotc-reasonName{width:190px;flex:none;color:var(--dsw-alias-label-secondary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}",
      ".cotc-reasonTrack{flex:1;min-width:40px;height:8px;border-radius:var(--dsw-radius-xs);background:var(--dsw-alias-interactive-bg-hover)}",
      ".cotc-reasonBar{height:8px;border-radius:var(--dsw-radius-xs);background:var(--dsw-alias-label-caption);opacity:.35}",
      ".cotc-reasonCount{width:56px;flex:none;text-align:right;color:var(--dsw-alias-label-secondary);white-space:nowrap}",
      ".cotc-body{display:flex;gap:20px;margin-top:24px;align-items:flex-start}",
      ".cotc-sidebar{width:320px;flex:none;min-width:0;display:flex;flex-direction:column;gap:8px}",
      ".cotc-detail{flex:1;min-width:0}",
      "@media (max-width:900px){.cotc-body{flex-direction:column}.cotc-sidebar{width:100%}}",
      ".cotc-sessionList{border:1px solid var(--dsw-alias-border-l1);border-radius:var(--dsw-radius-md);overflow:hidden}",
      ".cotc-sessionRow{width:100%;text-align:left;background:transparent;border:none;border-bottom:1px solid var(--dsw-alias-border-l1);padding:10px 12px;min-height:56px;cursor:pointer;color:var(--dsw-alias-label-primary);display:flex;flex-direction:column;gap:3px;font-family:var(--dsw-font-family)}",
      ".cotc-sessionRow:last-child{border-bottom:0}",
      ".cotc-sessionRow[data-active=true]{background:var(--dsw-alias-interactive-bg-hover)}",
      ".cotc-sessionTitle{font-size:13px;font-weight:500;line-height:20px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}",
      ".cotc-sessionMeta{display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:12px;line-height:18px;color:var(--dsw-alias-label-caption)}",
      ".cotc-badge{display:inline-block;font-size:11px;line-height:16px;padding:0 6px;border-radius:var(--dsw-radius-xs);background:var(--dsw-alias-interactive-bg-hover);color:var(--dsw-alias-label-secondary)}",
      ".cotc-more{width:100%;height:32px;margin-top:8px;border:1px solid var(--dsw-alias-border-l1);border-radius:var(--dsw-radius-sm);background:transparent;color:var(--dsw-alias-label-secondary);font-family:var(--dsw-font-family);font-size:12px;cursor:pointer}",
      ".cotc-more:hover{background:var(--dsw-alias-interactive-bg-hover)}",
      ".cotc-sessionHeader{display:flex;flex-direction:column;gap:4px;margin-bottom:12px;min-width:0}",
      ".cotc-sessionName{font-size:16px;font-weight:600;line-height:24px;margin:0}",
      ".cotc-sessionFacts{display:flex;align-items:center;gap:10px;flex-wrap:wrap;font-size:12px;line-height:18px;color:var(--dsw-alias-label-caption);min-width:0}",
      ".cotc-sessionId{font-family:var(--dsw-font-family);cursor:pointer;text-decoration:underline dotted;color:var(--dsw-alias-label-secondary);word-break:break-all}",
      ".cotc-parentLink{border:none;background:transparent;padding:0;font-family:var(--dsw-font-family);font-size:12px;line-height:18px;color:var(--dsw-alias-label-secondary);text-decoration:underline dotted;cursor:pointer;word-break:break-all}",
      ".cotc-parentLink:hover{color:var(--dsw-alias-label-primary)}",
      ".cotc-parentStatic{word-break:break-all}",
      ".cotc-chartWrap{border:1px solid var(--dsw-alias-border-l1);border-radius:var(--dsw-radius-md);padding:12px 16px;margin-bottom:16px}",
      ".cotc-chart{width:100%;height:120px;display:block}",
      ".cotc-legend{display:flex;align-items:center;gap:16px;margin-top:6px;font-size:12px;color:var(--dsw-alias-label-caption)}",
      ".cotc-legendItem{display:inline-flex;align-items:center;gap:6px}",
      ".cotc-legendSwatch{width:10px;height:10px;border-radius:var(--dsw-radius-xs);display:inline-block}",
      ".cotc-entryList{border-top:1px solid var(--dsw-alias-border-l1)}",
      ".cotc-entry{border-bottom:1px solid var(--dsw-alias-border-l1);padding:8px 0}",
      ".cotc-entryButton{width:100%;text-align:left;background:transparent;border:none;color:var(--dsw-alias-label-primary);padding:4px 6px;border-radius:var(--dsw-radius-sm);cursor:pointer;font-family:var(--dsw-font-family);display:flex;flex-direction:column;gap:3px}",
      ".cotc-entryButton:hover{background:var(--dsw-alias-interactive-bg-hover)}",
      ".cotc-entryTop{display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:12px;line-height:18px}",
      ".cotc-entryTime{color:var(--dsw-alias-label-secondary);white-space:nowrap}",
      ".cotc-entryTurn{color:var(--dsw-alias-label-primary)}",
      ".cotc-entrySource{color:var(--dsw-alias-label-caption)}",
      ".cotc-entryNums{color:var(--dsw-alias-label-caption);font-size:12px;line-height:18px}",
      ".cotc-resultSuccess{color:var(--dsw-alias-state-success-primary,var(--dsw-alias-label-primary))}",
      ".cotc-resultCaption{color:var(--dsw-alias-label-caption)}",
      ".cotc-resultWarn{color:var(--dsw-alias-state-error-primary,var(--dsw-alias-label-primary))}",
      ".cotc-compare{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:8px 0 4px}",
      ".cotc-failures{grid-column:1/-1;font-size:12px;line-height:18px;color:var(--dsw-alias-label-caption);background:var(--dsw-alias-interactive-bg-hover);border-radius:var(--dsw-radius-sm);padding:6px 10px}",
      ".cotc-compareCol{border:1px solid var(--dsw-alias-border-l1);border-radius:var(--dsw-radius-sm);padding:10px 12px;min-width:0}",
      ".cotc-compareTitle{display:flex;align-items:center;justify-content:space-between;gap:8px;font-size:12px;font-weight:600;line-height:18px;color:var(--dsw-alias-label-secondary);margin-bottom:6px}",
      ".cotc-copy{border:none;background:transparent;color:var(--dsw-alias-label-caption);font-family:var(--dsw-font-family);font-size:12px;cursor:pointer;padding:0 2px}",
      ".cotc-copy:hover{color:var(--dsw-alias-label-primary)}",
      ".cotc-pre{white-space:pre-wrap;font-size:12.5px;line-height:1.6;max-height:480px;overflow:auto;margin:0;color:var(--dsw-alias-label-primary);word-break:break-word}",
      "@media (max-width:700px){.cotc-compare{grid-template-columns:1fr}}",
      ".cotc-loading{font-size:12px;color:var(--dsw-alias-label-caption);padding:8px 0}",
      ".cotc-muted{font-size:12px;color:var(--dsw-alias-label-caption);padding:8px 0}",
      ".cotc-empty{padding:64px 16px;text-align:center;color:var(--dsw-alias-label-caption);font-size:14px;line-height:22px}",
      ".cotc-error{padding:56px 16px;text-align:center;font-size:14px;line-height:22px;color:var(--dsw-alias-label-secondary)}",
      ".cotc-errorText{color:var(--dsw-alias-label-caption);font-size:12px;margin-top:6px;word-break:break-word}",
      ".cotc-retry{height:32px;margin-top:14px;padding:0 14px;border:1px solid var(--dsw-alias-border-l1);border-radius:var(--dsw-radius-sm);background:transparent;color:var(--dsw-alias-label-primary);font-family:var(--dsw-font-family);font-size:13px;cursor:pointer}",
      ".cotc-retry:hover{background:var(--dsw-alias-interactive-bg-hover)}",
      ".cotc-inlineError{font-size:12px;line-height:18px;color:var(--dsw-alias-state-error-primary,var(--dsw-alias-label-primary));margin:0 0 12px}",
    ].join("");

    function injectStyle() {
      if (typeof document === "undefined" || typeof document.createElement !== "function") return;
      var selector = 'style[data-plugin-css="dsh-cot-compress/client.css"]';
      if (document.querySelector(selector) !== null) return;
      var tag = document.createElement("style");
      tag.dataset.plugin = "dsh-cot-compress";
      tag.dataset.pluginCss = "dsh-cot-compress/client.css";
      tag.textContent = CSS;
      document.head.appendChild(tag);
    }

    // ------------------------------------------------------------------
    // 图标
    // ------------------------------------------------------------------

    function CompressGlyph(props) {
      var size = props && isFiniteNumber(props.size) ? props.size : 16;
      var className = props && props.className ? props.className : undefined;
      return jsxs("svg", {
        width: size,
        height: size,
        viewBox: "0 0 16 16",
        fill: "none",
        stroke: "currentColor",
        strokeWidth: 1.5,
        strokeLinecap: "round",
        strokeLinejoin: "round",
        className: className,
        "aria-hidden": "true",
        children: [
          jsx("path", { d: "M6.5 3 3 8l3.5 5" }, "l1"),
          jsx("path", { d: "M3 8h4.5" }, "l2"),
          jsx("path", { d: "M9.5 3 13 8l-3.5 5" }, "r1"),
          jsx("path", { d: "M13 8H8.5" }, "r2"),
        ],
      });
    }

    function CompressSeatIcon(props) {
      var Icon = primitives.IconCompactOutlineRegular;
      var size = props && isFiniteNumber(props.size) ? props.size : 16;
      if (typeof Icon === "function") {
        return jsx(Icon, { size: size, className: props && props.className });
      }
      return jsx(CompressGlyph, { size: size, className: props && props.className });
    }

    function ChevronDown(props) {
      var Icon = primitives.IconChevronDownOutlineRegular;
      if (typeof Icon === "function") {
        return jsx(Icon, { size: 14, className: props && props.className });
      }
      return jsx("svg", {
        width: 14,
        height: 14,
        viewBox: "0 0 16 16",
        fill: "none",
        stroke: "currentColor",
        strokeWidth: 1.5,
        className: props && props.className,
        "aria-hidden": "true",
        children: jsx("path", { d: "M4 6l3.3 3.3a1 1 0 0 0 1.4 0L12 6" }),
      });
    }

    function RefreshIcon() {
      var Icon = primitives.IconRefreshOutlineRegular;
      if (typeof Icon === "function") return jsx(Icon, { size: 16 });
      return jsx("svg", {
        width: 16,
        height: 16,
        viewBox: "0 0 16 16",
        fill: "none",
        stroke: "currentColor",
        strokeWidth: 1.5,
        "aria-hidden": "true",
        children: jsx("path", { d: "M13 8a5 5 0 1 1-1.5-3.6M13 2.5V6h-3.5" }),
      });
    }

    function sidebarIcon(props) {
      var size = props && isFiniteNumber(props.size) ? props.size : 16;
      return jsx(CompressSeatIcon, { size: size });
    }

    // ------------------------------------------------------------------
    // 工具栏开关
    // ------------------------------------------------------------------

    function menuItemLabel(name, description) {
      return jsxs("span", {
        className: "cotc-item",
        children: [
          jsx("span", { className: "cotc-itemName", children: name }),
          jsx("span", { className: "cotc-itemDesc", children: description }),
        ],
      });
    }

    function composerAnchor() {
      if (typeof document === "undefined" || typeof document.querySelector !== "function") return null;
      try {
        return document.querySelector("[data-composer-card]");
      } catch (_error) {
        return null;
      }
    }

    function CompressChip(props) {
      var scope = useScopeStore();
      var openState = React.useState(false);
      var menuOpen = openState[0];
      var setMenuOpen = openState[1];
      var toastState = React.useState(null);
      var toast = toastState[0];
      var setToast = toastState[1];
      var flashState = React.useState(false);
      var failFlash = flashState[0];
      var setFailFlash = flashState[1];
      var rootRef = React.useRef(null);
      var adjustRef = React.useRef(null);
      var flashTimer = React.useRef(null);
      var toastSeq = React.useRef(0);

      var setRoot = React.useCallback(function (node) {
        rootRef.current = node;
        if (node === null) return;
        // 插槽渲染器会在外面再包一层 display:contents 的 div；跳过这类不占位的包装，
        // 找到真正参与排版的容器（带 margin-left:auto 的那个），最多向上 4 层。
        var parent = node.parentElement;
        for (var depth = 0; depth < 4 && parent; depth += 1) {
          var view = parent.ownerDocument && parent.ownerDocument.defaultView;
          var display = view && typeof view.getComputedStyle === "function" ? view.getComputedStyle(parent).display : "";
          if (display !== "contents") break;
          parent = parent.parentElement;
        }
        if (parent === null || parent === undefined) return;
        if (adjustRef.current === null) {
          adjustRef.current = { parent: parent, previous: parent.style.marginLeft };
        }
        // conversation.hero.options 的容器带 margin-left:auto；把开关拉到「标准模式」右边。
        parent.style.marginLeft = "0px";
      }, []);

      React.useEffect(function () {
        return function () {
          var adjust = adjustRef.current;
          if (adjust !== null && adjust !== undefined) {
            adjust.parent.style.marginLeft = adjust.previous;
            adjustRef.current = null;
          }
          if (flashTimer.current !== null) clearTimeout(flashTimer.current);
        };
      }, []);

      React.useEffect(function () {
        ensureScopeLoaded(false);
      }, []);

      var openVisualization = props && typeof props.openVisualization === "function" ? props.openVisualization : null;

      function flashFailure() {
        if (flashTimer.current !== null) clearTimeout(flashTimer.current);
        setFailFlash(true);
        flashTimer.current = setTimeout(function () {
          flashTimer.current = null;
          setFailFlash(false);
        }, 2000);
      }

      function handleSelect(id) {
        setMenuOpen(false);
        if (id === "viz") {
          if (openVisualization !== null) openVisualization();
          return;
        }
        if (SCOPES.indexOf(id) < 0) return;
        changeScope(id).then(function (result) {
          if (result.ok) {
            setToast(null);
            return;
          }
          if (typeof primitives.Toast === "function") {
            toastSeq.current += 1;
            setToast({ seq: toastSeq.current, text: "切换失败：" + result.error });
          } else {
            flashFailure();
          }
        });
      }

      var rangeDisabled = scope.status !== "ready" || scope.saving === true;
      var menuItems = SCOPES.map(function (id) {
        return {
          id: id,
          label: menuItemLabel(SCOPE_MENU[id].name, SCOPE_MENU[id].description),
          disabled: rangeDisabled,
        };
      });
      menuItems.push({ id: "viz-separator", type: "separator" });
      menuItems.push({
        id: "viz",
        label: menuItemLabel("压缩可视化", "查看每段推理压缩前后的对比"),
      });

      var label;
      if (failFlash) label = "切换失败";
      else if (scope.status !== "ready") label = "压缩";
      else label = SCOPE_CHIP_TEXT[scope.scope] || "压缩";

      var seat = jsxs("button", {
        type: "button",
        className: scope.scope === "off" && scope.status === "ready" ? "cotc-seat cotc-seatOff" : "cotc-seat",
        "aria-haspopup": "menu",
        "aria-expanded": menuOpen,
        title: scope.status === "ready"
          ? label
          : (scope.status === "unavailable"
            ? (scope.error !== null ? scope.error : "压缩插件的数据接口不可用")
            : "正在读取压缩范围"),
        onClick: function () {
          setMenuOpen(function (value) {
            return !value;
          });
        },
        children: [
          jsx(CompressSeatIcon, { size: 16, className: "cotc-seatIcon" }),
          jsx("span", { className: "cotc-seatLabel", children: label }),
          jsx(ChevronDown, { className: "cotc-chevron" }),
        ],
      });

      var Menu = primitives.Menu;
      var menuNode;
      if (typeof Menu === "function") {
        menuNode = jsx(Menu, {
          open: menuOpen && menuItems.length > 0,
          onClose: function () {
            setMenuOpen(false);
          },
          items: menuItems,
          selectedId: scope.scope,
          onSelect: handleSelect,
          align: "start",
          portal: true,
          selection: "check",
          className: "cotc-menuAnchor",
          anchor: seat,
        });
      } else {
        menuNode = jsxs("span", {
          className: "cotc-menuAnchor cotc-fallbackAnchor",
          children: [
            seat,
            menuOpen
              ? jsx("div", {
                  className: "cotc-fallbackMenu",
                  role: "menu",
                  children: menuItems.map(function (item) {
                    if (item.type === "separator") {
                      return jsx("div", { className: "cotc-fallbackSeparator", role: "separator" }, item.id);
                    }
                    return jsx("button", {
                      type: "button",
                      role: "menuitem",
                      className: "cotc-fallbackItem",
                      disabled: item.disabled === true,
                      onClick: function () {
                        handleSelect(item.id);
                      },
                      children: item.label,
                    }, item.id);
                  }),
                })
              : null,
          ],
        });
      }

      var toastNode = null;
      if (toast !== null && typeof primitives.Toast === "function") {
        toastNode = jsx(primitives.Toast, {
          text: toast.text,
          holdMs: TOAST_HOLD_MS,
          anchor: composerAnchor(),
          onDone: function () {
            setToast(null);
          },
        }, toast.seq);
      }

      return jsxs("span", {
        ref: setRoot,
        className: "cotc-toolbarRoot",
        children: [menuNode, toastNode],
      });
    }

    // ------------------------------------------------------------------
    // 可视化页：通用小组件
    // ------------------------------------------------------------------

    function KindFilter(props) {
      var options = [
        ["all", "全部"],
        ["lead", "主智能体"],
        ["subagent", "子智能体"],
      ];
      return jsx("div", {
        className: "cotc-filterRow",
        role: "group",
        "aria-label": props.label || "筛选",
        children: options.map(function (option) {
          return jsx("button", {
            type: "button",
            className: "cotc-filterButton",
            "data-active": props.value === option[0] ? "true" : "false",
            "aria-pressed": props.value === option[0],
            onClick: function () {
              props.onChange(option[0]);
            },
            children: option[1],
          }, option[0]);
        }),
      });
    }

    function SummaryCards(props) {
      var cards = props.view && Array.isArray(props.view.cards) ? props.view.cards : [];
      return jsx("div", {
        className: "cotc-cards",
        children: cards.map(function (card) {
          return jsxs("div", {
            className: "cotc-card",
            children: [
              jsx("div", { className: "cotc-cardTitle", children: card.title }),
              jsx("div", { className: "cotc-cardValue", children: card.value }),
              jsx("div", { className: "cotc-cardNote", children: card.note }),
            ],
          }, card.id);
        }),
      });
    }

    function ReasonBars(props) {
      var reasons = Array.isArray(props.reasons) ? props.reasons : [];
      if (reasons.length === 0) return jsx("div", { className: "cotc-muted", children: "暂无" });
      return jsx("div", {
        children: reasons.map(function (row) {
          return jsxs("div", {
            className: "cotc-reasonRow",
            children: [
              jsx("span", { className: "cotc-reasonName", title: row.label, children: row.label }),
              jsx("span", {
                className: "cotc-reasonTrack",
                children: jsx("span", {
                  className: "cotc-reasonBar",
                  style: { display: "block", width: String(row.pct) + "%" },
                }),
              }),
              jsx("span", { className: "cotc-reasonCount", children: formatNumber(row.count) + " 次" }),
            ],
          }, row.reason);
        }),
      });
    }

    function SessionRows(props) {
      var rows = Array.isArray(props.rows) ? props.rows : [];
      if (rows.length === 0) return jsx("div", { className: "cotc-muted", children: "没有会话。" });
      return jsx("div", {
        className: "cotc-sessionList",
        children: rows.map(function (row) {
          return jsxs("button", {
            type: "button",
            className: "cotc-sessionRow",
            "data-active": row.id === props.selectedId ? "true" : "false",
            onClick: function () {
              props.onSelect(row.id);
            },
            children: [
              jsx("span", { className: "cotc-sessionTitle", children: row.title }),
              jsxs("span", {
                className: "cotc-sessionMeta",
                children: [
                  jsx("span", { className: "cotc-badge", title: row.kindTitle || undefined, children: row.kindText }),
                  jsx("span", { children: row.time }),
                  jsx("span", { children: row.replaceText }),
                  jsx("span", { children: row.ratioText }),
                ],
              }),
            ],
          }, row.id);
        }),
      });
    }

    function CopyButton(props) {
      var state = React.useState("复制");
      var text = state[0];
      var setText = state[1];
      var timer = React.useRef(null);
      React.useEffect(function () {
        return function () {
          if (timer.current !== null) clearTimeout(timer.current);
        };
      }, []);
      function copy() {
        var payload = typeof props.text === "string" ? props.text : "";
        var done = function () {
          setText("已复制");
          if (timer.current !== null) clearTimeout(timer.current);
          timer.current = setTimeout(function () {
            timer.current = null;
            setText("复制");
          }, 1500);
        };
        if (typeof navigator !== "undefined" && navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
          navigator.clipboard.writeText(payload).then(done, done);
        } else {
          done();
        }
      }
      return jsx("button", { type: "button", className: "cotc-copy", onClick: copy, children: text });
    }

    function CompareColumn(props) {
      return jsxs("div", {
        className: "cotc-compareCol",
        children: [
          jsxs("div", {
            className: "cotc-compareTitle",
            children: [
              jsx("span", { children: props.title }),
              jsx(CopyButton, { text: props.text }),
            ],
          }),
          jsx("pre", {
            className: "cotc-pre",
            children: typeof props.text === "string" && props.text.length > 0 ? props.text : "（没有内容）",
          }),
        ],
      });
    }

    function EntryCompare(props) {
      var detail = props.detail;
      if (detail === null || detail === undefined) return null;
      if (detail.loading === true) return jsx("div", { className: "cotc-loading", children: "加载中…" });
      if (detail.error !== null && detail.error !== undefined) {
        return jsx("div", { className: "cotc-inlineError", children: "加载失败：" + detail.error });
      }
      var data = detail.data !== null && typeof detail.data === "object" ? detail.data : {};
      var failures = Array.isArray(data.chunkFailures) ? data.chunkFailures : [];
      var children = [];
      if (failures.length > 0) {
        children.push(jsx("div", {
          className: "cotc-failures",
          children: "分块失败：" + failures.map(function (failure) {
            var index = failure && failure.k !== undefined ? String(failure.k) : "?";
            return "第 " + index + " 块（" + reasonLabel(failure && failure.reason) + "）";
          }).join("；"),
        }, "failures"));
      }
      children.push(jsx(CompareColumn, { title: "原文", text: data.orig }, "orig"));
      children.push(jsx(CompareColumn, { title: "整理后", text: data.comp }, "comp"));
      return jsx("div", { className: "cotc-compare", children: children });
    }

    function resultBadge(entry) {
      if (entry.superseded === true) {
        return jsx("span", { className: "cotc-resultCaption", children: "迟到作废" });
      }
      if (entry.ok === true) {
        return jsx("span", { className: "cotc-resultSuccess", children: "已替换" });
      }
      return jsx("span", { className: "cotc-resultCaption", children: "保留原文 · " + reasonLabel(entry.reason) });
    }

    function EntryList(props) {
      var entries = Array.isArray(props.entries) ? props.entries : [];
      if (entries.length === 0) return jsx("div", { className: "cotc-muted", children: "这个会话还没有压缩记录。" });
      return jsxs("div", {
        className: "cotc-entryList",
        children: [
          entries.map(function (entry) {
            var isOpen = props.expandedLine === entry.line;
            var failures = failureCount(entry.chunkFailures);
            var numbers = "字数 " + formatNumber(entry.origChars) + " → " + formatNumber(entry.compChars) +
              " · token " + formatNumber(entry.origTokensEst) + " → " + formatNumber(entry.compTokensEst) +
              " · 用时 " + formatSeconds(entry.compressMs) +
              " · " + formatNumber(numberOr(entry.chunks, 0)) + " 块" +
              (failures > 0 ? "（失败 " + formatNumber(failures) + "）" : "");
            return jsxs("div", {
              className: "cotc-entry",
              children: [
                jsxs("button", {
                  type: "button",
                  className: "cotc-entryButton",
                  "aria-expanded": isOpen,
                  onClick: function () {
                    props.onToggle(entry.line);
                  },
                  children: [
                    jsxs("span", {
                      className: "cotc-entryTop",
                      children: [
                        jsx("span", { className: "cotc-entryTime", children: formatTime(entry.ts) }),
                        jsx("span", { className: "cotc-entryTurn", children: formatTurnLabel(entry.hostTurn, entry.turn) }),
                        jsx("span", { className: "cotc-entrySource", children: sourceLabel(entry.source) }),
                        resultBadge(entry),
                      ],
                    }),
                    jsx("span", { className: "cotc-entryNums", children: numbers }),
                  ],
                }),
                isOpen ? jsx(EntryCompare, { detail: props.detail }) : null,
              ],
            }, String(entry.line));
          }),
          props.cursor !== null && props.cursor !== undefined
            ? jsx("button", {
                type: "button",
                className: "cotc-more",
                onClick: props.onMore,
                disabled: props.loadingMore === true,
                children: props.loadingMore === true ? "加载中…" : "加载更多",
              })
            : null,
        ],
      });
    }

    function UsageChart(props) {
      var items = Array.isArray(props.items) ? props.items : [];
      if (items.length === 0) {
        return jsx("div", { className: "cotc-chartWrap", children: jsx("div", { className: "cotc-muted", children: "还没有请求用量记录。" }) });
      }
      var width = 720;
      var height = 120;
      var padX = 8;
      var base = 100;
      var top = 20;
      var area = base - top;
      var max = 1;
      items.forEach(function (item) {
        var total = numberOr(item.inputTokens, 0) + numberOr(item.cacheReadTokens, 0);
        if (total > max) max = total;
      });
      var slot = (width - padX * 2) / Math.max(items.length, 1);
      var barWidth = Math.max(1, Math.min(8, slot - 1));
      var children = [];
      items.forEach(function (item, index) {
        var input = Math.max(0, numberOr(item.inputTokens, 0));
        var cached = Math.max(0, numberOr(item.cacheReadTokens, 0));
        var total = input + cached;
        var totalHeight = (total / max) * area;
        var cachedHeight = (cached / max) * area;
        var inputHeight = (input / max) * area;
        var x = padX + index * slot + (slot - barWidth) / 2;
        var turnText = isFiniteNumber(item.hostTurn) ? "第 " + formatNumber(item.hostTurn) + " 回合" : "回合未知";
        children.push(jsx("rect", {
          x: x,
          y: base - cachedHeight,
          width: barWidth,
          height: Math.max(0, cachedHeight),
          fill: "var(--dsw-alias-label-secondary)",
          fillOpacity: 0.3,
          children: jsx("title", {
            children: turnText + " · 缓存命中 " + formatNumber(cached) + " · 未命中 " + formatNumber(input),
          }),
        }, "c" + String(index)));
        children.push(jsx("rect", {
          x: x,
          y: base - totalHeight,
          width: barWidth,
          height: Math.max(0, inputHeight),
          fill: "var(--dsw-alias-label-primary)",
          fillOpacity: 1,
          children: jsx("title", {
            children: turnText + " · 缓存命中 " + formatNumber(cached) + " · 未命中 " + formatNumber(input),
          }),
        }, "u" + String(index)));
      });
      items.forEach(function (item, index) {
        if (!isFiniteNumber(item.hostTurn)) return;
        if (index > 0 && items[index - 1].hostTurn === item.hostTurn) return;
        var x = padX + index * slot + slot / 2;
        children.push(jsx("line", {
          x1: x,
          y1: top - 4,
          x2: x,
          y2: base,
          stroke: "var(--dsw-alias-label-caption)",
          strokeWidth: 1,
          strokeDasharray: "3 3",
          strokeOpacity: 0.6,
        }, "m" + String(index)));
        children.push(jsx("text", {
          x: x + 3,
          y: 10,
          fontSize: 10,
          fill: "var(--dsw-alias-label-caption)",
          children: "第 " + formatNumber(item.hostTurn) + " 回合",
        }, "t" + String(index)));
      });
      return jsxs("div", {
        className: "cotc-chartWrap",
        children: [
          jsx("svg", {
            className: "cotc-chart",
            viewBox: "0 0 " + String(width) + " " + String(height),
            role: "img",
            "aria-label": "请求用量图",
            children: children,
          }),
          jsxs("div", {
            className: "cotc-legend",
            children: [
              jsxs("span", {
                className: "cotc-legendItem",
                children: [
                  jsx("span", {
                    className: "cotc-legendSwatch",
                    style: { background: "var(--dsw-alias-label-secondary)", opacity: 0.3 },
                  }),
                  jsx("span", { children: "缓存命中" }),
                ],
              }),
              jsxs("span", {
                className: "cotc-legendItem",
                children: [
                  jsx("span", {
                    className: "cotc-legendSwatch",
                    style: { background: "var(--dsw-alias-label-primary)", opacity: 1 },
                  }),
                  jsx("span", { children: "未命中" }),
                ],
              }),
            ],
          }),
        ],
      });
    }

    function SessionDetailHeader(props) {
      var row = props.row;
      var copyState = React.useState(false);
      var copied = copyState[0];
      var setCopied = copyState[1];
      if (row === null) return null;
      function copyId() {
        var done = function () {
          setCopied(true);
          setTimeout(function () {
            setCopied(false);
          }, 1500);
        };
        if (typeof navigator !== "undefined" && navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
          navigator.clipboard.writeText(row.id).then(done, done);
        } else {
          done();
        }
      }
      return jsxs("div", {
        className: "cotc-sessionHeader",
        children: [
          jsx("h2", { className: "cotc-sessionName", children: row.title }),
          jsxs("div", {
            className: "cotc-sessionFacts",
            children: [
              jsx("span", { className: "cotc-badge", children: row.kindText }),
              jsx("span", {
                className: "cotc-sessionId",
                title: "点击复制完整会话 ID",
                onClick: copyId,
                children: copied ? "已复制：" + row.id : row.id,
              }),
              jsx("span", { children: row.cwd !== null ? row.cwd : "工作目录未知" }),
              row.parentLabel !== null
                ? (props.parentTargetId !== null
                    ? jsx("button", {
                        type: "button",
                        className: "cotc-parentLink",
                        title: "切换到该会话",
                        onClick: function () {
                          props.onSelectSession(props.parentTargetId);
                        },
                        children: row.parentLabel,
                      })
                    : jsx("span", { className: "cotc-parentStatic", children: row.parentLabel }))
                : null,
            ],
          }),
        ],
      });
    }

    // ------------------------------------------------------------------
    // 可视化页
    // ------------------------------------------------------------------

    function CotCompressPage() {
      var scope = useScopeStore();
      var summaryKindState = React.useState("all");
      var summaryKind = summaryKindState[0];
      var setSummaryKind = summaryKindState[1];
      var sessionKindState = React.useState("all");
      var sessionKind = sessionKindState[0];
      var setSessionKind = sessionKindState[1];
      var summaryState = React.useState(null);
      var summary = summaryState[0];
      var setSummary = summaryState[1];
      var sessionsState = React.useState([]);
      var sessions = sessionsState[0];
      var setSessions = sessionsState[1];
      var sessionsCursorState = React.useState(null);
      var sessionsCursor = sessionsCursorState[0];
      var setSessionsCursor = sessionsCursorState[1];
      var selectedIdState = React.useState(null);
      var selectedId = selectedIdState[0];
      var setSelectedId = selectedIdState[1];
      var entriesState = React.useState([]);
      var entries = entriesState[0];
      var setEntries = entriesState[1];
      var entriesCursorState = React.useState(null);
      var entriesCursor = entriesCursorState[0];
      var setEntriesCursor = entriesCursorState[1];
      var usageState = React.useState([]);
      var usage = usageState[0];
      var setUsage = usageState[1];
      var expandedState = React.useState(null);
      var expandedLine = expandedState[0];
      var setExpandedLine = expandedState[1];
      var detailState = React.useState(null);
      var detail = detailState[0];
      var setDetail = detailState[1];
      var errorState = React.useState(null);
      var error = errorState[0];
      var setError = errorState[1];
      var loadingState = React.useState(true);
      var loading = loadingState[0];
      var setLoading = loadingState[1];
      var updatedState = React.useState(null);
      var updatedAt = updatedState[0];
      var setUpdatedAt = updatedState[1];
      var reloadState = React.useState(0);
      var reloadSeq = reloadState[0];
      var setReloadSeq = reloadState[1];
      var scopeErrorState = React.useState(null);
      var scopeError = scopeErrorState[0];
      var setScopeError = scopeErrorState[1];
      var moreSessionsState = React.useState(false);
      var loadingMoreSessions = moreSessionsState[0];
      var setLoadingMoreSessions = moreSessionsState[1];
      var moreEntriesState = React.useState(false);
      var loadingMoreEntries = moreEntriesState[0];
      var setLoadingMoreEntries = moreEntriesState[1];

      var entriesRef = React.useRef(entries);
      entriesRef.current = entries;
      var selectedRef = React.useRef(selectedId);
      selectedRef.current = selectedId;
      var kindRef = React.useRef({ summaryKind: summaryKind, sessionKind: sessionKind });
      kindRef.current = { summaryKind: summaryKind, sessionKind: sessionKind };
      var refreshRef = React.useRef(null);

      React.useEffect(function () {
        ensureScopeLoaded(false);
      }, []);

      React.useEffect(function () {
        var cancelled = false;
        setLoading(true);
        Promise.all([getSummary(summaryKind), getSessions(sessionKind, null)]).then(function (result) {
          if (cancelled) return;
          setSummary(result[0] || null);
          setSessions(Array.isArray(result[1] && result[1].items) ? result[1].items : []);
          setSessionsCursor(result[1] ? (result[1].nextCursor === undefined ? null : result[1].nextCursor) : null);
          setUpdatedAt(Date.now());
          setError(null);
        }, function (failure) {
          if (!cancelled) setError(errorMessage(failure));
        }).then(function () {
          if (!cancelled) setLoading(false);
        });
        return function () {
          cancelled = true;
        };
      }, [summaryKind, sessionKind, reloadSeq]);

      React.useEffect(function () {
        if (sessions.length === 0) {
          if (selectedRef.current !== null) setSelectedId(null);
          return;
        }
        var exists = selectedRef.current !== null && sessions.some(function (item) {
          return item.sessionId === selectedRef.current;
        });
        if (!exists) setSelectedId(sessions[0].sessionId);
      }, [sessions]);

      React.useEffect(function () {
        if (selectedId === null || selectedId === undefined) {
          setEntries([]);
          setEntriesCursor(null);
          setUsage([]);
          setExpandedLine(null);
          setDetail(null);
          return undefined;
        }
        var cancelled = false;
        setExpandedLine(null);
        setDetail(null);
        Promise.all([getEntries(selectedId, null), getUsage(selectedId)]).then(function (result) {
          if (cancelled) return;
          setEntries(Array.isArray(result[0] && result[0].items) ? result[0].items : []);
          setEntriesCursor(result[0] ? (result[0].nextCursor === undefined ? null : result[0].nextCursor) : null);
          setUsage(Array.isArray(result[1] && result[1].items) ? result[1].items : []);
          setError(null);
        }, function (failure) {
          if (!cancelled) setError(errorMessage(failure));
        });
        return function () {
          cancelled = true;
        };
      }, [selectedId, reloadSeq]);

      refreshRef.current = function refreshQuiet() {
        var kinds = kindRef.current;
        Promise.all([getSummary(kinds.summaryKind), getSessions(kinds.sessionKind, null)]).then(function (result) {
          setSummary(result[0] || null);
          setSessions(Array.isArray(result[1] && result[1].items) ? result[1].items : []);
          setSessionsCursor(result[1] ? (result[1].nextCursor === undefined ? null : result[1].nextCursor) : null);
          setUpdatedAt(Date.now());
          setError(null);
          var current = selectedRef.current;
          if (current !== null && current !== undefined) {
            getUsage(current).then(function (body) {
              setUsage(Array.isArray(body && body.items) ? body.items : []);
            }, function () {
              // 轮询失败不覆盖已有内容。
            });
            if (entriesRef.current.length <= PAGE_SIZE) {
              getEntries(current, null).then(function (body) {
                setEntries(Array.isArray(body && body.items) ? body.items : []);
                setEntriesCursor(body ? (body.nextCursor === undefined ? null : body.nextCursor) : null);
              }, function () {
                // 同上。
              });
            }
          }
        }, function (failure) {
          setError(errorMessage(failure));
        });
      };

      React.useEffect(function () {
        var timer = setInterval(function () {
          if (typeof document !== "undefined" && document.visibilityState === "hidden") return;
          if (typeof refreshRef.current === "function") refreshRef.current();
        }, POLL_MS);
        function onVisibilityChange() {
          if (typeof document !== "undefined" && document.visibilityState === "visible") {
            if (typeof refreshRef.current === "function") refreshRef.current();
          }
        }
        if (typeof document !== "undefined") document.addEventListener("visibilitychange", onVisibilityChange);
        return function () {
          clearInterval(timer);
          if (typeof document !== "undefined") document.removeEventListener("visibilitychange", onVisibilityChange);
        };
      }, []);

      React.useEffect(function () {
        if (scopeError === null) return undefined;
        var timer = setTimeout(function () {
          setScopeError(null);
        }, 3000);
        return function () {
          clearTimeout(timer);
        };
      }, [scopeError]);

      function retry() {
        setError(null);
        ensureScopeLoaded(true);
        setReloadSeq(function (value) {
          return value + 1;
        });
      }

      function refresh() {
        ensureScopeLoaded(true);
        setReloadSeq(function (value) {
          return value + 1;
        });
      }

      function changeScopeFromPage(next) {
        changeScope(next).then(function (result) {
          if (!result.ok) setScopeError("切换失败：" + result.error);
          else setScopeError(null);
        });
      }

      function toggleEntry(line) {
        if (expandedLine === line) {
          setExpandedLine(null);
          setDetail(null);
          return;
        }
        setExpandedLine(line);
        setDetail({ loading: true, error: null, data: null });
        getEntry(line).then(function (body) {
          setDetail({ loading: false, error: null, data: body || null });
        }, function (failure) {
          setDetail({ loading: false, error: errorMessage(failure), data: null });
        });
      }

      function loadMoreSessions() {
        if (sessionsCursor === null || loadingMoreSessions) return;
        setLoadingMoreSessions(true);
        getSessions(sessionKind, sessionsCursor).then(function (body) {
          var items = Array.isArray(body && body.items) ? body.items : [];
          setSessions(function (previous) {
            var seen = new Set(previous.map(function (item) {
              return item.sessionId;
            }));
            var merged = previous.slice();
            items.forEach(function (item) {
              if (!seen.has(item.sessionId)) {
                seen.add(item.sessionId);
                merged.push(item);
              }
            });
            return merged;
          });
          setSessionsCursor(body ? (body.nextCursor === undefined ? null : body.nextCursor) : null);
          setLoadingMoreSessions(false);
        }, function (failure) {
          setLoadingMoreSessions(false);
          setScopeError("加载失败：" + errorMessage(failure));
        });
      }

      function loadMoreEntries() {
        if (entriesCursor === null || selectedId === null || loadingMoreEntries) return;
        setLoadingMoreEntries(true);
        getEntries(selectedId, entriesCursor).then(function (body) {
          var items = Array.isArray(body && body.items) ? body.items : [];
          setEntries(function (previous) {
            return previous.concat(items);
          });
          setEntriesCursor(body ? (body.nextCursor === undefined ? null : body.nextCursor) : null);
          setLoadingMoreEntries(false);
        }, function (failure) {
          setLoadingMoreEntries(false);
          setScopeError("加载失败：" + errorMessage(failure));
        });
      }

      var view = buildSummaryView(summary);
      var rows = buildSessionList(sessions);
      var selectedRow = null;
      for (var index = 0; index < rows.length; index += 1) {
        if (rows[index].id === selectedId) {
          selectedRow = rows[index];
          break;
        }
      }
      if (selectedRow === null && selectedId !== null && selectedId !== undefined) {
        selectedRow = buildSessionList([{ sessionId: selectedId }])[0];
      }
      var parentTargetId = selectedRow === null ? null : matchSessionId(
        selectedRow.parentSessionId,
        sessions.map(function (item) {
          return item.sessionId;
        }),
      );
      var attempted = summary !== null && typeof summary === "object" ? numberOr(summary.attempted, 0) : 0;
      var isEmpty = summary !== null && attempted === 0 && sessions.length === 0;
      var unavailable = scope.status === "unavailable" || error !== null;

      var Seg = primitives.SegmentedControl;
      var scopeControl;
      if (typeof Seg === "function") {
        scopeControl = jsx(Seg, {
          id: "cotc-scope",
          value: scope.scope,
          options: [
            { value: "lead", label: "主智能体" },
            { value: "subagent", label: "子智能体" },
            { value: "all", label: "全部" },
            { value: "off", label: "关闭" },
          ],
          onChange: changeScopeFromPage,
          label: "压缩范围",
          disabled: scope.status !== "ready" || scope.saving === true,
        });
      } else {
        scopeControl = jsx("div", {
          className: "cotc-filterRow",
          role: "group",
          "aria-label": "压缩范围",
          children: [
            ["lead", "主智能体"],
            ["subagent", "子智能体"],
            ["all", "全部"],
            ["off", "关闭"],
          ].map(function (option) {
            return jsx("button", {
              type: "button",
              className: "cotc-filterButton",
              "data-active": scope.scope === option[0] ? "true" : "false",
              disabled: scope.status !== "ready" || scope.saving === true,
              onClick: function () {
                changeScopeFromPage(option[0]);
              },
              children: option[1],
            }, option[0]);
          }),
        });
      }

      var scopeGroup = jsxs("div", {
        className: "cotc-controlGroup",
        children: [
          jsx("span", { className: "cotc-controlLabel", children: "压缩对象" }),
          jsx("span", {
            className: "cotc-controlWrap",
            title: "只对之后新产生的推理生效；与输入框上方的开关是同一个设置",
            children: scopeControl,
          }),
        ],
      });

      var detailBody;
      if (selectedId === null || selectedId === undefined) {
        detailBody = jsx("div", { className: "cotc-muted", children: "选择左侧会话查看详情。" });
      } else {
        detailBody = jsxs("div", {
          children: [
            jsx(SessionDetailHeader, {
              row: selectedRow,
              parentTargetId: parentTargetId,
              onSelectSession: setSelectedId,
            }),
            jsx(UsageChart, { items: usage }),
            jsx("h3", { className: "cotc-sectionTitle", children: "压缩时间线" }),
            jsx(EntryList, {
              entries: entries,
              expandedLine: expandedLine,
              detail: detail,
              onToggle: toggleEntry,
              cursor: entriesCursor,
              onMore: loadMoreEntries,
              loadingMore: loadingMoreEntries,
            }),
          ],
        });
      }

      var content;
      if (unavailable) {
        content = jsxs("div", {
          className: "cotc-error",
          children: [
            jsx("div", { children: "读不到压缩插件的数据接口（插件后端可能还没重新加载）。" }),
            error !== null ? jsx("div", { className: "cotc-errorText", children: error }) : null,
            jsx("button", { type: "button", className: "cotc-retry", onClick: retry, children: "重试" }),
          ],
        });
      } else if (loading === true && summary === null) {
        content = jsx("div", { className: "cotc-loading", children: "加载中…" });
      } else if (isEmpty) {
        content = jsx("div", {
          className: "cotc-empty",
          children: "还没有压缩记录。推理超过 5000 字的回合结束后，会出现在这里。",
        });
      } else {
        content = jsxs(React.Fragment, {
          children: [
            jsxs("div", {
              className: "cotc-filterWithLabel",
              children: [
                jsx("span", { className: "cotc-controlLabel", children: "统计" }),
                jsx(KindFilter, { value: summaryKind, onChange: setSummaryKind, label: "统计范围" }),
              ],
            }),
            jsx(SummaryCards, { view: view }),
            jsxs("div", {
              className: "cotc-section",
              children: [
                jsx("h2", { className: "cotc-sectionTitle", children: "保留原文的原因" }),
                jsx(ReasonBars, { reasons: view.reasons }),
              ],
            }),
            jsxs("div", {
              className: "cotc-body",
              children: [
                jsxs("div", {
                  className: "cotc-sidebar",
                  children: [
                    jsxs("div", {
                      className: "cotc-filterWithLabel",
                      children: [
                        jsx("span", { className: "cotc-controlLabel", children: "查看" }),
                        jsx(KindFilter, { value: sessionKind, onChange: setSessionKind, label: "会话范围" }),
                      ],
                    }),
                    jsx(SessionRows, { rows: rows, selectedId: selectedId, onSelect: setSelectedId }),
                    sessionsCursor !== null && sessionsCursor !== undefined
                      ? jsx("button", {
                          type: "button",
                          className: "cotc-more",
                          onClick: loadMoreSessions,
                          disabled: loadingMoreSessions,
                          children: loadingMoreSessions ? "加载中…" : "加载更多",
                        })
                      : null,
                  ],
                }),
                jsx("div", { className: "cotc-detail", children: detailBody }),
              ],
            }),
          ],
        });
      }

      return jsx("div", {
        className: "cotc-page",
        children: jsxs("div", {
          className: "cotc-pageInner",
          children: [
            jsxs("div", {
              className: "cotc-titleRow",
              children: [
                jsx("h1", { className: "cotc-title", children: "推理压缩" }),
                jsxs("div", {
                  className: "cotc-titleActions",
                  children: [
                    scopeGroup,
                    jsx("button", {
                      type: "button",
                      className: "cotc-iconButton",
                      "aria-label": "刷新",
                      title: "刷新",
                      onClick: refresh,
                      children: jsx(RefreshIcon, {}),
                    }),
                    jsx("span", {
                      className: "cotc-updated",
                      children: updatedAt === null ? "尚未更新" : "更新于 " + formatClock(updatedAt),
                    }),
                  ],
                }),
              ],
            }),
            scopeError !== null ? jsx("div", { className: "cotc-inlineError", children: scopeError }) : null,
            content,
          ],
        }),
      });
    }

    // ------------------------------------------------------------------
    // 注册
    // ------------------------------------------------------------------

    function apply(ctx) {
      injectStyle();
      var layout = ctx && ctx.layout ? ctx.layout : null;
      function openVisualization() {
        if (layout !== null && typeof layout.selectPanel === "function") {
          try {
            layout.selectPanel(PANEL_ID);
          } catch (_error) {
            // 打不开页面时保持当前界面。
          }
        }
      }

      ctx.slots.inject("conversation.hero.options", function () {
        return ctx.slots.register({
          name: "conversation.hero.options",
          id: PANEL_ID,
          order: 10,
          inject: function () {
            return { openVisualization: openVisualization };
          },
        }, CompressChip);
      });

      ctx.slots.inject("sidebar.panellist", function () {
        return ctx.slots.register({
          name: "sidebar.panellist",
          id: PANEL_ID,
          order: 20,
          label: function () {
            return "推理压缩";
          },
        }, sidebarIcon);
      });

      ctx.slots.inject("main", function () {
        return ctx.slots.register({
          name: "main",
          key: PANEL_ID,
        }, CotCompressPage);
      });
    }

    exports.apply = apply;
    exports.inject = ["slots", "layout"];
    exports.__test = {
      reasonLabel: reasonLabel,
      normalizeScope: normalizeScope,
      formatNumber: formatNumber,
      formatSeconds: formatSeconds,
      formatTime: formatTime,
      formatClock: formatClock,
      formatRatio: formatRatio,
      kindLabel: kindLabel,
      kindName: kindName,
      parentSessionLabel: parentSessionLabel,
      matchSessionId: matchSessionId,
      sourceLabel: sourceLabel,
      formatTurnLabel: formatTurnLabel,
      classifyLogLine: classifyLogLine,
      buildSummaryView: buildSummaryView,
      buildSessionList: buildSessionList,
      REASON_TEXT: REASON_TEXT,
      UNKNOWN_KIND_TITLE: UNKNOWN_KIND_TITLE,
      SCOPE_CHIP_TEXT: SCOPE_CHIP_TEXT,
      SCOPE_MENU: SCOPE_MENU,
    };

    return module.exports;
  },
});
