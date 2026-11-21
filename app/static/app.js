(function () {
  "use strict";

  const panel = document.getElementById("drill-panel");
  if (!panel) return;

  const drillId = panel.dataset.drillId;
  const initialStatus = panel.dataset.status;
  const TERMINAL = ["recovered", "not_recovered", "ended", "error"];
  let stopped = TERMINAL.includes(initialStatus);

  function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function render(drill) {
    const holder = drill.holder;
    const waiter = drill.waiter;
    setText("holder-state", holder ? holder.state : "已结束");
    setText("waiter-state", waiter ? waiter.state : "已结束");

    const waitEl = document.getElementById("waiter-wait");
    if (waitEl) {
      if (waiter && waiter.wait_event_type) {
        waitEl.innerHTML =
          '<span class="tag tag-warn">' +
          waiter.wait_event_type + " / " + waiter.wait_event +
          "</span>";
      } else if (waiter) {
        waitEl.textContent = "不在等待";
      } else {
        waitEl.textContent = "—";
      }
    }

    const blockersEl = document.getElementById("waiter-blockers");
    if (blockersEl) {
      blockersEl.textContent = (drill.blockers || []).length
        ? drill.blockers.map(function (b) { return b.pid; }).join(", ")
        : "无";
    }

    const confirmedEl = document.getElementById("blocking-confirmed");
    if (confirmedEl) {
      if (drill.blocking_confirmed) {
        confirmedEl.innerHTML =
          '<span class="tag tag-ok">是（' + drill.identity.holder.pid +
          " 正在阻塞 " + drill.identity.waiter.pid + "）</span>";
      } else {
        confirmedEl.innerHTML = '<span class="tag tag-bad">未观察到</span>';
      }
    }

    const badge = document.getElementById("status-badge");
    if (badge) {
      badge.className = "status status-" + drill.status;
      badge.textContent = drill.status;
    }
  }

  async function poll() {
    if (stopped) return;
    try {
      const res = await fetch(
        "/api/state?drill_id=" + encodeURIComponent(drillId),
        { headers: { Accept: "application/json" }, cache: "no-store" }
      );
      if (!res.ok) {
        stopped = true;
        return;
      }
      const data = await res.json();
      const drill = data.drill;
      if (!drill) {
        // The server no longer knows this drill (e.g. restart).
        stopped = true;
        const wrap = document.createElement("div");
        wrap.className = "banner banner-error";
        wrap.textContent =
          "该演练记录已不存在（服务可能已重启），本页面不再可操作。";
        panel.prepend(wrap);
        return;
      }
      if (drill.status !== panel.dataset.status) {
        // Status transitions change which controls are valid: reload so the
        // page never shows an action the server would reject.
        stopped = true;
        window.location.reload();
        return;
      }
      render(drill);
    } catch (err) {
      /* transient network error: keep polling */
    }
    if (!stopped) setTimeout(poll, 1500);
  }

  if (!stopped) setTimeout(poll, 1000);
})();
