const METRIC_META = [
  { key: "discussion_total", label: "Discussion", subtitle: "Daily Discussion Volume", accent: "accent-entertainment" },
  { key: "engagement_total", label: "Engagement", subtitle: "Daily Engagement Total", accent: "accent-experience" },
  { key: "unique_authors", label: "Unique Authors", subtitle: "Daily Unique Authors", accent: "accent-shopping" },
  { key: "velocity", label: "Velocity", subtitle: "Daily Post Volume", accent: "accent-accommodation" },
];
const API_BASE = "/api/full-web-heat-analysis";
const HEAT_ROUTE = "/full-web-heat-analysis";
const MARKET_ROUTE = "/operation_panel.html";
const LOGIN_ROUTE = "/login_page.html";
const SESSION_KEYS = ["first_name", "last_name", "email", "position", "role", "token"];

const PLATFORM_LABELS = {
  fb: "Facebook",
  wb: "Weibo",
};

const STATUS_META = {
  completed: {
    label: "Completed",
    className: "status-completed",
    detail: "This snapshot already has analyzed clusters.",
  },
  to_be_analyzed: {
    label: "To Be Analyzed",
    className: "status-updated",
    detail: "Raw posts already exist, but cluster analysis still needs to run.",
  },
  to_be_updated: {
    label: "To Be Updated",
    className: "status-available",
    detail: "No raw posts exist in this window yet.",
  },
  future: {
    label: "Future",
    className: "status-future",
    detail: "Future windows cannot be selected yet.",
  },
};

const elements = {
  trendDbPathLabel: document.getElementById("trendDbPathLabel"),
  trendPlatformSelect: document.getElementById("trendPlatformSelect"),
  trendPlatformChoiceInputs: Array.from(document.querySelectorAll('input[name="trendPlatformChoice"]')),
  trendWindowModeSelect: document.getElementById("trendWindowModeSelect"),
  trendWindowModeButtons: Array.from(document.querySelectorAll("#trendWindowModeSegmentedControl .filter-segment-button")),
  trendWindowEyebrow: document.getElementById("trendWindowEyebrow"),
  trendWeekLabel: document.getElementById("trendWeekLabel"),
  trendWeekSubLabel: document.getElementById("trendWeekSubLabel"),
  trendSnapshotFilterLabel: document.getElementById("trendSnapshotFilterLabel"),
  trendSnapshotFilterCopy: document.getElementById("trendSnapshotFilterCopy"),
  openTrendCalendarButton: document.getElementById("openTrendCalendarButton"),
  trendSnapshotWindowList: document.getElementById("trendSnapshotWindowList"),
  trendEventSelect: document.getElementById("trendEventSelect"),
  trendIndicatorGrid: document.getElementById("trendIndicatorGrid"),
  trendHeatFocusCard: document.getElementById("trendHeatFocusCard"),
  chartGrid: document.getElementById("chartGrid"),
  backToLeaderboardLink: document.getElementById("backToLeaderboardLink"),
  trendBackToMarketLink: document.getElementById("trendBackToMarketLink"),
  trendPageTitle: document.getElementById("trendPageTitle"),
  trendPageSubtitlePrefix: document.getElementById("trendPageSubtitlePrefix"),
  trendBusyOverlay: document.getElementById("trendBusyOverlay"),
  trendBusyEmoji: document.getElementById("trendBusyEmoji"),
  trendBusyTitle: document.getElementById("trendBusyTitle"),
  trendBusyDetail: document.getElementById("trendBusyDetail"),
  trendControlBusyOverlay: document.getElementById("trendControlBusyOverlay"),
  trendControlBusyEmoji: document.getElementById("trendControlBusyEmoji"),
  trendControlBusyTitle: document.getElementById("trendControlBusyTitle"),
  trendControlBusyDetail: document.getElementById("trendControlBusyDetail"),
  openIndicatorGuideButton: document.getElementById("openIndicatorGuideButton"),
  indicatorGuideModal: document.getElementById("indicatorGuideModal"),
  indicatorGuideBackdrop: document.getElementById("indicatorGuideBackdrop"),
  closeIndicatorGuideButton: document.getElementById("closeIndicatorGuideButton"),
  trendCalendarModal: document.getElementById("trendCalendarModal"),
  trendCalendarBackdrop: document.getElementById("trendCalendarBackdrop"),
  closeTrendCalendarButton: document.getElementById("closeTrendCalendarButton"),
  trendCalendarMonthLabel: document.getElementById("trendCalendarMonthLabel"),
  trendCalendarGrid: document.getElementById("trendCalendarGrid"),
  trendCalendarEyebrow: document.getElementById("trendCalendarEyebrow"),
  trendCalendarTitle: document.getElementById("trendCalendarTitle"),
  trendCalendarHelper: document.getElementById("trendCalendarHelper"),
  trendCalendarSelectionLabel: document.getElementById("trendCalendarSelectionLabel"),
  trendCalendarSelectionDetail: document.getElementById("trendCalendarSelectionDetail"),
  trendCalendarLegend: document.getElementById("trendCalendarLegend"),
  confirmTrendCalendarButton: document.getElementById("confirmTrendCalendarButton"),
};

const state = {
  selectedEvent: "",
  leaderboard: [],
  platform: "wb",
  windowMode: "monthly",
  useInitialUrlEvent: true,
  windows: [],
  calendarSelectedWindow: null,
  currentWeek: {
    week_start: "",
    week_end: "",
    month_key: "",
    quarter_key: "",
    status: "",
  },
  overviewCache: {},
  quarterlyMessage: "",
};

const QUARTERLY_PENDING_COPY =
  "Quarterly reporting aggregates the monthly snapshots already available inside that quarter.";

function syncPlatformControls() {
  elements.trendPlatformSelect.value = state.platform;
  elements.trendPlatformChoiceInputs.forEach((input) => {
    input.checked = input.value === state.platform;
  });
}

function syncWindowModeControls() {
  elements.trendWindowModeSelect.value = state.windowMode;
  elements.trendWindowModeButtons.forEach((button) => {
    const isActive = button.dataset.windowMode === state.windowMode;
    button.classList.toggle("active", isActive);
    button.classList.toggle("secondary", !isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

async function requestJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return response.json();
}

function getSessionParams() {
  const url = new URL(window.location.href);
  const params = new URLSearchParams();
  for (const key of SESSION_KEYS) {
    const value = url.searchParams.get(key);
    if (value) {
      params.set(key, value);
    }
  }
  return params;
}

function requireSession() {
  const sessionParams = getSessionParams();
  if (!sessionParams.get("first_name")) {
    window.location.href = LOGIN_ROUTE;
    throw new Error("Login session missing.");
  }
  return sessionParams;
}

function buildUrlWithSession(pathname, extra = {}) {
  const url = new URL(pathname, window.location.origin);
  const params = getSessionParams();
  params.forEach((value, key) => {
    url.searchParams.set(key, value);
  });
  Object.entries(extra).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  });
  return `${url.pathname}${url.search}`;
}

function clearNode(node) {
  if (!node) {
    return;
  }
  while (node.firstChild) {
    node.removeChild(node.firstChild);
  }
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value || 0);
}

function formatScore(value) {
  return Number(value || 0).toFixed(1);
}

function formatAxisDateLabel(value) {
  if (!value) {
    return "";
  }
  const [, month, day] = String(value).split("-");
  if (!month || !day) {
    return value;
  }
  if (isMonthlyMode()) {
    return `${month}/${day}`;
  }
  return `${Number(month)}/${Number(day)}`;
}

function getAxisLabelIndexes(length) {
  if (length <= 0) {
    return new Set();
  }
  const targetTickCount = isMonthlyMode() ? 6 : 7;
  const step = Math.max(1, Math.ceil(length / targetTickCount));
  const indexes = new Set([0, length - 1]);
  for (let index = 0; index < length; index += step) {
    indexes.add(index);
  }
  return indexes;
}

function buildLinePath(points) {
  if (!points.length) {
    return "";
  }
  return points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
}

function buildAreaPath(points, baselineY) {
  if (!points.length) {
    return "";
  }
  return [
    `M ${points[0].x} ${baselineY}`,
    ...points.map((point) => `L ${point.x} ${point.y}`),
    `L ${points[points.length - 1].x} ${baselineY}`,
    "Z",
  ].join(" ");
}

function clipOptionLabel(value, maxLength = 34) {
  const text = String(value || "").trim();
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 3)}...`;
}

function getClusterDisplayText(item, fallback = "") {
  return String(item?.cluster_key_display || item?.cluster_key || fallback).trim();
}

function shouldHideFutureWeeklyWindow(item) {
  return !isMonthlyMode() && !isQuarterlyMode() && Boolean(item?.status === "future" || item?.is_future);
}

function isMonthlyMode() {
  return state.windowMode === "monthly";
}

function isQuarterlyMode() {
  return state.windowMode === "quarterly";
}

function resolveBusyEmoji(title = "", detail = "") {
  const text = `${title} ${detail}`.toLowerCase();
  if (text.includes("platform")) return "📡";
  if (text.includes("date range") || text.includes("month") || text.includes("week") || text.includes("quarter")) return "🗓️";
  if (text.includes("event")) return "📈";
  if (text.includes("analysis")) return "🔥";
  return "⏳";
}

function setPanelBusy(isBusy, title = "Data is loading...", detail = "Please wait while the trend charts refresh.") {
  elements.trendBusyOverlay?.classList.toggle("hidden", !isBusy);
  elements.trendBusyOverlay?.setAttribute("aria-hidden", isBusy ? "false" : "true");
  elements.trendControlBusyOverlay?.classList.toggle("hidden", !isBusy);
  elements.trendControlBusyOverlay?.setAttribute("aria-hidden", isBusy ? "false" : "true");
  const busyEmoji = resolveBusyEmoji(title, detail);
  if (elements.trendBusyTitle) {
    elements.trendBusyTitle.textContent = title;
  }
  if (elements.trendBusyDetail) {
    elements.trendBusyDetail.textContent = detail;
  }
  if (elements.trendBusyEmoji) {
    elements.trendBusyEmoji.textContent = busyEmoji;
  }
  if (elements.trendControlBusyTitle) {
    elements.trendControlBusyTitle.textContent = isBusy ? "Responding..." : "Ready";
  }
  if (elements.trendControlBusyDetail) {
    elements.trendControlBusyDetail.textContent = detail;
  }
  if (elements.trendControlBusyEmoji) {
    elements.trendControlBusyEmoji.textContent = busyEmoji;
  }
}

function showInteractionBusy(title, detail) {
  setPanelBusy(true, title, detail);
}

function getUrlState() {
  const url = new URL(window.location.href);
  return {
    event: url.searchParams.get("event") || "",
    platform: url.searchParams.get("platform") || "wb",
    windowMode: url.searchParams.get("window_mode") || "monthly",
    quarterKey: url.searchParams.get("quarter_key") || "",
    weekStart: url.searchParams.get("week_start") || "",
    weekEnd: url.searchParams.get("week_end") || "",
    monthKey: url.searchParams.get("month_key") || "",
  };
}

function formatWeekLabel(weekStart, weekEnd) {
  if (!weekStart || !weekEnd) {
    return "No weekly snapshot selected";
  }
  return `${weekStart} to ${weekEnd}`;
}

function formatMonthLabel(monthKey) {
  if (!monthKey) {
    return "No monthly snapshot selected";
  }
  return monthKey;
}

function formatMonthHeading(monthKey) {
  const [year, month] = String(monthKey || "").split("-");
  if (!year || !month) {
    return monthKey || "Unknown month";
  }
  const dateValue = new Date(Number(year), Number(month) - 1, 1);
  return dateValue.toLocaleDateString("en-US", { month: "long", year: "numeric" });
}

function formatSelectedWindowLabel(windowValue) {
  if (!windowValue) {
    if (isQuarterlyMode()) {
      return "No quarter selected";
    }
    return isMonthlyMode() ? "No month selected" : "No week selected";
  }
  if (isQuarterlyMode()) {
    return windowValue.quarter_key || "No quarter selected";
  }
  return isMonthlyMode()
    ? formatMonthLabel(windowValue.month_key)
    : formatWeekLabel(windowValue.week_start, windowValue.week_end);
}

function monthOverlapsWindow(monthKey, week) {
  const [year, month] = String(monthKey || "").split("-");
  const monthStart = new Date(Number(year), Number(month) - 1, 1);
  const monthEnd = new Date(Number(year), Number(month), 0);
  const weekStart = new Date(`${week.week_start}T00:00:00`);
  const weekEnd = new Date(`${week.week_end}T00:00:00`);
  return weekStart <= monthEnd && weekEnd >= monthStart;
}

function listMonthKeysForWindow(week) {
  const keys = new Set();
  const start = new Date(`${week.week_start}T00:00:00`);
  const end = new Date(`${week.week_end}T00:00:00`);
  const cursor = new Date(start.getFullYear(), start.getMonth(), 1);
  const last = new Date(end.getFullYear(), end.getMonth(), 1);
  while (cursor <= last) {
    keys.add(`${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, "0")}`);
    cursor.setMonth(cursor.getMonth() + 1);
  }
  return [...keys];
}

function getWindowMonthKeys(sourceWindows) {
  const keys = new Set();
  if (isMonthlyMode()) {
    sourceWindows.forEach((item) => {
      if (item.month_key) {
        keys.add(item.month_key);
      }
    });
  } else {
    sourceWindows.forEach((item) => {
      listMonthKeysForWindow(item).forEach((key) => keys.add(key));
    });
  }
  return [...keys].sort((left, right) => right.localeCompare(left));
}

function syncLeaderboardLink() {
  elements.backToLeaderboardLink.href = buildUrlWithSession(HEAT_ROUTE, {
    event: state.selectedEvent,
    platform: state.platform,
    window_mode: state.windowMode,
    quarter_key: isQuarterlyMode() ? state.currentWeek.quarter_key : "",
    month_key: isMonthlyMode() ? state.currentWeek.month_key : "",
    week_start: !isQuarterlyMode() ? state.currentWeek.week_start : "",
    week_end: !isQuarterlyMode() ? state.currentWeek.week_end : "",
  });
  if (elements.trendBackToMarketLink) {
    elements.trendBackToMarketLink.href = buildUrlWithSession(MARKET_ROUTE);
  }
}

function renderSnapshotWindowList() {
  clearNode(elements.trendSnapshotWindowList);
  const visibleWindows = state.windows.filter((item) => !shouldHideFutureWeeklyWindow(item));
  if (!visibleWindows.length) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent = isQuarterlyMode()
      ? "No quarterly windows were found for the selected platform."
      : isMonthlyMode()
      ? "No monthly windows were found for the selected platform."
      : "No weekly windows were found for the selected platform.";
    elements.trendSnapshotWindowList.appendChild(empty);
    return;
  }

  visibleWindows.forEach((item) => {
    const statusMeta = STATUS_META[item.status] || STATUS_META.to_be_updated;
    const isSelected = isQuarterlyMode()
      ? state.currentWeek?.quarter_key === item.quarter_key
      : isMonthlyMode()
      ? state.currentWeek?.month_key === item.month_key
      : state.currentWeek?.week_start === item.week_start && state.currentWeek?.week_end === item.week_end;
    const button = document.createElement("button");
    button.type = "button";
    button.className = `snapshot-window-card ${statusMeta.className}${isSelected ? " selected" : ""}`;
    button.innerHTML = `
      <div class="snapshot-window-head">
        <strong>${isQuarterlyMode() ? item.quarter_key : isMonthlyMode() ? formatMonthLabel(item.month_key) : `${item.week_start.slice(5)} to ${item.week_end.slice(5)}`}</strong>
        <span class="snapshot-status-badge ${statusMeta.className}">${statusMeta.label}</span>
      </div>
    `;
    button.addEventListener("click", async () => {
      showInteractionBusy(
        isQuarterlyMode() ? "Switching quarter..." : isMonthlyMode() ? "Switching month..." : "Switching week...",
        `Refreshing trend charts for ${isQuarterlyMode() ? item.quarter_key : isMonthlyMode() ? formatMonthLabel(item.month_key) : `${item.week_start} to ${item.week_end}`}.`
      );
      state.currentWeek = item;
      renderSnapshotWindowList();
      syncTrendWindowCopy();
      await loadPageData();
    });
    elements.trendSnapshotWindowList.appendChild(button);
  });
}

function syncTrendWindowCopy() {
  if (elements.trendWindowEyebrow) {
    elements.trendWindowEyebrow.textContent = isQuarterlyMode() ? "Current Quarter" : isMonthlyMode() ? "Current Month" : "Current Week";
  }
  elements.trendSnapshotFilterLabel.textContent = "Date Range";
  if (elements.trendSnapshotFilterCopy) {
    elements.trendSnapshotFilterCopy.textContent = "";
  }
  elements.openTrendCalendarButton.textContent = isQuarterlyMode() ? "Pick Quarter" : isMonthlyMode() ? "Pick Month" : "Pick Week";
  if (isQuarterlyMode()) {
    if (elements.trendWeekLabel) {
      elements.trendWeekLabel.textContent = state.currentWeek?.quarter_key || "No analyzed quarter yet";
    }
    if (elements.trendWeekSubLabel) {
      elements.trendWeekSubLabel.textContent = state.currentWeek?.note || "";
    }
    return;
  }
  if ((!isMonthlyMode() && !state.currentWeek.week_start) || (isMonthlyMode() && !state.currentWeek.month_key)) {
    if (elements.trendWeekLabel) {
      elements.trendWeekLabel.textContent = isMonthlyMode() ? "No analyzed month yet" : "No analyzed week yet";
    }
    if (elements.trendWeekSubLabel) {
      elements.trendWeekSubLabel.textContent = "";
    }
    return;
  }
  if (elements.trendWeekLabel) {
    elements.trendWeekLabel.textContent = formatSelectedWindowLabel(state.currentWeek);
  }
  if (elements.trendWeekSubLabel) {
    elements.trendWeekSubLabel.textContent = "";
  }
}

function renderTrendCalendarLegend() {
  clearNode(elements.trendCalendarLegend);
  const legendItems = [
    { label: "To Be Updated", className: "swatch-available" },
    { label: "To Be Analyzed", className: "swatch-updated" },
    { label: "Completed", className: "swatch-imported" },
    { label: "Future", className: "swatch-future" },
  ].filter((item) => !(!isMonthlyMode() && !isQuarterlyMode() && item.label === "Future"));
  legendItems.forEach((item) => {
    const node = document.createElement("span");
    node.className = "calendar-legend-item";
    node.innerHTML = `<span class="calendar-swatch ${item.className}"></span>${item.label}`;
    elements.trendCalendarLegend.appendChild(node);
  });
}

function syncTrendCalendarSelectionSummary() {
  elements.trendCalendarSelectionLabel.textContent = state.calendarSelectedWindow
    ? formatSelectedWindowLabel(state.calendarSelectedWindow)
    : (isQuarterlyMode() ? "No quarter selected" : isMonthlyMode() ? "No month selected" : "No week selected");
  if (elements.trendCalendarSelectionDetail) {
    elements.trendCalendarSelectionDetail.textContent = isQuarterlyMode() ? state.calendarSelectedWindow?.note || QUARTERLY_PENDING_COPY : "";
  }
  elements.confirmTrendCalendarButton.disabled = !state.calendarSelectedWindow;
  elements.confirmTrendCalendarButton.textContent = isQuarterlyMode() ? "Use This Quarter" : isMonthlyMode() ? "Use This Month" : "Use This Week";
}

function renderTrendCalendar() {
  clearNode(elements.trendCalendarGrid);
  elements.trendCalendarGrid.classList.add("calendar-scroll-grid");
  if (isQuarterlyMode()) {
    elements.trendCalendarMonthLabel.textContent = "Available Quarters";
    elements.trendCalendarEyebrow.textContent = "Quarterly Trend Filter";
    elements.trendCalendarTitle.textContent = "Pick Quarter";
    if (elements.trendCalendarHelper) {
      elements.trendCalendarHelper.textContent = "Choose one calendar quarter to inspect the aggregated trend metrics for that period.";
    }
    if (!state.windows.length) {
      const empty = document.createElement("div");
      empty.className = "snapshot-window-empty";
      empty.textContent = "No quarterly windows are available for this platform.";
      elements.trendCalendarGrid.appendChild(empty);
      clearNode(elements.trendCalendarLegend);
      syncTrendCalendarSelectionSummary();
      return;
    }
    const section = document.createElement("section");
    section.className = "calendar-month-section";
    section.innerHTML = `<h3 class="calendar-section-title">Calendar Quarters</h3>`;
    const grid = document.createElement("div");
    grid.className = "calendar-section-grid calendar-section-grid-months";
    state.windows.forEach((item) => {
      const statusMeta = STATUS_META[item.status] || STATUS_META.to_be_updated;
      const isSelected = state.calendarSelectedWindow?.quarter_key === item.quarter_key;
      const button = document.createElement("button");
      button.type = "button";
      button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
      button.innerHTML = `
        <span class="calendar-day-month">${item.quarter_key}</span>
        <span class="calendar-day-status-text">${statusMeta.label}</span>
        ${item.note ? `<span class="calendar-day-helper">${item.note}</span>` : ""}
      `;
      button.addEventListener("click", () => {
        state.calendarSelectedWindow = item;
        syncTrendCalendarSelectionSummary();
        renderTrendCalendar();
      });
      grid.appendChild(button);
    });
    section.appendChild(grid);
    elements.trendCalendarGrid.appendChild(section);
    renderTrendCalendarLegend();
    syncTrendCalendarSelectionSummary();
    return;
  }
  const monthKeys = getWindowMonthKeys(state.windows);
  elements.trendCalendarMonthLabel.textContent = isMonthlyMode() ? "Available Months" : "Available Weeks";
  elements.trendCalendarEyebrow.textContent = isMonthlyMode() ? "Monthly Trend Filter" : "Weekly Trend Filter";
  elements.trendCalendarTitle.textContent = isMonthlyMode() ? "Pick Month" : "Pick Week";
  if (elements.trendCalendarHelper) {
    elements.trendCalendarHelper.textContent = "";
  }

  if (isMonthlyMode()) {
    const section = document.createElement("section");
    section.className = "calendar-month-section";
    section.innerHTML = `<h3 class="calendar-section-title">Calendar Months</h3>`;
    const grid = document.createElement("div");
    grid.className = "calendar-section-grid calendar-section-grid-months";
    state.windows.forEach((item) => {
      const statusMeta = STATUS_META[item.status] || STATUS_META.to_be_updated;
      const isSelected = state.calendarSelectedWindow?.month_key === item.month_key;
      const button = document.createElement("button");
      button.type = "button";
      button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
      button.innerHTML = `
        <span class="calendar-day-month">${formatMonthLabel(item.month_key)}</span>
      `;
      button.addEventListener("click", () => {
        state.calendarSelectedWindow = item;
        syncTrendCalendarSelectionSummary();
        renderTrendCalendar();
      });
      grid.appendChild(button);
    });
    section.appendChild(grid);
    elements.trendCalendarGrid.appendChild(section);
  } else {
    monthKeys.forEach((monthKey) => {
      const visibleWeeks = state.windows.filter(
        (item) => monthOverlapsWindow(monthKey, item) && !shouldHideFutureWeeklyWindow(item)
      );
      if (!visibleWeeks.length) {
        return;
      }
      const section = document.createElement("section");
      section.className = "calendar-month-section";
      section.innerHTML = `<h3 class="calendar-section-title">${formatMonthHeading(monthKey)}</h3>`;
      const grid = document.createElement("div");
      grid.className = "calendar-section-grid";
      visibleWeeks.forEach((week) => {
        const statusMeta = STATUS_META[week.status] || STATUS_META.to_be_updated;
        const isSelected =
          state.calendarSelectedWindow?.week_start === week.week_start &&
          state.calendarSelectedWindow?.week_end === week.week_end;
        const button = document.createElement("button");
        button.type = "button";
        button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
        button.innerHTML = `
          <span class="calendar-week-range">${week.week_start} to ${week.week_end}</span>
        `;
        button.addEventListener("click", () => {
          state.calendarSelectedWindow = week;
          syncTrendCalendarSelectionSummary();
          renderTrendCalendar();
        });
        grid.appendChild(button);
      });
      section.appendChild(grid);
      elements.trendCalendarGrid.appendChild(section);
    });
  }

  renderTrendCalendarLegend();
  syncTrendCalendarSelectionSummary();
}

function openTrendCalendar() {
  state.calendarSelectedWindow = state.currentWeek;
  renderTrendCalendar();
  elements.trendCalendarModal.classList.remove("hidden");
  elements.trendCalendarModal.setAttribute("aria-hidden", "false");
}

function closeTrendCalendar() {
  elements.trendCalendarModal.classList.add("hidden");
  elements.trendCalendarModal.setAttribute("aria-hidden", "true");
}

function openIndicatorGuideModal() {
  elements.indicatorGuideModal?.classList.remove("hidden");
  elements.indicatorGuideModal?.setAttribute("aria-hidden", "false");
}

function closeIndicatorGuideModal() {
  elements.indicatorGuideModal?.classList.add("hidden");
  elements.indicatorGuideModal?.setAttribute("aria-hidden", "true");
}

function renderIndicatorCards(summary) {
  clearNode(elements.trendIndicatorGrid);
  if (isQuarterlyMode()) {
    const cards = [
      {
        label: "Selected Range",
        value: state.currentWeek?.quarter_key || "No quarter selected",
        sub: state.currentWeek?.note || "",
        wide: true,
      },
      {
        label: "Discussion",
        value: formatNumber(summary.discussion_total || 0),
        sub: "Conversation volume in the selected quarter.",
      },
      {
        label: "Engagement",
        value: formatNumber(summary.total_engagement || 0),
        sub: "Likes, comments, and shares in the selected quarter.",
      },
      {
        label: "Unique Authors",
        value: formatNumber(summary.unique_authors || 0),
        sub: "Distinct authors contributing to the selected quarter.",
      },
      {
        label: "Velocity",
        value: formatNumber(summary.post_count || 0),
        sub: "Posting speed across the selected quarter.",
      },
    ];
    cards.forEach((card) => {
      const node = document.createElement("article");
      node.className = `trend-indicator-card${card.wide ? " trend-indicator-card-wide" : ""}`;
      node.innerHTML = `
        <span>${card.label}</span>
        <strong>${card.value}</strong>
      `;
      elements.trendIndicatorGrid.appendChild(node);
    });
    return;
  }
  const cards = [
    {
      label: "Selected Range",
      value: formatSelectedWindowLabel(state.currentWeek),
      sub: "",
      wide: true,
    },
    {
      label: "Discussion",
      value: formatNumber(summary.discussion_total || 0),
      sub: `Conversation volume in the selected ${isMonthlyMode() ? "month" : "week"}`,
    },
    {
      label: "Engagement",
      value: formatNumber(summary.total_engagement || 0),
      sub: `Likes, comments, and shares in the selected ${isMonthlyMode() ? "month" : "week"}`,
    },
    {
      label: "Unique Authors",
      value: formatNumber(summary.unique_authors || 0),
      sub: `Distinct authors in the selected ${isMonthlyMode() ? "month" : "week"}`,
    },
    {
      label: "Velocity",
      value: formatNumber(summary.post_count || 0),
      sub: `Posting speed inside the selected ${isMonthlyMode() ? "monthly" : "weekly"} snapshot`,
    },
  ];

  cards.forEach((card) => {
    const node = document.createElement("article");
    node.className = `trend-indicator-card${card.wide ? " trend-indicator-card-wide" : ""}`;
    node.innerHTML = `
      <span>${card.label}</span>
      <strong>${card.value}</strong>
    `;
    elements.trendIndicatorGrid.appendChild(node);
  });
}

function renderHeatFocusCard(summary) {
  elements.trendHeatFocusCard.classList.remove("pending");
  elements.trendHeatFocusCard.innerHTML = `
    <p class="eyebrow">${isQuarterlyMode() ? "Quarter Heat Score" : "Final Heat Score"}</p>
    <h3>${getClusterDisplayText(summary, "-")}</h3>
    <div class="trend-heat-score">${formatScore(summary.heat_score || 0)}</div>
    <p class="helper-copy">Normalized to a 0-100 scale for easier comparison across windows.</p>
  `;
}

function renderMetricChart(metricSeries, meta) {
  const wrapper = document.createElement("article");
  wrapper.className = "chart-card";
  wrapper.innerHTML = `
    <div class="chart-card-head">
      <div>
        <h3>${meta.subtitle}</h3>
      </div>
      <span class="pill-badge ${meta.accent}">${meta.label}</span>
    </div>
    <div class="chart-stage">
      <svg class="metric-chart" viewBox="0 0 520 250" preserveAspectRatio="none"></svg>
    </div>
  `;

  const svg = wrapper.querySelector("svg");
  const chartStage = wrapper.querySelector(".chart-stage");
  const validSeries = metricSeries.filter((item) => item.value !== null && item.value !== undefined);
  if (!validSeries.length) {
    svg.innerHTML = `<text class="metric-label" x="260" y="126" text-anchor="middle">No ${isQuarterlyMode() ? "quarterly" : isMonthlyMode() ? "monthly" : "weekly"} data</text>`;
    return wrapper;
  }

  const width = 520;
  const height = 250;
  const padding = { top: 20, right: 16, bottom: 36, left: 42 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const maxValue = Math.max(1, ...validSeries.map((item) => Number(item.value || 0)));
  const points = metricSeries.map((item, index) => {
    const x = padding.left + (chartWidth * index) / Math.max(metricSeries.length - 1, 1);
    const hasValue = item.value !== null && item.value !== undefined;
    const y = hasValue ? padding.top + chartHeight - ((item.value || 0) / maxValue) * chartHeight : null;
    return { ...item, x, y, hasValue };
  });
  const plottedPoints = points.filter((point) => point.hasValue);
  const linePath = buildLinePath(plottedPoints);
  const areaPath = buildAreaPath(plottedPoints, padding.top + chartHeight);
  const axisLabelIndexes = getAxisLabelIndexes(metricSeries.length);
  const xLabels = points
    .filter((point, index) => axisLabelIndexes.has(index))
    .map(
      (point) => `<text class="metric-label" x="${point.x}" y="${height - 12}" text-anchor="middle">${formatAxisDateLabel(
        point.date
      )}</text>`
    )
    .join("");
  const trailingMissingIndex = points.findIndex((point, index) => !point.hasValue && index > 0);
  const pendingLabel = trailingMissingIndex >= 0
    ? `<text class="metric-pending-label" x="${width - padding.right}" y="${padding.top + 16}" text-anchor="end">Awaiting later data</text>`
    : "";

  svg.innerHTML = `
    <line class="metric-axis" x1="${padding.left}" y1="${padding.top + chartHeight}" x2="${width - padding.right}" y2="${padding.top + chartHeight}"></line>
    <line class="metric-axis" x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${padding.top + chartHeight}"></line>
    ${areaPath ? `<path class="metric-area" d="${areaPath}"></path>` : ""}
    ${linePath ? `<path class="metric-line" d="${linePath}"></path>` : ""}
    ${plottedPoints
      .map(
        (point, index) =>
          `<circle class="metric-point interactive-point" data-point-index="${index}" cx="${point.x}" cy="${point.y}" r="4.5" tabindex="0"></circle>`
      )
      .join("")}
    ${xLabels}
    ${pendingLabel}
  `;

  const tooltip = document.createElement("div");
  tooltip.className = "chart-tooltip hidden";
  chartStage.appendChild(tooltip);

  svg.querySelectorAll(".interactive-point").forEach((node, index) => {
    const point = plottedPoints[index];
    const showTooltip = () => {
      node.setAttribute("r", "8");
      node.classList.add("is-active");
      tooltip.classList.remove("hidden");
      tooltip.innerHTML = `
        <span class="chart-tooltip-meta">${meta.label}</span>
        <strong>${formatAxisDateLabel(point.date)}</strong>
        <span class="chart-tooltip-value">${formatNumber(point.value || 0)}</span>
      `;
      const pointRect = node.getBoundingClientRect();
      const stageRect = chartStage.getBoundingClientRect();
      tooltip.style.left = `${pointRect.left - stageRect.left + pointRect.width / 2}px`;
      tooltip.style.top = `${pointRect.top - stageRect.top}px`;
    };
    const hideTooltip = () => {
      node.setAttribute("r", "4.5");
      node.classList.remove("is-active");
      tooltip.classList.add("hidden");
    };
    node.addEventListener("mouseenter", showTooltip);
    node.addEventListener("mouseleave", hideTooltip);
    node.addEventListener("focus", showTooltip);
    node.addEventListener("blur", hideTooltip);
  });

  return wrapper;
}

function renderCharts(metrics) {
  clearNode(elements.chartGrid);
  METRIC_META.forEach((meta) => {
    elements.chartGrid.appendChild(renderMetricChart(metrics?.[meta.key] || [], meta));
  });
}

function populateEventSelect(items) {
  clearNode(elements.trendEventSelect);
  if (!items.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No event available";
    elements.trendEventSelect.appendChild(option);
    state.selectedEvent = "";
    elements.trendEventSelect.disabled = true;
    return;
  }

  elements.trendEventSelect.disabled = false;
  items.forEach((item) => {
    const option = document.createElement("option");
    const displayLabel = getClusterDisplayText(item, item.cluster_key || "");
    option.value = item.cluster_key;
    option.textContent = clipOptionLabel(displayLabel);
    option.title = displayLabel;
    elements.trendEventSelect.appendChild(option);
  });

  const initial = state.useInitialUrlEvent ? getUrlState().event : "";
  if (initial && items.some((item) => item.cluster_key === initial)) {
    state.selectedEvent = initial;
  } else if (!items.some((item) => item.cluster_key === state.selectedEvent)) {
    state.selectedEvent = items[0].cluster_key;
  }
  elements.trendEventSelect.value = state.selectedEvent;
  state.useInitialUrlEvent = false;
  syncLeaderboardLink();
}

async function loadTrendData() {
  setPanelBusy(true, "Data is loading...", "Refreshing the trend charts for the selected platform, window, and event.");
  if (
    !state.selectedEvent ||
    (isQuarterlyMode() && !state.currentWeek.quarter_key) ||
    (!isQuarterlyMode() && !isMonthlyMode() && (!state.currentWeek.week_start || !state.currentWeek.week_end)) ||
    (isMonthlyMode() && !state.currentWeek.month_key)
  ) {
    renderIndicatorCards({});
    renderHeatFocusCard({});
    renderCharts({});
    setPanelBusy(false);
    return;
  }

  const params = new URLSearchParams({
    platform: state.platform,
    event_family_key: state.selectedEvent,
    days: "7",
    window_mode: state.windowMode,
  });
  if (isQuarterlyMode()) {
    params.set("quarter_key", state.currentWeek.quarter_key);
  } else if (isMonthlyMode()) {
    params.set("month_key", state.currentWeek.month_key);
  } else {
    params.set("week_start", state.currentWeek.week_start);
    params.set("week_end", state.currentWeek.week_end);
  }
  try {
    const payload = await requestJson(`${API_BASE}/event-trend?${params.toString()}`);
    renderIndicatorCards(payload.summary || {});
    renderHeatFocusCard(payload.summary || {});
    renderCharts(payload.metrics || {});
  } finally {
    setPanelBusy(false);
  }
}

function resolveSnapshot() {
  const urlState = getUrlState();
  const todayKey = new Date().toISOString().slice(0, 10);
  const requestedWeek = state.windows.find((item) =>
    isQuarterlyMode()
      ? item.quarter_key === urlState.quarterKey
      : isMonthlyMode()
      ? item.month_key === urlState.monthKey
      : item.week_start === urlState.weekStart && item.week_end === urlState.weekEnd
  );
  const currentMatched = state.windows.find((item) =>
    isQuarterlyMode()
      ? item.quarter_key === state.currentWeek.quarter_key
      : isMonthlyMode()
      ? item.month_key === state.currentWeek.month_key
      : item.week_start === state.currentWeek.week_start && item.week_end === state.currentWeek.week_end
  );
  const latestClosedCompleted = state.windows.find(
    (item) => item.quarter_end && item.quarter_end < todayKey && item.status === "completed"
  );
  const latestClosedAnalyzable = state.windows.find(
    (item) => item.quarter_end && item.quarter_end < todayKey && item.status === "to_be_analyzed"
  );
  const latestClosedUpdate = state.windows.find(
    (item) => item.quarter_end && item.quarter_end < todayKey && item.status === "to_be_updated"
  );
  const latestCompleted = state.windows.find((item) => item.status === "completed");
  const latestAnalyzable = state.windows.find((item) => item.status === "to_be_analyzed");
  const latestUpdate = state.windows.find((item) => item.status === "to_be_updated");
  state.currentWeek =
    (shouldHideFutureWeeklyWindow(currentMatched) ? null : currentMatched) ||
    (shouldHideFutureWeeklyWindow(requestedWeek) ? null : requestedWeek) ||
    (isQuarterlyMode() ? latestClosedCompleted || latestClosedAnalyzable || latestClosedUpdate : null) ||
    latestCompleted ||
    latestAnalyzable ||
    latestUpdate ||
    { week_start: "", week_end: "", month_key: "", quarter_key: "", status: "" };
}

async function loadWindows() {
  const payload = await requestJson(
    `${API_BASE}/analysis-windows?platform=${encodeURIComponent(state.platform)}&weeks=24&window_mode=${encodeURIComponent(
      state.windowMode
    )}`
  );
  state.windows = payload.items || [];
  state.quarterlyMessage = payload.message || "";
  resolveSnapshot();
  syncTrendWindowCopy();
  renderSnapshotWindowList();
}

async function refreshOverviewMeta(force = false) {
  if (!force && state.overviewCache[state.platform]) {
    const cached = state.overviewCache[state.platform];
    elements.trendDbPathLabel.textContent = cached.db_path || "Unknown analytics database";
    return cached;
  }
  const overview = await requestJson(`${API_BASE}/overview?platform=${encodeURIComponent(state.platform)}&auto_sync=false`);
  state.overviewCache[state.platform] = overview;
  elements.trendDbPathLabel.textContent = overview.db_path || "Unknown analytics database";
  return overview;
}

async function loadPageData() {
  setPanelBusy(true, "Data is loading...", "Refreshing the trend page for the current filters.");
  try {
    const query = new URLSearchParams({
      platform: state.platform,
      limit: "60",
    });
    if (isQuarterlyMode() && state.currentWeek.quarter_key) {
      query.set("quarter_key", state.currentWeek.quarter_key);
    } else if (isMonthlyMode() && state.currentWeek.month_key) {
      query.set("month_key", state.currentWeek.month_key);
    } else if (state.currentWeek.week_start && state.currentWeek.week_end) {
      query.set("week_start", state.currentWeek.week_start);
      query.set("week_end", state.currentWeek.week_end);
    }

    elements.trendDbPathLabel.textContent = state.overviewCache[state.platform]?.db_path || "Loading analytics database...";
    const [overview, leaderboard] = await Promise.all([
      refreshOverviewMeta(),
      requestJson(`${API_BASE}/event-clusters?${query.toString()}`),
    ]);

    elements.trendPageTitle.textContent = isQuarterlyMode()
      ? "Quarterly Trend Analysis"
      : isMonthlyMode()
      ? "Monthly Trend Analysis"
      : "Weekly Trend Analysis";
    if (elements.trendPageSubtitlePrefix) {
      elements.trendPageSubtitlePrefix.textContent = "";
    }
    if (overview.total_posts === 0) {
      elements.trendWeekLabel.textContent = "Database is empty";
      if (elements.trendWeekSubLabel) {
        elements.trendWeekSubLabel.textContent = "";
      }
    } else {
      syncTrendWindowCopy();
    }

    state.leaderboard = leaderboard.items || [];
    populateEventSelect(state.leaderboard);
    syncLeaderboardLink();
    await loadTrendData();
  } catch (error) {
    setPanelBusy(false);
    throw error;
  }
}

async function bootstrap() {
  requireSession();
  const urlState = getUrlState();
  state.platform = urlState.platform || "wb";
  state.windowMode = urlState.windowMode || "monthly";
  syncPlatformControls();
  syncWindowModeControls();
  elements.trendDbPathLabel.textContent = "Loading analytics database...";

  elements.trendPlatformSelect.addEventListener("change", async (event) => {
    const nextPlatform = event.target.value || "wb";
    showInteractionBusy("Switching platform...", `Refreshing trend charts for ${PLATFORM_LABELS[nextPlatform]}.`);
    state.platform = event.target.value || "wb";
    syncPlatformControls();
    state.selectedEvent = "";
    state.useInitialUrlEvent = false;
    state.currentWeek = { week_start: "", week_end: "", month_key: "", quarter_key: "", status: "" };
    await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
    await loadPageData();
  });

  elements.trendPlatformChoiceInputs.forEach((input) => {
    input.addEventListener("change", async (event) => {
      if (!event.target.checked) {
        return;
      }
      const nextPlatform = event.target.value || "wb";
      showInteractionBusy("Switching platform...", `Refreshing trend charts for ${PLATFORM_LABELS[nextPlatform]}.`);
      state.platform = event.target.value || "wb";
      syncPlatformControls();
      state.selectedEvent = "";
      state.useInitialUrlEvent = false;
      state.currentWeek = { week_start: "", week_end: "", month_key: "", quarter_key: "", status: "" };
      await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
      await loadPageData();
    });
  });

  elements.trendWindowModeSelect.addEventListener("change", async (event) => {
    const nextMode = event.target.value || "monthly";
    showInteractionBusy("Switching date range...", `Updating ${nextMode} filters and rebuilding the trend view.`);
    state.windowMode = event.target.value || "monthly";
    syncWindowModeControls();
    state.selectedEvent = "";
    state.useInitialUrlEvent = false;
    state.currentWeek = { week_start: "", week_end: "", month_key: "", quarter_key: "", status: "" };
    await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
    await loadPageData();
  });

  elements.trendWindowModeButtons.forEach((button) => {
    button.addEventListener("click", async () => {
      const nextMode = button.dataset.windowMode || "monthly";
      if (nextMode === state.windowMode) {
        return;
      }
      showInteractionBusy("Switching date range...", `Updating ${nextMode} filters and rebuilding the trend view.`);
      state.windowMode = nextMode;
      syncWindowModeControls();
      state.selectedEvent = "";
      state.useInitialUrlEvent = false;
      state.currentWeek = { week_start: "", week_end: "", month_key: "", quarter_key: "", status: "" };
      await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
      await loadPageData();
    });
  });

  elements.trendEventSelect.addEventListener("change", async (event) => {
    const nextEvent = event.target.value;
    showInteractionBusy("Switching event...", nextEvent ? "Refreshing charts for the selected event." : "Refreshing charts for the current event selection.");
    state.selectedEvent = event.target.value;
    syncLeaderboardLink();
    await loadTrendData();
  });

  elements.openTrendCalendarButton.addEventListener("click", openTrendCalendar);
  elements.closeTrendCalendarButton.addEventListener("click", closeTrendCalendar);
  elements.trendCalendarBackdrop.addEventListener("click", closeTrendCalendar);
  elements.openIndicatorGuideButton?.addEventListener("click", openIndicatorGuideModal);
  elements.closeIndicatorGuideButton?.addEventListener("click", closeIndicatorGuideModal);
  elements.indicatorGuideBackdrop?.addEventListener("click", closeIndicatorGuideModal);
  elements.confirmTrendCalendarButton.addEventListener("click", async () => {
    if (!state.calendarSelectedWindow) {
      return;
    }
    showInteractionBusy(
      isQuarterlyMode() ? "Applying quarter filter..." : isMonthlyMode() ? "Applying month filter..." : "Applying week filter...",
      `Refreshing the trend page for ${formatSelectedWindowLabel(state.calendarSelectedWindow)}.`
    );
    state.currentWeek = state.calendarSelectedWindow;
    renderSnapshotWindowList();
    syncTrendWindowCopy();
    closeTrendCalendar();
    await loadPageData();
  });

  await Promise.all([loadWindows(), refreshOverviewMeta()]);
  await loadPageData();
}

bootstrap().catch((error) => {
  elements.trendDbPathLabel.textContent = `Load failed: ${error.message}`;
});
