const LEADERBOARD_LIMIT = 40;
const WEEKDAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const API_BASE = "/api/full-web-heat-analysis";
const HEAT_ROUTE = "/full-web-heat-analysis";
const TRENDS_ROUTE = "/full-web-heat-analysis/trends";
const MARKET_ROUTE = "/operation_panel.html";
const LOGIN_ROUTE = "/login_page.html";
const SESSION_KEYS = ["first_name", "last_name", "email", "position", "role", "token"];

const PLATFORM_LABELS = {
  wb: "Weibo",
  fb: "Facebook",
};

const STATUS_META = {
  completed: {
    label: "Completed",
    className: "status-completed",
    detail: "This weekly snapshot already has analyzed clusters.",
  },
  to_be_analyzed: {
    label: "To Be Analyzed",
    className: "status-updated",
    detail: "Raw posts exist in the database, but weekly clustering still needs to run.",
  },
  to_be_updated: {
    label: "To Be Updated",
    className: "status-available",
    detail: "No raw posts exist for this week yet. Update Database first.",
  },
  future: {
    label: "Future",
    className: "status-future",
    detail: "Future weeks cannot be selected yet.",
  },
};

const FILTER_STATUS_META = {
  completed: {
    label: "Completed",
    className: "status-completed",
    detail: "Imported data already exists in the database for this window.",
  },
  to_be_updated: {
    label: "To Be Updated",
    className: "status-available",
    detail: "No imported posts exist in the database for this window yet.",
  },
  future: {
    label: "Future",
    className: "status-future",
    detail: "Future windows stay visible but cannot be selected yet.",
  },
};

const UPDATE_CALENDAR_META = {
  future: {
    label: "Future",
    className: "status-future",
  },
  to_be_updated: {
    label: "To Be Updated",
    className: "status-updated",
  },
  to_be_analyzed: {
    label: "Updated",
    className: "status-completed",
  },
  completed: {
    label: "Updated",
    className: "status-completed",
  },
  ready_to_import: {
    label: "Ready to Import",
    className: "status-available",
  },
};

const CALENDAR_MODE_CONFIG = {
  filter: {
    eyebrow: "Weekly Heat Filter",
    title: "Pick Week",
    helper:
      "Choose one fixed Sunday to Saturday window for the leaderboard filter. Date statuses below reflect whether imported database data already exists for that window.",
    confirmLabel: "Use This Week",
    emptyLabel: "No week selected",
    emptyDetail: "Choose one fixed Sunday to Saturday week to refresh the leaderboard and heat overview.",
    selectableStatuses: new Set(["to_be_updated", "to_be_analyzed", "completed"]),
    legend: [
      { label: "Completed", className: "swatch-imported" },
      { label: "To Be Updated", className: "swatch-available" },
      { label: "Future", className: "swatch-future" },
    ],
  },
  update: {
    eyebrow: "Weekly Heat Analysis",
    title: "Update Database",
    helper:
      "Choose any finished Sunday to Saturday week to crawl again or for the first time. New crawl results are staged first and only enter the database after you confirm import.",
    confirmLabel: "Start Crawl",
    emptyLabel: "No week selected",
    emptyDetail: "Choose one finished week to stage a fresh crawl for this platform. Future weeks stay visible but cannot be selected.",
    selectableStatuses: new Set(["to_be_updated", "to_be_analyzed", "completed", "ready_to_import"]),
    legend: [
      { label: "Updated", className: "swatch-imported" },
      { label: "To Be Updated", className: "swatch-updated" },
      { label: "Ready to Import", className: "swatch-available" },
      { label: "Future", className: "swatch-future" },
    ],
  },
};

const QUARTERLY_PENDING_COPY =
  "Quarterly reporting aggregates the monthly snapshots already available inside that quarter.";

const state = {
  boardType: "event",
  platform: "wb",
  updatePlatform: "wb",
  windowMode: "monthly",
  sortMetric: "heat_score",
  selectedEvent: "",
  selectedWeek: null,
  calendarMode: "filter",
  calendarSelectedWeek: null,
  lastUpdateSelection: null,
  crawlMonitorMinimized: false,
  calendarNotice: "",
  windows: [],
  updateWindows: [],
  calendarScrollTop: 0,
  monthCursor: new Date(),
  latestJob: null,
  pollingJobId: "",
  leaderboardLoading: false,
  contextRow: null,
  lastRenderedItems: [],
  overviewCache: {},
  updateOverviewCache: {},
};

const elements = {
  heatDbPathLabel: document.getElementById("heatDbPathLabel"),
  heatOverviewGrid: document.getElementById("heatOverviewGrid"),
  leaderboardTableBody: document.getElementById("leaderboardTableBody"),
  leaderboardTitle: document.getElementById("leaderboardTitle"),
  leaderboardSubtitle: document.getElementById("leaderboardSubtitle"),
  leaderboardCounter: document.getElementById("leaderboardCounter"),
  leaderboardLoadingHint: document.getElementById("leaderboardLoadingHint"),
  leaderboardBusyOverlay: document.getElementById("leaderboardBusyOverlay"),
  leaderboardBusyEmoji: document.getElementById("leaderboardBusyEmoji"),
  leaderboardBusyTitle: document.getElementById("leaderboardBusyTitle"),
  leaderboardBusyDetail: document.getElementById("leaderboardBusyDetail"),
  controlBusyOverlay: document.getElementById("controlBusyOverlay"),
  controlBusyEmoji: document.getElementById("controlBusyEmoji"),
  controlBusyTitle: document.getElementById("controlBusyTitle"),
  controlBusyDetail: document.getElementById("controlBusyDetail"),
  openHeatFormulaButton: document.getElementById("openHeatFormulaButton"),
  heatFormulaModal: document.getElementById("heatFormulaModal"),
  heatFormulaBackdrop: document.getElementById("heatFormulaBackdrop"),
  closeHeatFormulaButton: document.getElementById("closeHeatFormulaButton"),
  trendPageLink: document.getElementById("trendPageLink"),
  backToMarketLink: document.getElementById("backToMarketLink"),
  eventTabButton: document.getElementById("eventTabButton"),
  topicTabButton: document.getElementById("topicTabButton"),
  platformSelect: document.getElementById("platformSelect"),
  platformChoiceInputs: Array.from(document.querySelectorAll('input[name="platformChoice"]')),
  updatePlatformSelect: document.getElementById("updatePlatformSelect"),
  updatePlatformRow: document.getElementById("updatePlatformRow"),
  updatePlatformChoiceInputs: Array.from(document.querySelectorAll('input[name="updatePlatformChoice"]')),
  updatePlatformStateCard: document.getElementById("updatePlatformStateCard"),
  updatePlatformStatePill: document.getElementById("updatePlatformStatePill"),
  updatePlatformStateCopy: document.getElementById("updatePlatformStateCopy"),
  updatePlatformLatestWeek: document.getElementById("updatePlatformLatestWeek"),
  updatePlatformCrawlableCount: document.getElementById("updatePlatformCrawlableCount"),
  updatePlatformReadyCount: document.getElementById("updatePlatformReadyCount"),
  updatePlatformFutureCount: document.getElementById("updatePlatformFutureCount"),
  sortMetricSelect: document.getElementById("sortMetricSelect"),
  windowModeSelect: document.getElementById("windowModeSelect"),
  windowModeButtons: Array.from(document.querySelectorAll("#windowModeSegmentedControl .filter-segment-button")),
  activeWindowEyebrow: document.getElementById("activeWindowEyebrow"),
  activeWeekLabel: document.getElementById("activeWeekLabel"),
  activeWeekSubLabel: document.getElementById("activeWeekSubLabel"),
  snapshotFilterLabel: document.getElementById("snapshotFilterLabel"),
  snapshotFilterCopy: document.getElementById("snapshotFilterCopy"),
  heatJobStatusLabel: document.getElementById("heatJobStatusLabel"),
  heatJobDetailLabel: document.getElementById("heatJobDetailLabel"),
  updateDatabaseButton: document.getElementById("updateDatabaseButton"),
  openSnapshotCalendarButton: document.getElementById("openSnapshotCalendarButton"),
  sidebarRunAnalysisButton: document.getElementById("sidebarRunAnalysisButton"),
  cancelUpdateJobButton: document.getElementById("cancelUpdateJobButton"),
  confirmImportButton: document.getElementById("confirmImportButton"),
  confirmUpdateButton: document.getElementById("confirmUpdateButton"),
  crawlMonitorShell: document.getElementById("crawlMonitorShell"),
  crawlMonitorChip: document.getElementById("crawlMonitorChip"),
  crawlMonitorChipLabel: document.getElementById("crawlMonitorChipLabel"),
  crawlMonitorPanel: document.getElementById("crawlMonitorPanel"),
  crawlMonitorBody: document.getElementById("crawlMonitorBody"),
  crawlMonitorTitle: document.getElementById("crawlMonitorTitle"),
  crawlMonitorStatusBadge: document.getElementById("crawlMonitorStatusBadge"),
  crawlMonitorPlatform: document.getElementById("crawlMonitorPlatform"),
  crawlMonitorWeek: document.getElementById("crawlMonitorWeek"),
  crawlMonitorDetail: document.getElementById("crawlMonitorDetail"),
  crawlMonitorUpdatedAt: document.getElementById("crawlMonitorUpdatedAt"),
  crawlMonitorLog: document.getElementById("crawlMonitorLog"),
  crawlMonitorToggleButton: document.getElementById("crawlMonitorToggleButton"),
  crawlMonitorStopButton: document.getElementById("crawlMonitorStopButton"),
  crawlMonitorImportButton: document.getElementById("crawlMonitorImportButton"),
  updateCalendarModal: document.getElementById("updateCalendarModal"),
  updateCalendarPanel: document.querySelector("#updateCalendarModal .modal-panel"),
  updateCalendarBackdrop: document.getElementById("updateCalendarBackdrop"),
  closeCalendarButton: document.getElementById("closeCalendarButton"),
  previousMonthButton: document.getElementById("previousMonthButton"),
  nextMonthButton: document.getElementById("nextMonthButton"),
  calendarToolbar: document.getElementById("calendarToolbar"),
  calendarMonthLabel: document.getElementById("calendarMonthLabel"),
  calendarWeekdayRow: document.getElementById("calendarWeekdayRow"),
  calendarGrid: document.getElementById("calendarGrid"),
  calendarModeEyebrow: document.getElementById("calendarModeEyebrow"),
  calendarModeTitle: document.getElementById("calendarModeTitle"),
  calendarModeHelper: document.getElementById("calendarModeHelper"),
  calendarSelectionLabel: document.getElementById("calendarSelectionLabel"),
  calendarSelectionDetail: document.getElementById("calendarSelectionDetail"),
  calendarLegend: document.getElementById("calendarLegend"),
  snapshotWindowList: document.getElementById("snapshotWindowList"),
  clusterContextMenu: document.getElementById("clusterContextMenu"),
  clusterMarkNoiseButton: document.getElementById("clusterMarkNoiseButton"),
  clusterMergeButton: document.getElementById("clusterMergeButton"),
  clusterMergeModal: document.getElementById("clusterMergeModal"),
  clusterMergeBackdrop: document.getElementById("clusterMergeBackdrop"),
  closeClusterMergeButton: document.getElementById("closeClusterMergeButton"),
  cancelClusterMergeButton: document.getElementById("cancelClusterMergeButton"),
  clusterMergeTitle: document.getElementById("clusterMergeTitle"),
  clusterMergeHelper: document.getElementById("clusterMergeHelper"),
  clusterMergeSourceHeading: document.getElementById("clusterMergeSourceHeading"),
  clusterMergeTargetHeading: document.getElementById("clusterMergeTargetHeading"),
  clusterMergeSourceLabel: document.getElementById("clusterMergeSourceLabel"),
  clusterMergeTargetSelect: document.getElementById("clusterMergeTargetSelect"),
  confirmClusterMergeButton: document.getElementById("confirmClusterMergeButton"),
  clusterNoiseModal: document.getElementById("clusterNoiseModal"),
  clusterNoiseBackdrop: document.getElementById("clusterNoiseBackdrop"),
  closeClusterNoiseButton: document.getElementById("closeClusterNoiseButton"),
  cancelClusterNoiseButton: document.getElementById("cancelClusterNoiseButton"),
  clusterNoiseTitle: document.getElementById("clusterNoiseTitle"),
  clusterNoiseHelper: document.getElementById("clusterNoiseHelper"),
  clusterNoiseSourceHeading: document.getElementById("clusterNoiseSourceHeading"),
  clusterNoiseSourceLabel: document.getElementById("clusterNoiseSourceLabel"),
  confirmClusterNoiseButton: document.getElementById("confirmClusterNoiseButton"),
  emptyTemplate: document.getElementById("leaderboardEmptyStateTemplate"),
};

function syncPlatformControls() {
  elements.platformSelect.value = state.platform;
  elements.platformChoiceInputs.forEach((input) => {
    input.checked = input.value === state.platform;
  });
}

function syncUpdatePlatformControls() {
  if (elements.updatePlatformSelect) {
    elements.updatePlatformSelect.value = state.updatePlatform;
  }
  elements.updatePlatformChoiceInputs.forEach((input) => {
    input.checked = input.value === state.updatePlatform;
  });
}

function syncWindowModeControls() {
  elements.windowModeSelect.value = state.windowMode;
  elements.windowModeButtons.forEach((button) => {
    const isActive = button.dataset.windowMode === state.windowMode;
    button.classList.toggle("active", isActive);
    button.classList.toggle("secondary", !isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return response.json();
}

async function postJson(url) {
  return requestJson(url, { method: "POST" });
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

function clipText(value, maxLength = 132) {
  const text = String(value || "").trim();
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 1)}...`;
}

function getClusterDisplayText(item, fallback = "") {
  return String(item?.cluster_key_display || item?.cluster_key || fallback).trim();
}

function isMonthlyMode() {
  return state.windowMode === "monthly";
}

function isQuarterlyMode() {
  return state.windowMode === "quarterly";
}

function formatWeekLabel(weekStart, weekEnd) {
  if (!weekStart || !weekEnd) {
    return "No week selected";
  }
  return `${weekStart} to ${weekEnd}`;
}

function formatMonthLabel(monthKey) {
  if (!monthKey) {
    return "No month selected";
  }
  const [year, month] = String(monthKey).split("-");
  if (!year || !month) {
    return monthKey;
  }
  return `${year}-${month}`;
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

function formatCalendarSelectedWindowLabel(windowValue) {
  if (state.calendarMode === "update") {
    return windowValue?.week_start && windowValue?.week_end
      ? formatWeekLabel(windowValue.week_start, windowValue.week_end)
      : "No week selected";
  }
  return formatSelectedWindowLabel(windowValue);
}

function formatDatabaseUpdatedDateLabel(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "--";
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return text;
  }
  const dateValue = new Date(text);
  if (Number.isNaN(dateValue.getTime())) {
    return text.slice(0, 10) || "--";
  }
  return `${dateValue.getFullYear()}-${String(dateValue.getMonth() + 1).padStart(2, "0")}-${String(dateValue.getDate()).padStart(2, "0")}`;
}

function formatDatabaseUpdatedAtLabel(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  const dateValue = new Date(text);
  if (Number.isNaN(dateValue.getTime())) {
    return text;
  }
  return dateValue.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatCoverageLabel(startDate, endDate) {
  const start = String(startDate || "").trim();
  const end = String(endDate || "").trim();
  if (!start && !end) {
    return "--";
  }
  if (start && end) {
    return `${start} to ${end}`;
  }
  return start || end;
}

function formatCoverageLabelMarkup(startDate, endDate) {
  const start = String(startDate || "").trim();
  const end = String(endDate || "").trim();
  if (!start && !end) {
    return '<span class="metric-coverage-line">--</span>';
  }
  if (start && end) {
    return [
      `<span class="metric-coverage-line"><span class="metric-coverage-date">${escapeHtml(start)}</span><span class="metric-coverage-separator">to</span></span>`,
      `<span class="metric-coverage-line">${escapeHtml(end)}</span>`,
    ].join("");
  }
  if (start) {
    return `<span class="metric-coverage-line">${escapeHtml(start)}</span>`;
  }
  return `<span class="metric-coverage-line">${escapeHtml(end)}</span>`;
}

function getBoardTypeLabel() {
  return state.boardType === "topic" ? "Topic" : "Event";
}

function getCurrentStatusMeta() {
  return STATUS_META[state.selectedWeek?.status] || STATUS_META.to_be_updated;
}

function hasWindowDatabaseData(windowValue) {
  if (!windowValue) {
    return false;
  }
  return (
    Number(windowValue.post_count || 0) > 0 ||
    Number(windowValue.source_ready_posts || 0) > 0 ||
    Number(windowValue.extracted_post_rows || 0) > 0 ||
    Number(windowValue.event_cluster_rows || 0) > 0 ||
    Number(windowValue.topic_cluster_rows || 0) > 0
  );
}

function getFilterWindowStatus(windowValue) {
  if (!windowValue) {
    return "to_be_updated";
  }
  if (windowValue.status === "future" || windowValue.is_future) {
    return "future";
  }
  return hasWindowDatabaseData(windowValue) ? "completed" : "to_be_updated";
}

function getFilterStatusMeta(windowValue) {
  return FILTER_STATUS_META[getFilterWindowStatus(windowValue)] || FILTER_STATUS_META.to_be_updated;
}

function getUpdateWindowStatus(windowValue) {
  return windowValue?.update_status || windowValue?.status || "";
}

function getUpdateWindowForWeek(week) {
  if (!week?.week_start || !week?.week_end) {
    return null;
  }
  return (
    state.updateWindows.find(
      (item) => item.week_start === week.week_start && item.week_end === week.week_end
    ) || null
  );
}

function getSelectedWeeklyUpdateWindow() {
  if (isMonthlyMode() || isQuarterlyMode()) {
    return null;
  }
  return getUpdateWindowForWeek(state.selectedWeek);
}

function getImportCandidateWindow() {
  const selectedWeekly = getSelectedWeeklyUpdateWindow();
  if (selectedWeekly && getUpdateWindowStatus(selectedWeekly) === "ready_to_import") {
    return selectedWeekly;
  }
  if (
    state.calendarMode === "update" &&
    state.calendarSelectedWeek &&
    getUpdateWindowStatus(state.calendarSelectedWeek) === "ready_to_import"
  ) {
    return state.calendarSelectedWeek;
  }
  const latestJob = getLatestPlatformJob();
  if (latestJob?.payload?.week_start && latestJob?.payload?.week_end) {
    const latestWindow = state.updateWindows.find(
      (item) =>
        item.week_start === latestJob.payload.week_start &&
        item.week_end === latestJob.payload.week_end
    );
    if (latestWindow && getUpdateWindowStatus(latestWindow) === "ready_to_import") {
      return latestWindow;
    }
  }
  return null;
}

function getLatestPlatformJob() {
  const job = state.latestJob;
  if (!job || job.job_type !== "update_week") {
    return null;
  }
  if ((job.payload?.platform || "") !== state.updatePlatform) {
    return null;
  }
  return job;
}

function getAnyActiveUpdateJob() {
  const job = state.latestJob;
  if (!job || job.job_type !== "update_week") {
    return null;
  }
  return new Set(["queued", "running", "cancelling"]).has(job.status || "") ? job : null;
}

function formatJobWeekLabel(job) {
  const weekStart = job?.payload?.week_start || "";
  const weekEnd = job?.payload?.week_end || "";
  if (!weekStart || !weekEnd) {
    return "the selected week";
  }
  return formatWeekLabel(weekStart, weekEnd);
}

function getCalendarModeConfig() {
  if (state.calendarMode === "update") {
    return CALENDAR_MODE_CONFIG.update;
  }
  if (isMonthlyMode()) {
    return {
      eyebrow: "Monthly Heat Filter",
      title: "Pick Month",
      helper: "Choose one calendar month for the leaderboard filter. Date statuses below reflect whether imported database data already exists for that month.",
      confirmLabel: "Use This Month",
      emptyLabel: "No month selected",
      emptyDetail: "Choose one calendar month to refresh the leaderboard and heat overview.",
      selectableStatuses: new Set(["to_be_updated", "to_be_analyzed", "completed"]),
      legend: [
        { label: "Completed", className: "swatch-imported" },
        { label: "To Be Updated", className: "swatch-available" },
        { label: "Future", className: "swatch-future" },
      ],
    };
  }
  if (isQuarterlyMode()) {
    return {
      eyebrow: "Quarterly Heat Filter",
      title: "Pick Quarter",
      helper:
        "Choose one calendar quarter for an aggregated leaderboard. Quarterly panels combine the monthly snapshots already available inside that quarter.",
      confirmLabel: "Use This Quarter",
      emptyLabel: "No quarter selected",
      emptyDetail: "Choose one calendar quarter to refresh the aggregated leaderboard and heat overview.",
      selectableStatuses: new Set(["to_be_updated", "to_be_analyzed", "completed"]),
      legend: [
        { label: "Completed", className: "swatch-imported" },
        { label: "To Be Updated", className: "swatch-available" },
        { label: "Future", className: "swatch-future" },
      ],
    };
  }
  return CALENDAR_MODE_CONFIG.filter;
}

function setStatus(status, detail) {
  elements.heatJobStatusLabel.textContent = status;
  elements.heatJobDetailLabel.textContent = detail;
}

function getHeatTone(value) {
  if (value >= 85) return { flames: "🔥🔥🔥", className: "heat-hot" };
  if (value >= 70) return { flames: "🔥🔥", className: "heat-warm" };
  if (value >= 50) return { flames: "🔥", className: "heat-mild" };
  return { flames: "", className: "heat-mild" };
}

function getSortMetricValue(item, metric) {
  const metricAlias = {
    heat_score: "heat_score",
    post_count: "post_count",
    engagement_total: "total_engagement",
    discussion_total: "discussion_total",
    unique_authors: "unique_authors",
  };
  const fieldName = metricAlias[metric] || metric;
  return Number(item?.[fieldName] || 0);
}

function sortItems(items) {
  const metric = state.sortMetric;
  return [...items]
    .sort((left, right) => {
      const rightValue = getSortMetricValue(right, metric);
      const leftValue = getSortMetricValue(left, metric);
      if (rightValue !== leftValue) {
        return rightValue - leftValue;
      }
      return Number(right.heat_score || 0) - Number(left.heat_score || 0);
    })
    .slice(0, LEADERBOARD_LIMIT);
}

function setLeaderboardLoading(isLoading, message = "Refreshing the leaderboard for the current filters...") {
  state.leaderboardLoading = isLoading;
  if (!elements.leaderboardLoadingHint) {
    return;
  }
  elements.leaderboardLoadingHint.textContent = message;
  elements.leaderboardLoadingHint.classList.toggle("hidden", !isLoading);
}

function resolveBusyEmoji(title = "", detail = "") {
  const text = `${title} ${detail}`.toLowerCase();
  if (text.includes("database")) return "🗂️";
  if (text.includes("analysis")) return "🔥";
  if (text.includes("platform")) return "📡";
  if (text.includes("date range") || text.includes("month") || text.includes("week") || text.includes("quarter")) return "🗓️";
  if (text.includes("topic")) return "🧭";
  if (text.includes("event")) return "📈";
  if (text.includes("reorder") || text.includes("sort")) return "↕️";
  if (text.includes("feedback") || text.includes("cluster")) return "🛠️";
  return "⏳";
}

function setPanelBusy(
  isBusy,
  title = "Data is loading...",
  detail = "Please wait while the leaderboard refreshes.",
  options = {}
) {
  const { blockControls = true } = options;
  elements.leaderboardBusyOverlay?.classList.toggle("hidden", !isBusy);
  elements.leaderboardBusyOverlay?.setAttribute("aria-hidden", isBusy ? "false" : "true");
  const showControlOverlay = isBusy && blockControls;
  elements.controlBusyOverlay?.classList.toggle("hidden", !showControlOverlay);
  elements.controlBusyOverlay?.setAttribute("aria-hidden", showControlOverlay ? "false" : "true");
  const busyEmoji = resolveBusyEmoji(title, detail);
  if (elements.leaderboardBusyTitle) {
    elements.leaderboardBusyTitle.textContent = title;
  }
  if (elements.leaderboardBusyDetail) {
    elements.leaderboardBusyDetail.textContent = detail;
  }
  if (elements.leaderboardBusyEmoji) {
    elements.leaderboardBusyEmoji.textContent = busyEmoji;
  }
  if (elements.controlBusyTitle) {
    elements.controlBusyTitle.textContent = isBusy ? "Responding..." : "Ready";
  }
  if (elements.controlBusyDetail) {
    elements.controlBusyDetail.textContent = detail;
  }
  if (elements.controlBusyEmoji) {
    elements.controlBusyEmoji.textContent = busyEmoji;
  }
}

function showInteractionBusy(title, detail) {
  setPanelBusy(true, title, detail);
}

function hasActiveUpdateJob() {
  return Boolean(getAnyActiveUpdateJob());
}

function shouldShowCrawlMonitor(job = state.latestJob) {
  if (!job || job.job_type !== "update_week") {
    return false;
  }
  return new Set(["queued", "running", "cancelling", "awaiting_confirmation", "failed", "cancelled", "completed"]).has(job.status || "");
}

function formatRelativeMonitorTime(timestampText) {
  if (!timestampText) {
    return "Waiting for updates...";
  }
  const parsed = new Date(timestampText);
  if (Number.isNaN(parsed.getTime())) {
    return "Updated just now";
  }
  return `Updated ${parsed.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  })}`;
}

function getMonitorStatusCopy(job) {
  const status = job?.status || "";
  if (status === "queued") {
    return {
      title: "Crawl queued",
      badge: "Queued",
      detail: `The crawler is queued for ${formatJobWeekLabel(job)} and will start shortly.`,
    };
  }
  if (status === "running") {
    return {
      title: "Crawl in progress",
      badge: "Running",
      detail: `The crawler is running for ${formatJobWeekLabel(job)}. You can keep browsing and open this panel any time.`,
    };
  }
  if (status === "cancelling") {
    return {
      title: "Stopping crawl",
      badge: "Stopping",
      detail: `Stopping the active crawl for ${formatJobWeekLabel(job)}. The task is shutting down now.`,
    };
  }
  if (status === "awaiting_confirmation") {
    return {
      title: "Ready to import",
      badge: "Ready",
      detail: `Crawl files were staged for ${formatJobWeekLabel(job)}. Review the output below, then confirm import when ready.`,
    };
  }
  if (status === "failed") {
    return {
      title: "Crawl failed",
      badge: "Failed",
      detail: job.error || `The crawl for ${formatJobWeekLabel(job)} failed. Review the output below for details.`,
    };
  }
  if (status === "cancelled") {
    return {
      title: "Crawl cancelled",
      badge: "Cancelled",
      detail: `The crawl for ${formatJobWeekLabel(job)} was stopped before import. Existing database data was not changed.`,
    };
  }
  if (status === "completed") {
    return {
      title: "Import completed",
      badge: "Done",
      detail: `The staged files for ${formatJobWeekLabel(job)} were imported successfully.`,
    };
  }
  return {
    title: "Background crawl",
    badge: "Active",
    detail: "The crawler is running in the background.",
  };
}

function renderCrawlMonitor() {
  const job = state.latestJob;
  const shouldShow = shouldShowCrawlMonitor(job);
  elements.crawlMonitorShell?.classList.toggle("hidden", !shouldShow);
  if (!shouldShow) {
    return;
  }

  const copy = getMonitorStatusCopy(job);
  const isMinimized = state.crawlMonitorMinimized;
  elements.crawlMonitorChip?.classList.toggle("hidden", !isMinimized);
  elements.crawlMonitorPanel?.classList.toggle("hidden", isMinimized);
  if (elements.crawlMonitorToggleButton) {
    elements.crawlMonitorToggleButton.textContent = isMinimized ? "Expand" : "Minimize";
  }
  if (elements.crawlMonitorChipLabel) {
    elements.crawlMonitorChipLabel.textContent = `${copy.badge}: ${formatJobWeekLabel(job)}`;
  }
  if (elements.crawlMonitorTitle) {
    elements.crawlMonitorTitle.textContent = copy.title;
  }
  if (elements.crawlMonitorStatusBadge) {
    elements.crawlMonitorStatusBadge.textContent = copy.badge;
    elements.crawlMonitorStatusBadge.dataset.status = job?.status || "";
  }
  if (elements.crawlMonitorPlatform) {
    elements.crawlMonitorPlatform.textContent = PLATFORM_LABELS[job?.payload?.platform] || PLATFORM_LABELS[state.platform] || "Unknown";
  }
  if (elements.crawlMonitorWeek) {
    elements.crawlMonitorWeek.textContent = formatJobWeekLabel(job);
  }
  if (elements.crawlMonitorDetail) {
    elements.crawlMonitorDetail.textContent = copy.detail;
  }
  if (elements.crawlMonitorUpdatedAt) {
    elements.crawlMonitorUpdatedAt.textContent = formatRelativeMonitorTime(job?.finished_at || job?.started_at || job?.created_at);
  }
  if (elements.crawlMonitorLog) {
    const logText = (job?.log_tail || job?.error || "").trim();
    elements.crawlMonitorLog.textContent = logText || "Waiting for crawl output...";
    elements.crawlMonitorLog.scrollTop = elements.crawlMonitorLog.scrollHeight;
  }
  if (elements.crawlMonitorStopButton) {
    elements.crawlMonitorStopButton.disabled = !new Set(["queued", "running", "cancelling"]).has(job?.status || "");
  }
  if (elements.crawlMonitorImportButton) {
    elements.crawlMonitorImportButton.disabled = !(job?.status === "awaiting_confirmation");
  }
}

function getUpdatePlatformLabel() {
  return PLATFORM_LABELS[state.updatePlatform] || PLATFORM_LABELS[state.platform] || "Unknown";
}

function getJobPlatformLabel(job) {
  return PLATFORM_LABELS[job?.payload?.platform] || getUpdatePlatformLabel();
}

function renderUpdatePlatformState() {
  if (!elements.updatePlatformStateCard) {
    return;
  }
  const isUpdateMode = state.calendarMode === "update";
  const label = isUpdateMode ? getUpdatePlatformLabel() : (PLATFORM_LABELS[state.platform] || "Unknown");
  const sourceWindows = isUpdateMode ? state.updateWindows : state.windows;
  const latestFinishedWeek = sourceWindows.find((item) => (isUpdateMode ? getUpdateWindowStatus(item) : item.status) !== "future") || null;

  elements.updatePlatformStateCard.dataset.platform = isUpdateMode ? state.updatePlatform : state.platform;
  elements.updatePlatformStateCard.dataset.mode = isUpdateMode ? "update" : "filter";

  if (isUpdateMode) {
    const updateOverview = state.updateOverviewCache[state.updatePlatform] || {};
    const databaseMeta = updateOverview.update_database_meta || {};
    elements.updatePlatformCrawlableCount.classList.add("metric-value-coverage");
    elements.updatePlatformReadyCount.classList.remove("metric-value-coverage");
    elements.updatePlatformFutureCount.classList.remove("metric-value-coverage");
    elements.updatePlatformStatePill.textContent = `${label} database metadata`;
    elements.updatePlatformStateCopy.textContent = "";
    elements.updatePlatformLatestWeek.textContent = databaseMeta.metadata_fetched_at
      ? `Metadata loaded: ${formatDatabaseUpdatedAtLabel(databaseMeta.metadata_fetched_at)}`
      : "";
    elements.updatePlatformCrawlableCount.previousElementSibling.textContent = "Update Period Coverage";
    elements.updatePlatformReadyCount.previousElementSibling.textContent = "Updated Posts in Database";
    elements.updatePlatformFutureCount.previousElementSibling.textContent = "Latest Updated Date";
    elements.updatePlatformCrawlableCount.innerHTML = formatCoverageLabelMarkup(
      databaseMeta.coverage_start_date || "",
      databaseMeta.coverage_end_date || ""
    );
    elements.updatePlatformReadyCount.textContent = formatNumber(databaseMeta.updated_posts_count || 0);
    elements.updatePlatformFutureCount.textContent = formatDatabaseUpdatedDateLabel(databaseMeta.latest_updated_date || "");
    return;
  }

  const completedCount = sourceWindows.filter((item) => getFilterWindowStatus(item) === "completed").length;
  const updateCount = sourceWindows.filter((item) => getFilterWindowStatus(item) === "to_be_updated").length;
  const futureCount = sourceWindows.filter((item) => getFilterWindowStatus(item) === "future").length;
  elements.updatePlatformCrawlableCount.classList.remove("metric-value-coverage");
  elements.updatePlatformReadyCount.classList.remove("metric-value-coverage");
  elements.updatePlatformFutureCount.classList.remove("metric-value-coverage");
  elements.updatePlatformStatePill.textContent = `${label} filter overview`;
  elements.updatePlatformStateCopy.textContent = `Pick one ${isMonthlyMode() ? "month" : "week"} from ${label}. Date badges here only show whether imported database data already exists for that window.`;
  elements.updatePlatformLatestWeek.textContent = latestFinishedWeek
    ? `Latest available window: ${formatSelectedWindowLabel(latestFinishedWeek)}`
    : "Latest available window: none yet";
  elements.updatePlatformCrawlableCount.previousElementSibling.textContent = "Completed";
  elements.updatePlatformReadyCount.previousElementSibling.textContent = "To Update";
  elements.updatePlatformFutureCount.previousElementSibling.textContent = "Future";
  elements.updatePlatformCrawlableCount.textContent = formatNumber(completedCount);
  elements.updatePlatformReadyCount.textContent = formatNumber(updateCount);
  elements.updatePlatformFutureCount.textContent = formatNumber(futureCount);
}

function getSortMetricLabel(metric) {
  return (
    {
      heat_score: "heat score",
      post_count: "post count",
      engagement_total: "engagement count",
      discussion_total: "discussion count",
      unique_authors: "unique authors",
    }[metric] || "the selected metric"
  );
}

function syncTrendLink() {
  elements.trendPageLink.href = buildUrlWithSession(TRENDS_ROUTE, {
    event: state.selectedEvent,
    platform: state.platform,
    window_mode: state.windowMode,
    quarter_key: isQuarterlyMode() ? state.selectedWeek?.quarter_key : "",
    month_key: isMonthlyMode() ? state.selectedWeek?.month_key : "",
    week_start: !isQuarterlyMode() ? state.selectedWeek?.week_start : "",
    week_end: !isQuarterlyMode() ? state.selectedWeek?.week_end : "",
  });
  if (elements.backToMarketLink) {
    elements.backToMarketLink.href = buildUrlWithSession(MARKET_ROUTE);
  }
}

function setSelectedWeek(week) {
  state.selectedWeek = week || null;
  if (elements.activeWindowEyebrow) {
    elements.activeWindowEyebrow.textContent = isQuarterlyMode() ? "Current Quarter" : isMonthlyMode() ? "Current Month" : "Current Week";
  }
  if (elements.activeWeekLabel) {
    elements.activeWeekLabel.textContent = formatSelectedWindowLabel(state.selectedWeek);
  }
  updateActionButtons();
  syncTrendLink();
}

function updateActionButtons() {
  const status = state.selectedWeek?.status || "";
  const selectedWeekLabel = formatSelectedWindowLabel(state.selectedWeek);
  const latestJob = getLatestPlatformJob() || getAnyActiveUpdateJob();
  const latestJobStatus = latestJob?.status || "";
  const latestJobActive = hasActiveUpdateJob();
  const importCandidateWindow = getImportCandidateWindow();
  const selectedUpdateWindow = getSelectedWeeklyUpdateWindow() || importCandidateWindow;
  const selectedUpdateStatus = getUpdateWindowStatus(importCandidateWindow);
  if (isQuarterlyMode()) {
    const quarterlyStatus = state.selectedWeek?.status || "";
    elements.sidebarRunAnalysisButton.disabled = true;
    elements.updateDatabaseButton.disabled = latestJobActive;
    elements.cancelUpdateJobButton.disabled = !latestJobActive;
    elements.confirmImportButton.disabled = !(
      !latestJobActive &&
      importCandidateWindow &&
      selectedUpdateStatus === "ready_to_import"
    );
    elements.openSnapshotCalendarButton.disabled = false;
    elements.openSnapshotCalendarButton.textContent = "Pick Quarter";
    elements.snapshotFilterLabel.textContent = "Date Range";
    if (elements.snapshotFilterCopy) {
      elements.snapshotFilterCopy.textContent = "";
    }
    if (!state.selectedWeek) {
      setStatus("Select a quarter", "Choose one calendar quarter to load the aggregated leaderboard.");
    } else if (quarterlyStatus === "completed") {
      setStatus(
        "Quarterly view ready",
        `${selectedWeekLabel} is aggregated from the monthly snapshots already available in this quarter.`
      );
    } else if (quarterlyStatus === "to_be_analyzed") {
      setStatus(
        "Quarterly data is partial",
        `${selectedWeekLabel} already has raw or extracted posts, but not every month inside this quarter has a completed cluster snapshot yet.`
      );
    } else if (quarterlyStatus === "to_be_updated") {
      setStatus(
        "Quarterly data is waiting",
        `${selectedWeekLabel} does not yet have imported posts inside this quarter, so the quarterly leaderboard is still empty.`
      );
    } else {
      setStatus("Quarterly view", state.calendarNotice || "Quarterly panels aggregate the monthly snapshots available in this quarter.");
    }
    if (latestJobStatus === "queued" || latestJobStatus === "running" || latestJobStatus === "cancelling") {
      setStatus(
        latestJobStatus === "cancelling" ? "Stopping crawl" : "Crawling in progress",
        `${formatJobWeekLabel(latestJob)} is currently reserved for a weekly crawl. Quarterly cards stay available while the crawl runs in the background.`
      );
    } else if (selectedUpdateStatus === "ready_to_import") {
      setStatus(
        "Ready to import",
        `${formatWeekLabel(importCandidateWindow.week_start, importCandidateWindow.week_end)} has staged crawl files waiting for confirmation before they enter the database.`
      );
    }
    renderCrawlMonitor();
    return;
  }
  elements.sidebarRunAnalysisButton.disabled =
    latestJobActive || !state.selectedWeek || !new Set(["to_be_analyzed", "completed"]).has(status);
  elements.updateDatabaseButton.disabled = latestJobActive;
  elements.cancelUpdateJobButton.disabled = !latestJobActive;
  elements.confirmImportButton.disabled = !(
    !latestJobActive &&
    importCandidateWindow &&
    selectedUpdateStatus === "ready_to_import"
  );
  elements.openSnapshotCalendarButton.disabled = false;
  elements.openSnapshotCalendarButton.textContent = isMonthlyMode() ? "Pick Month" : "Pick Week";
  elements.snapshotFilterLabel.textContent = "Date Range";
  if (elements.snapshotFilterCopy) {
    elements.snapshotFilterCopy.textContent = "";
  }

  if (!state.selectedWeek) {
    if (latestJobStatus === "awaiting_confirmation") {
      const summaryWindow = latestJob?.summary?.window || null;
      const stagedCount = Number(summaryWindow?.staged_file_count || 0);
      setStatus(
        "Ready to import",
        `${formatJobWeekLabel(latestJob)} finished crawling for ${getJobPlatformLabel(latestJob)}. ${formatNumber(
          stagedCount
        )} staged file${stagedCount === 1 ? "" : "s"} are waiting for confirmation.`
      );
      renderCrawlMonitor();
      return;
    }
    setStatus(
      isMonthlyMode() ? "Select a month" : "Select a week",
      isMonthlyMode()
        ? "Choose one monthly snapshot from the left-side date filter."
        : "Choose one weekly snapshot from the left-side date filter."
    );
    renderCrawlMonitor();
    return;
  }

  if (latestJobStatus === "queued") {
    setStatus(
      "Crawl queued",
      `${formatJobWeekLabel(latestJob)} is queued for ${getJobPlatformLabel(latestJob)}.`
    );
    renderCrawlMonitor();
    return;
  }
  if (latestJobStatus === "running") {
    setStatus(
      "Crawling in progress",
      `${formatJobWeekLabel(latestJob)} is currently crawling ${getJobPlatformLabel(latestJob)}. You can stop this crawl before import.`
    );
    renderCrawlMonitor();
    return;
  }
  if (latestJobStatus === "cancelling") {
    setStatus(
      "Stopping crawl",
      `Cancelling the active ${getJobPlatformLabel(latestJob)} crawl for ${formatJobWeekLabel(latestJob)}.`
    );
    renderCrawlMonitor();
    return;
  }
  if (latestJobStatus === "awaiting_confirmation") {
    const summaryWindow = latestJob?.summary?.window || selectedUpdateWindow;
    const stagedCount = Number(summaryWindow?.staged_file_count || 0);
    setStatus(
      "Ready to import",
      `${formatJobWeekLabel(latestJob)} finished crawling. ${formatNumber(stagedCount)} staged file${
        stagedCount === 1 ? "" : "s"
      } are waiting for confirmation before they enter the database.`
    );
    renderCrawlMonitor();
    return;
  }
  if (latestJobStatus === "cancelled") {
    setStatus(
      "Crawl cancelled",
      `${formatJobWeekLabel(latestJob)} was stopped before import confirmation. Existing database data was not changed.`
    );
    renderCrawlMonitor();
    return;
  }
  if (selectedUpdateStatus === "ready_to_import") {
    const stagedCount = Number(importCandidateWindow?.staged_file_count || 0);
    const readyWeekLabel = importCandidateWindow
      ? formatWeekLabel(importCandidateWindow.week_start, importCandidateWindow.week_end)
      : selectedWeekLabel;
    setStatus(
      "Ready to import",
      `${readyWeekLabel} already has ${formatNumber(stagedCount)} staged file${
        stagedCount === 1 ? "" : "s"
      } waiting for confirmation. Click Confirm Import when you want them written into the database.`
    );
    renderCrawlMonitor();
    return;
  }

  if (status === "completed") {
    setStatus(
      "Completed",
      `${selectedWeekLabel} already has analyzed ${state.boardType} clusters. You can inspect the leaderboard or rerun analysis for this ${
        isMonthlyMode() ? "month" : "week"
      }.`
    );
  } else if (status === "to_be_analyzed") {
    setStatus(
      "To Be Analyzed",
      `${selectedWeekLabel} is already updated in the database. Click Run Analysis to build ${
        isMonthlyMode() ? "monthly" : "weekly"
      } clusters.`
    );
  } else if (status === "to_be_updated") {
    setStatus(
      "To Be Updated",
      isMonthlyMode()
        ? `${selectedWeekLabel} has no stored raw posts yet. Monthly analysis will become available after the related weeks are imported.`
        : `${selectedWeekLabel} has no stored raw posts yet. Click Update Database to crawl and ingest this fixed weekly window first.`
    );
  }
  renderCrawlMonitor();
}

function renderOverview(items) {
  clearNode(elements.heatOverviewGrid);
  if (isQuarterlyMode()) {
    const totalEngagement = items.reduce((sum, item) => sum + Number(item.total_engagement || 0), 0);
    const totalDiscussion = items.reduce((sum, item) => sum + Number(item.discussion_total || 0), 0);
    const totalPosts = items.reduce((sum, item) => sum + Number(item.post_count || 0), 0);
    const averagePerPost = totalPosts > 0 ? Math.round(totalEngagement / totalPosts) : 0;
    const topItem = items[0];
    const cards = [
      {
        key: "platform",
        label: "Platform",
        value: PLATFORM_LABELS[state.platform] || "Unknown",
        sub: "",
      },
      {
        key: "selected-range",
        label: "Selected Range",
        value: state.selectedWeek?.quarter_key || "No quarter selected",
        sub: "",
      },
      {
        key: "monthly-posts",
        label: "Quarter Posts",
        value: formatNumber(state.selectedWeek?.post_count || totalPosts),
        sub: "",
      },
      {
        key: "top-title",
        label: state.boardType === "topic" ? "Top Topic" : "Top Event",
        value: clipText(getClusterDisplayText(topItem, "No cluster"), 34),
        subHtml: topItem
          ? `<span class="overview-heat-flames">${getHeatTone(Number(topItem.heat_score || 0)).flames}</span><span>Heat ${formatScore(
              topItem.heat_score
            )} / 100</span>`
          : "",
      },
      {
        key: "engagement",
        label: "Engagement",
        value: formatNumber(totalEngagement),
        sub: "",
      },
      {
        key: "discussion",
        label: "Discussion",
        value: formatNumber(totalDiscussion),
        sub: "",
      },
      {
        key: "posts",
        label: "Posts",
        value: formatNumber(totalPosts),
        sub: "",
      },
      {
        key: "avg-per-post",
        label: "Avg. Per Post",
        value: formatNumber(averagePerPost),
        sub: "",
      },
    ];
    cards.forEach((card) => {
      const node = document.createElement("article");
      node.className = `overview-card overview-card-${card.key}`;
      node.innerHTML = `
        <span>${card.label}</span>
        <strong>${card.value}</strong>
        ${card.subHtml ? `<small class="overview-rich-sub">${card.subHtml}</small>` : card.sub ? `<small>${card.sub}</small>` : ""}
      `;
      elements.heatOverviewGrid.appendChild(node);
    });
    return;
  }
  const totalEngagement = items.reduce((sum, item) => sum + Number(item.total_engagement || 0), 0);
  const totalDiscussion = items.reduce((sum, item) => sum + Number(item.discussion_total || 0), 0);
  const totalPosts = items.reduce((sum, item) => sum + Number(item.post_count || 0), 0);
  const averagePerPost = totalPosts > 0 ? Math.round(totalEngagement / totalPosts) : 0;
  const topItem = items[0];
  const cards = [
    {
      key: "platform",
      label: "Platform",
      value: PLATFORM_LABELS[state.platform] || "Unknown",
      sub: "",
    },
    {
      key: "selected-range",
      label: "Selected Range",
      value: formatSelectedWindowLabel(state.selectedWeek),
      sub: "",
    },
    {
      key: "monthly-posts",
      label: isMonthlyMode() ? "Monthly Posts" : "Weekly Posts",
      value: formatNumber(state.selectedWeek?.post_count || 0),
      sub: "",
    },
    {
      key: "top-title",
      label: state.boardType === "topic" ? "Top Topic" : "Top Event",
      value: clipText(getClusterDisplayText(topItem, "No cluster"), 34),
      subHtml: topItem
        ? `<span class="overview-heat-flames">${getHeatTone(Number(topItem.heat_score || 0)).flames}</span><span>Heat ${formatScore(
            topItem.heat_score
          )} / 100</span>`
        : "",
    },
    {
      key: "engagement",
      label: "Engagement",
      value: formatNumber(totalEngagement),
      sub: "",
    },
    {
      key: "discussion",
      label: "Discussion",
      value: formatNumber(totalDiscussion),
      sub: "",
    },
    {
      key: "posts",
      label: "Posts",
      value: formatNumber(totalPosts),
      sub: "",
    },
    {
      key: "avg-per-post",
      label: "Avg. Per Post",
      value: formatNumber(averagePerPost),
      sub: "",
    },
  ];

  cards.forEach((card) => {
    const node = document.createElement("article");
    node.className = `overview-card overview-card-${card.key}`;
    node.innerHTML = `
      <span>${card.label}</span>
      <strong>${card.value}</strong>
      ${card.subHtml ? `<small class="overview-rich-sub">${card.subHtml}</small>` : card.sub ? `<small>${card.sub}</small>` : ""}
    `;
    elements.heatOverviewGrid.appendChild(node);
  });
}

function renderLeaderboard(items) {
  clearNode(elements.leaderboardTableBody);
  state.lastRenderedItems = items;
  hideClusterContextMenu();
  if (!items.length) {
    elements.leaderboardTableBody.appendChild(elements.emptyTemplate.content.cloneNode(true));
    elements.leaderboardCounter.textContent = "0 rows";
    return;
  }

  elements.leaderboardCounter.textContent = `${formatNumber(items.length)} rows`;
  items.forEach((item, index) => {
    const tone = getHeatTone(Number(item.heat_score || 0));
    const row = document.createElement("tr");
    row.className = "leaderboard-row-focus";
    row.tabIndex = 0;
    row.innerHTML = `
      <td class="leaderboard-rank-cell"><span class="rank-pill">${index + 1}</span></td>
      <td class="heat-title-cell">
        <strong>${getClusterDisplayText(item, item.cluster_key || "")}</strong>
      </td>
      <td class="leaderboard-metric-cell">${PLATFORM_LABELS[item.platform] || item.platform || "-"}</td>
      <td class="leaderboard-metric-cell">${formatNumber(item.post_count)}</td>
      <td class="leaderboard-metric-cell">${formatNumber(item.total_engagement)}</td>
      <td class="leaderboard-metric-cell">${formatNumber(item.discussion_total)}</td>
      <td class="leaderboard-metric-cell">${formatNumber(item.unique_authors)}</td>
      <td class="heat-score-cell ${tone.className}">
        <div class="heat-score-shell">
          <strong>${formatScore(item.heat_score)}</strong>
          <span class="heat-flames ${tone.flames ? "has-flames" : "no-flames"}" aria-hidden="true">${tone.flames || ""}</span>
        </div>
      </td>
    `;
    row.addEventListener("click", () => {
      state.selectedEvent = item.cluster_key || "";
      syncTrendLink();
    });
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        state.selectedEvent = item.cluster_key || "";
        syncTrendLink();
      }
    });
    row.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      state.contextRow = item;
      showClusterContextMenu(event.clientX, event.clientY);
    });
    elements.leaderboardTableBody.appendChild(row);
  });
}

function openHeatFormulaModal() {
  elements.heatFormulaModal?.classList.remove("hidden");
  elements.heatFormulaModal?.setAttribute("aria-hidden", "false");
}

function closeHeatFormulaModal() {
  elements.heatFormulaModal?.classList.add("hidden");
  elements.heatFormulaModal?.setAttribute("aria-hidden", "true");
}

function syncLeaderboardCopy() {
  if (isQuarterlyMode()) {
    elements.leaderboardTitle.textContent = `Quarterly ${getBoardTypeLabel()} Leaderboard`;
    if (elements.leaderboardSubtitle) {
      elements.leaderboardSubtitle.textContent = "";
    }
    return;
  }
  if (state.boardType === "topic") {
    elements.leaderboardTitle.textContent = "Topic Leaderboard";
    if (elements.leaderboardSubtitle) {
      elements.leaderboardSubtitle.textContent = "";
    }
    return;
  }
  elements.leaderboardTitle.textContent = "Event Leaderboard";
  if (elements.leaderboardSubtitle) {
    elements.leaderboardSubtitle.textContent = "";
  }
}

function renderSnapshotWindowList() {
  clearNode(elements.snapshotWindowList);
  const visibleWindows = state.windows.filter((item) => {
    if (!isMonthlyMode() && !isQuarterlyMode() && getFilterWindowStatus(item) === "future") {
      return false;
    }
    return true;
  });
  if (!visibleWindows.length) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent = isQuarterlyMode()
      ? "No quarterly windows were found for the selected platform."
      : isMonthlyMode()
      ? "No monthly windows were found for the selected platform."
      : "No weekly windows were found for the selected platform.";
    elements.snapshotWindowList.appendChild(empty);
    return;
  }

  visibleWindows.forEach((item) => {
    const statusMeta = getFilterStatusMeta(item);
    const isSelected = isQuarterlyMode()
      ? state.selectedWeek?.quarter_key === item.quarter_key
      : isMonthlyMode()
      ? state.selectedWeek?.month_key === item.month_key
      : state.selectedWeek?.week_start === item.week_start && state.selectedWeek?.week_end === item.week_end;
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
        `Loading ${isQuarterlyMode() ? item.quarter_key : isMonthlyMode() ? formatMonthLabel(item.month_key) : `${item.week_start} to ${item.week_end}`} for the current leaderboard view.`
      );
      setSelectedWeek(item);
      renderSnapshotWindowList();
      await fetchLeaderboardData();
    });
    elements.snapshotWindowList.appendChild(button);
  });
}

function openCalendar() {
  syncCalendarModal();
  renderCalendar();
  elements.updateCalendarModal.classList.remove("hidden");
  elements.updateCalendarModal.setAttribute("aria-hidden", "false");
}

function closeCalendar() {
  elements.updateCalendarModal.classList.add("hidden");
  elements.updateCalendarModal.setAttribute("aria-hidden", "true");
}

function hideClusterContextMenu() {
  elements.clusterContextMenu?.classList.add("hidden");
  elements.clusterContextMenu?.setAttribute("aria-hidden", "true");
}

function syncFeedbackCopy() {
  const boardLabel = getBoardTypeLabel();
  const lowerBoardLabel = boardLabel.toLowerCase();
  if (elements.clusterMarkNoiseButton) {
    elements.clusterMarkNoiseButton.textContent = `Mark ${boardLabel} as Noise`;
  }
  if (elements.clusterMergeButton) {
    elements.clusterMergeButton.textContent = `Merge Into Existing ${boardLabel}`;
  }
  if (elements.clusterMergeTitle) {
    elements.clusterMergeTitle.textContent = `Merge ${boardLabel}`;
  }
  if (elements.clusterMergeHelper) {
    elements.clusterMergeHelper.textContent = `Choose an existing ${lowerBoardLabel} to merge this cluster into. The leaderboard will treat them as one ${lowerBoardLabel} inside the selected window.`;
  }
  if (elements.clusterMergeSourceHeading) {
    elements.clusterMergeSourceHeading.textContent = `Source ${boardLabel}`;
  }
  if (elements.clusterMergeTargetHeading) {
    elements.clusterMergeTargetHeading.textContent = `Merge Into ${boardLabel}`;
  }
  if (elements.clusterNoiseTitle) {
    elements.clusterNoiseTitle.textContent = `Mark ${boardLabel} as Noise`;
  }
  if (elements.clusterNoiseHelper) {
    elements.clusterNoiseHelper.textContent = `Confirm that this ${lowerBoardLabel} should be treated as noise inside the selected window. It will be removed from the leaderboard and heat calculations for this scope.`;
  }
  if (elements.clusterNoiseSourceHeading) {
    elements.clusterNoiseSourceHeading.textContent = `Selected ${boardLabel}`;
  }
}

function showClusterContextMenu(clientX, clientY) {
  if (!elements.clusterContextMenu) {
    return;
  }
  syncFeedbackCopy();
  const menu = elements.clusterContextMenu;
  menu.style.left = `${clientX}px`;
  menu.style.top = `${clientY}px`;
  menu.classList.remove("hidden");
  menu.setAttribute("aria-hidden", "false");
}

function openClusterMergeModal() {
  if (!state.contextRow) {
    return;
  }
  syncFeedbackCopy();
  elements.clusterMergeSourceLabel.textContent = getClusterDisplayText(state.contextRow, "No event selected");
  clearNode(elements.clusterMergeTargetSelect);
  const options = state.lastRenderedItems
    .filter((item) => item.cluster_key !== state.contextRow.cluster_key)
    .map((item) => ({
      value: item.cluster_key || "",
      label: getClusterDisplayText(item, item.cluster_key || ""),
    }));
  options.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = clipText(item.label, 48);
    option.title = item.label;
    elements.clusterMergeTargetSelect.appendChild(option);
  });
  elements.confirmClusterMergeButton.disabled = options.length === 0;
  elements.clusterMergeModal.classList.remove("hidden");
  elements.clusterMergeModal.setAttribute("aria-hidden", "false");
}

function closeClusterMergeModal() {
  elements.clusterMergeModal.classList.add("hidden");
  elements.clusterMergeModal.setAttribute("aria-hidden", "true");
}

function openClusterNoiseModal() {
  if (!state.contextRow) {
    return;
  }
  syncFeedbackCopy();
  if (elements.clusterNoiseSourceLabel) {
    elements.clusterNoiseSourceLabel.textContent = getClusterDisplayText(state.contextRow, "No selection");
  }
  elements.clusterNoiseModal.classList.remove("hidden");
  elements.clusterNoiseModal.setAttribute("aria-hidden", "false");
}

function closeClusterNoiseModal() {
  elements.clusterNoiseModal.classList.add("hidden");
  elements.clusterNoiseModal.setAttribute("aria-hidden", "true");
}

async function submitClusterFeedback(action, targetClusterKey = "") {
  if (!state.contextRow) {
    return;
  }
  const query = new URLSearchParams({
    platform: state.platform,
    board_type: state.boardType,
    action,
    source_cluster_key: state.contextRow.cluster_key || "",
  });
  if (isMonthlyMode()) {
    query.set("month_key", state.selectedWeek?.month_key || "");
  } else if (isQuarterlyMode()) {
    query.set("quarter_key", state.selectedWeek?.quarter_key || "");
  } else {
    query.set("week_start", state.selectedWeek?.week_start || "");
    query.set("week_end", state.selectedWeek?.week_end || "");
  }
  if (targetClusterKey) {
    query.set("target_cluster_key", targetClusterKey);
  }
  setPanelBusy(true, "Applying feedback...", "Updating the current cluster view based on your feedback.");
  try {
    await postJson(`${API_BASE}/cluster-feedback?${query.toString()}`);
    await fetchLeaderboardData();
  } finally {
    setPanelBusy(false);
    hideClusterContextMenu();
    closeClusterMergeModal();
    closeClusterNoiseModal();
  }
}

function startOfWeek(dateValue) {
  const copy = new Date(dateValue);
  const diff = copy.getDate() - copy.getDay();
  copy.setDate(diff);
  copy.setHours(0, 0, 0, 0);
  return copy;
}

function endOfWeek(dateValue) {
  const copy = startOfWeek(dateValue);
  copy.setDate(copy.getDate() + 6);
  return copy;
}

function toIsoDate(dateValue) {
  return `${dateValue.getFullYear()}-${String(dateValue.getMonth() + 1).padStart(2, "0")}-${String(dateValue.getDate()).padStart(2, "0")}`;
}

function formatCalendarSelectionDetail(week) {
  if (!week) {
    return getCalendarModeConfig().emptyDetail;
  }

  if (state.calendarMode === "update") {
    const updateStatus = getUpdateWindowStatus(week);
    if (updateStatus === "future") {
      return `${formatWeekLabel(week.week_start, week.week_end)} is still in the future and cannot be crawled yet.`;
    }
    if (updateStatus === "ready_to_import") {
      return `${formatWeekLabel(week.week_start, week.week_end)} already has staged crawl files. Confirm import when you are ready to write them into the database.`;
    }
    if (updateStatus === "to_be_updated") {
      return `${formatWeekLabel(week.week_start, week.week_end)} has not been imported yet for ${getUpdatePlatformLabel()}. Start Crawl to fetch this fixed week into staging first.`;
    }
    if (new Set(["to_be_analyzed", "completed"]).has(updateStatus)) {
      return `${formatWeekLabel(week.week_start, week.week_end)} already has database data for ${getUpdatePlatformLabel()}. You can crawl again if you want to refresh that week.`;
    }
    return `${formatWeekLabel(week.week_start, week.week_end)} can be crawled again for ${getUpdatePlatformLabel()}.`;
  }

  if (isQuarterlyMode()) {
    const statusMeta = STATUS_META[week.status] || STATUS_META.to_be_updated;
    return week.note || `${statusMeta.label}. Quarterly view aggregates the months that currently belong to this quarter.`;
  }

  const statusMeta = getFilterStatusMeta(week);
  return `${statusMeta.label}. ${statusMeta.detail}`;
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
  if (state.calendarMode === "filter" && isMonthlyMode()) {
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
  const sorted = [...keys].sort((left, right) => left.localeCompare(right));
  const useAscending = state.calendarMode === "update" || (state.calendarMode === "filter" && !isMonthlyMode());
  return useAscending ? sorted : [...sorted].reverse();
}

function syncCalendarSelectionSummary() {
  const config = getCalendarModeConfig();
  elements.calendarSelectionLabel.textContent = state.calendarSelectedWeek
    ? formatCalendarSelectedWindowLabel(state.calendarSelectedWeek)
    : config.emptyLabel;
  elements.calendarSelectionDetail.textContent =
    state.calendarMode === "update" ? "" : formatCalendarSelectionDetail(state.calendarSelectedWeek);
  const selectedStatus =
    state.calendarMode === "update"
      ? getUpdateWindowStatus(state.calendarSelectedWeek)
      : state.calendarSelectedWeek?.status;
  elements.confirmUpdateButton.disabled = !(
    state.calendarSelectedWeek &&
    config.selectableStatuses.has(selectedStatus) &&
    !(state.calendarMode === "update" && hasActiveUpdateJob())
  );
}

function renderCalendarLegend() {
  clearNode(elements.calendarLegend);
  getCalendarModeConfig().legend.forEach((item) => {
    const node = document.createElement("span");
    node.className = "calendar-legend-item";
    node.innerHTML = `<span class="calendar-swatch ${item.className}"></span>${item.label}`;
    elements.calendarLegend.appendChild(node);
  });
}

function syncCalendarModal() {
  const config = getCalendarModeConfig();
  const isUpdateMode = state.calendarMode === "update";
  elements.updateCalendarPanel?.setAttribute("data-mode", isUpdateMode ? "update" : "filter");
  elements.updatePlatformRow?.classList.toggle("hidden", !isUpdateMode);
  elements.calendarModeEyebrow.textContent = config.eyebrow;
  elements.calendarModeTitle.textContent = config.title;
  elements.calendarModeHelper.textContent = isUpdateMode ? "" : config.helper;
  elements.confirmUpdateButton.textContent = config.confirmLabel;
  if (elements.calendarToolbar) {
    elements.calendarToolbar.classList.add("hidden");
  }
  elements.calendarWeekdayRow.classList.add("hidden");
  renderUpdatePlatformState();
  renderCalendarLegend();
  syncCalendarSelectionSummary();
}

function setCalendarMode(mode) {
  const previousMode = state.calendarMode;
  const previousUpdateSelection =
    previousMode === "update" ? getUpdateWindowForWeek(state.calendarSelectedWeek) || getUpdateWindowForWeek(state.lastUpdateSelection) : null;
  state.calendarMode = mode;
  state.calendarScrollTop = 0;
  const sourceWindows = mode === "update" ? state.updateWindows : state.windows;
  if (mode === "update") {
    const selectedMatch =
      previousUpdateSelection ||
      getUpdateWindowForWeek(state.lastUpdateSelection) ||
      getUpdateWindowForWeek(state.selectedWeek) ||
      null;
    state.calendarSelectedWeek =
      selectedMatch ||
      sourceWindows.find((item) => getUpdateWindowStatus(item) === "ready_to_import") ||
      sourceWindows.find((item) => getUpdateWindowStatus(item) !== "future") ||
      null;
    state.lastUpdateSelection = state.calendarSelectedWeek
      ? { week_start: state.calendarSelectedWeek.week_start, week_end: state.calendarSelectedWeek.week_end }
      : state.lastUpdateSelection;
  } else {
    state.calendarSelectedWeek = state.selectedWeek;
  }
  syncCalendarModal();
}

function renderCalendar() {
  const preservedScrollTop = elements.calendarGrid?.scrollTop || state.calendarScrollTop || 0;
  clearNode(elements.calendarWeekdayRow);
  clearNode(elements.calendarGrid);
  const isFilterQuarterly = state.calendarMode === "filter" && isQuarterlyMode();
  if (isFilterQuarterly) {
    elements.calendarGrid.classList.toggle("calendar-month-grid", false);
    elements.calendarGrid.classList.toggle("calendar-week-grid", false);
    elements.calendarGrid.classList.add("calendar-scroll-grid");
    elements.calendarMonthLabel.textContent = "Available Quarters";
    if (!state.windows.length) {
      const empty = document.createElement("div");
      empty.className = "snapshot-window-empty";
      empty.textContent = "No quarterly windows are available for this platform.";
      elements.calendarGrid.appendChild(empty);
      syncCalendarSelectionSummary();
      return;
    }
    const section = document.createElement("section");
    section.className = "calendar-month-section";
    section.innerHTML = `<h3 class="calendar-section-title">Calendar Quarters</h3>`;
    const grid = document.createElement("div");
    grid.className = "calendar-section-grid calendar-section-grid-months";
    state.windows.forEach((item) => {
      const statusMeta = getFilterStatusMeta(item);
      const isSelected = state.calendarSelectedWeek?.quarter_key === item.quarter_key;
      const button = document.createElement("button");
      button.type = "button";
      button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
        button.innerHTML = `
        <span class="calendar-day-month">${item.quarter_key}</span>
        <span class="calendar-day-status-text">${statusMeta.label}</span>
        ${item.note ? `<span class="calendar-day-helper">${item.note}</span>` : ""}
      `;
      if (getCalendarModeConfig().selectableStatuses.has(item.status)) {
        button.addEventListener("click", () => {
          state.calendarScrollTop = elements.calendarGrid?.scrollTop || 0;
          state.calendarSelectedWeek = item;
          syncCalendarSelectionSummary();
          renderCalendar();
        });
      } else {
        button.disabled = true;
      }
      grid.appendChild(button);
    });
    section.appendChild(grid);
    elements.calendarGrid.appendChild(section);
    syncCalendarSelectionSummary();
    elements.calendarGrid.scrollTop = preservedScrollTop;
    return;
  }
  const useMonthPicker = state.calendarMode === "filter" && isMonthlyMode();
  const useDailyWeekPicker = state.calendarMode === "filter" && !isMonthlyMode();
  const sourceWindows = state.calendarMode === "update" ? state.updateWindows : state.windows;
  const monthKeys = getWindowMonthKeys(sourceWindows);
  elements.calendarGrid.classList.toggle("calendar-month-grid", false);
  elements.calendarGrid.classList.toggle("calendar-week-grid", false);
  elements.calendarGrid.classList.add("calendar-scroll-grid");
  elements.calendarMonthLabel.textContent = useMonthPicker ? "Available Months" : "Available Weeks";

  if (state.calendarMode === "update" || useDailyWeekPicker) {
    elements.calendarMonthLabel.textContent = state.calendarMode === "update" ? `${getUpdatePlatformLabel()} Crawl Calendar` : "Weekly Snapshot Calendar";
    if (!monthKeys.length) {
      const empty = document.createElement("div");
      empty.className = "snapshot-window-empty";
      empty.textContent = state.calendarMode === "update"
        ? `No weekly windows are available for ${getUpdatePlatformLabel()}.`
        : "No weekly windows are available for this platform.";
      elements.calendarGrid.appendChild(empty);
      syncCalendarSelectionSummary();
      return;
    }

    monthKeys.forEach((monthKey) => {
      const [yearText, monthText] = String(monthKey).split("-");
      const year = Number(yearText);
      const month = Number(monthText);
      if (!year || !month) {
        return;
      }

      const section = document.createElement("section");
      section.className = "calendar-month-section calendar-month-section-daily";
      section.innerHTML = `
        ${state.calendarMode === "update" ? "" : `<p class="calendar-section-kicker">Showing ${PLATFORM_LABELS[state.platform] || "Unknown"} weekly windows</p>`}
        <h3 class="calendar-section-title">${formatMonthHeading(monthKey)}</h3>
      `;

      const weekdayRow = document.createElement("div");
      weekdayRow.className = "calendar-month-weekday-row";
      WEEKDAY_LABELS.forEach((label) => {
        const node = document.createElement("span");
        node.className = "calendar-month-weekday";
        node.textContent = label;
        weekdayRow.appendChild(node);
      });
      section.appendChild(weekdayRow);

      const grid = document.createElement("div");
      grid.className = "calendar-date-grid";

      const firstDay = new Date(year, month - 1, 1);
      const leadingBlanks = firstDay.getDay();
      for (let index = 0; index < leadingBlanks; index += 1) {
        const blank = document.createElement("div");
        blank.className = "calendar-day-blank";
        grid.appendChild(blank);
      }

      const lastDay = new Date(year, month, 0).getDate();
      for (let day = 1; day <= lastDay; day += 1) {
        const dateValue = new Date(year, month - 1, day);
        const dateKey = toIsoDate(dateValue);
        const week = sourceWindows.find((item) => item.week_start <= dateKey && item.week_end >= dateKey) || null;
        const forcedFirstWeekUpdated =
          state.calendarMode === "update" && !week && dateKey >= "2026-01-01" && dateKey <= "2026-01-03";
        const calendarStatus = forcedFirstWeekUpdated
          ? "completed"
          : week
          ? (state.calendarMode === "update" ? getUpdateWindowStatus(week) : getFilterWindowStatus(week))
          : "future";
        const statusMeta = state.calendarMode === "update"
          ? (UPDATE_CALENDAR_META[calendarStatus] || UPDATE_CALENDAR_META.future)
          : (FILTER_STATUS_META[calendarStatus] || FILTER_STATUS_META.future);
        const isSelected =
          week &&
          state.calendarSelectedWeek?.week_start === week.week_start &&
          state.calendarSelectedWeek?.week_end === week.week_end;
        const isSelectable =
          week &&
          getCalendarModeConfig().selectableStatuses.has(calendarStatus) &&
          !(state.calendarMode === "update" && hasActiveUpdateJob());

        const button = document.createElement("button");
        button.type = "button";
        button.className = `calendar-day calendar-day-compact ${statusMeta.className}${isSelected ? " selected-week" : ""}${state.calendarMode === "filter" ? " calendar-day-filter" : ""}`;
        button.innerHTML = `
          <span class="calendar-day-number">${String(day)}</span>
          <span class="calendar-day-status-text">${statusMeta.label}</span>
        `;
        button.title = week
          ? `${formatWeekLabel(week.week_start, week.week_end)} • ${statusMeta.label}`
          : forcedFirstWeekUpdated
          ? `${dateKey} • Updated`
          : `${dateKey} • Future`;

        if (isSelectable) {
          button.addEventListener("click", () => {
            state.calendarScrollTop = elements.calendarGrid?.scrollTop || 0;
            state.calendarSelectedWeek = week;
            if (state.calendarMode === "update") {
              state.lastUpdateSelection = { week_start: week.week_start, week_end: week.week_end };
            }
            syncCalendarSelectionSummary();
            renderCalendar();
          });
        } else {
          button.disabled = true;
        }
        grid.appendChild(button);
      }

      const totalCells = leadingBlanks + lastDay;
      const trailingBlanks = (7 - (totalCells % 7)) % 7;
      for (let index = 0; index < trailingBlanks; index += 1) {
        const blank = document.createElement("div");
        blank.className = "calendar-day-blank";
        grid.appendChild(blank);
      }

      section.appendChild(grid);
      elements.calendarGrid.appendChild(section);
    });

    syncCalendarSelectionSummary();
    elements.calendarGrid.scrollTop = preservedScrollTop;
    return;
  }

  if (useMonthPicker) {
    const section = document.createElement("section");
    section.className = "calendar-month-section";
    section.innerHTML = `<h3 class="calendar-section-title">Calendar Months</h3>`;
    const grid = document.createElement("div");
    grid.className = "calendar-section-grid calendar-section-grid-months";
    state.windows.forEach((item) => {
      const statusMeta = getFilterStatusMeta(item);
      const isSelected = state.calendarSelectedWeek?.month_key === item.month_key;
      const isSelectable = getCalendarModeConfig().selectableStatuses.has(item.status);
      const button = document.createElement("button");
      button.type = "button";
      button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
      button.innerHTML = `
        <span class="calendar-day-month">${formatMonthLabel(item.month_key)}</span>
      `;
      if (isSelectable) {
        button.addEventListener("click", () => {
          state.calendarScrollTop = elements.calendarGrid?.scrollTop || 0;
          state.calendarSelectedWeek = item;
          syncCalendarSelectionSummary();
          renderCalendar();
        });
      } else {
        button.disabled = true;
      }
      grid.appendChild(button);
    });
    section.appendChild(grid);
    elements.calendarGrid.appendChild(section);
    syncCalendarSelectionSummary();
    elements.calendarGrid.scrollTop = preservedScrollTop;
    return;
  }

  if (!monthKeys.length) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent = "No weekly windows are available for this platform.";
    elements.calendarGrid.appendChild(empty);
    syncCalendarSelectionSummary();
    return;
  }

  monthKeys.forEach((monthKey) => {
    const visibleWeeks = sourceWindows.filter((item) => monthOverlapsWindow(monthKey, item));
    if (!visibleWeeks.length) {
      return;
    }
    const section = document.createElement("section");
    section.className = "calendar-month-section";
    section.innerHTML = `<h3 class="calendar-section-title">${formatMonthHeading(monthKey)}</h3>`;
    const grid = document.createElement("div");
    grid.className = "calendar-section-grid";

    visibleWeeks.forEach((week) => {
      const calendarStatus =
        state.calendarMode === "update"
          ? getUpdateWindowStatus(week)
          : getFilterWindowStatus(week);
      const statusMeta =
        state.calendarMode === "update"
          ? UPDATE_CALENDAR_META[calendarStatus] || UPDATE_CALENDAR_META.future
          : FILTER_STATUS_META[calendarStatus] || FILTER_STATUS_META.future;
      const isSelected =
        state.calendarSelectedWeek?.week_start === week.week_start && state.calendarSelectedWeek?.week_end === week.week_end;
      const isSelectable = getCalendarModeConfig().selectableStatuses.has(calendarStatus);

      const button = document.createElement("button");
      button.type = "button";
      button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
      const selectedPill = isSelected ? '<span class="calendar-selected-pill">Selected</span>' : "";
      const statusPill = `<span class="calendar-status-pill ${statusMeta.className}">${statusMeta.label}</span>`;
      button.innerHTML = `
          <div class="calendar-week-card-head">
            ${statusPill}
            ${selectedPill}
          </div>
          <span class="calendar-week-range">${week.week_start} to ${week.week_end}</span>
        `;
      const updateModeLocked = state.calendarMode === "update" && hasActiveUpdateJob();
      if (isSelectable) {
        button.addEventListener("click", () => {
          state.calendarScrollTop = elements.calendarGrid?.scrollTop || 0;
          state.calendarSelectedWeek = week;
          if (state.calendarMode === "update") {
            state.lastUpdateSelection = { week_start: week.week_start, week_end: week.week_end };
          }
          syncCalendarSelectionSummary();
          renderCalendar();
        });
      } else {
        button.disabled = true;
      }
      if (updateModeLocked) {
        button.disabled = true;
      }
      grid.appendChild(button);
    });

    section.appendChild(grid);
    elements.calendarGrid.appendChild(section);
  });

  syncCalendarSelectionSummary();
  elements.calendarGrid.scrollTop = preservedScrollTop;
}

async function loadWindows() {
  const payload = await requestJson(
    `${API_BASE}/analysis-windows?platform=${encodeURIComponent(state.platform)}&weeks=24&window_mode=${encodeURIComponent(
      state.windowMode
    )}`
  );
  state.windows = payload.items || [];
  state.calendarNotice = payload.message || "";

  if (state.selectedWeek) {
    const matched = state.windows.find((item) =>
      isQuarterlyMode()
        ? item.quarter_key === state.selectedWeek.quarter_key
        : isMonthlyMode()
        ? item.month_key === state.selectedWeek.month_key
        : item.week_start === state.selectedWeek.week_start && item.week_end === state.selectedWeek.week_end
    );
    if (matched) {
      state.selectedWeek = matched;
    }
  }

  if (!state.selectedWeek) {
    const todayKey = toIsoDate(new Date());
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
    setSelectedWeek(
      (isQuarterlyMode()
        ? latestClosedCompleted || latestClosedAnalyzable || latestClosedUpdate
        : null) ||
        latestCompleted ||
        latestAnalyzable ||
        latestUpdate ||
        null
    );
  } else {
    setSelectedWeek(state.selectedWeek);
  }

  renderSnapshotWindowList();
  state.calendarSelectedWeek = state.selectedWeek;
  syncCalendarModal();
  renderCalendar();
  updateActionButtons();
}

async function loadUpdateWindows(forceOverview = false) {
  const [payload] = await Promise.all([
    requestJson(`${API_BASE}/analysis-windows?platform=${encodeURIComponent(state.updatePlatform)}&weeks=24&window_mode=weekly`),
    refreshUpdateOverviewMeta(forceOverview),
  ]);
  state.updateWindows = payload.items || [];
  renderUpdatePlatformState();
  if (state.calendarMode === "update") {
    const previousSelection = state.calendarSelectedWeek;
    state.calendarSelectedWeek =
      getUpdateWindowForWeek(previousSelection) ||
      getUpdateWindowForWeek(state.lastUpdateSelection) ||
      sourceWindowsFallback(state.updateWindows) ||
      null;
    state.lastUpdateSelection = state.calendarSelectedWeek
      ? { week_start: state.calendarSelectedWeek.week_start, week_end: state.calendarSelectedWeek.week_end }
      : state.lastUpdateSelection;
    syncCalendarModal();
    renderCalendar();
  }
  updateActionButtons();
}

async function refreshUpdateOverviewMeta(force = false) {
  if (!force && state.updateOverviewCache[state.updatePlatform]) {
    return state.updateOverviewCache[state.updatePlatform];
  }
  const overview = await requestJson(`${API_BASE}/overview?platform=${encodeURIComponent(state.updatePlatform)}&auto_sync=false`);
  const fetchedAt = new Date().toISOString();
  overview.metadata_fetched_at = fetchedAt;
  overview.update_database_meta = {
    ...(overview.update_database_meta || {}),
    metadata_fetched_at: fetchedAt,
  };
  state.updateOverviewCache[state.updatePlatform] = overview;
  return overview;
}

function sourceWindowsFallback(items) {
  return (
    items.find((item) => getUpdateWindowStatus(item) === "ready_to_import") ||
    items.find((item) => getUpdateWindowStatus(item) !== "future") ||
    null
  );
}

async function refreshOverviewMeta(force = false) {
  if (!force && state.overviewCache[state.platform]) {
    const cached = state.overviewCache[state.platform];
    elements.heatDbPathLabel.textContent = cached.db_path || "Unknown analytics database";
    return cached;
  }
  const overview = await requestJson(`${API_BASE}/overview?platform=${encodeURIComponent(state.platform)}&auto_sync=false`);
  state.overviewCache[state.platform] = overview;
  elements.heatDbPathLabel.textContent = overview.db_path || "Unknown analytics database";
  return overview;
}

async function fetchLeaderboardData() {
  setLeaderboardLoading(true);
  setPanelBusy(true, "Data is loading...", "Refreshing the leaderboard for the current filters.");
  try {
    elements.heatDbPathLabel.textContent = state.overviewCache[state.platform]?.db_path || "Loading analytics database...";
    refreshOverviewMeta().catch((error) => {
      elements.heatDbPathLabel.textContent = `Overview unavailable: ${error.message}`;
    });

    if (!state.selectedWeek) {
      renderOverview([]);
      renderLeaderboard([]);
      return;
    }

    const query = new URLSearchParams({
      platform: state.platform,
      limit: "120",
    });
    if (isQuarterlyMode()) {
      query.set("quarter_key", state.selectedWeek.quarter_key || "");
    } else if (isMonthlyMode()) {
      query.set("month_key", state.selectedWeek.month_key);
    } else {
      query.set("week_start", state.selectedWeek.week_start);
      query.set("week_end", state.selectedWeek.week_end);
    }
    const endpoint = state.boardType === "topic" ? `${API_BASE}/topic-clusters` : `${API_BASE}/event-clusters`;
    const clusterPayload = await requestJson(`${endpoint}?${query.toString()}`);
    const items = sortItems(clusterPayload.items || []);
    renderOverview(items);
    renderLeaderboard(items);
    if (clusterPayload.total > 0) {
      state.selectedEvent = items[0]?.cluster_key || "";
    } else {
      state.selectedEvent = "";
    }
    syncTrendLink();
  } finally {
    setLeaderboardLoading(false);
    setPanelBusy(false);
  }
}

async function pollProjectJob(jobId) {
  state.pollingJobId = jobId;
  const tick = async () => {
    const job = await requestJson(`${API_BASE}/jobs/${jobId}`);
    state.latestJob = job;
    if (job.status === "queued" || job.status === "running" || job.status === "cancelling") {
      const isCancelling = job.status === "cancelling";
      setStatus(
        isCancelling ? "Stopping crawl" : "Updating Database",
        isCancelling
          ? `Stopping the active ${getJobPlatformLabel(job)} crawl for ${formatJobWeekLabel(job)}.`
          : `Background job is crawling ${getJobPlatformLabel(job)} for ${formatJobWeekLabel(job)}. New results will be staged before import.`
      );
      setPanelBusy(false);
      updateActionButtons();
      renderCrawlMonitor();
      window.setTimeout(tick, 2000);
      return;
    }
    state.pollingJobId = "";
    if (job.status === "failed") {
      await loadUpdateWindows();
      setPanelBusy(false);
      setStatus("Update failed", job.error || "The update job failed.");
      updateActionButtons();
      renderCrawlMonitor();
      return;
    }
    if (job.status === "cancelled") {
      await loadUpdateWindows();
      setPanelBusy(false);
      setStatus("Crawl cancelled", "The crawl stopped before import confirmation. Existing database data was not changed.");
      updateActionButtons();
      renderCrawlMonitor();
      return;
    }
    if (job.status === "awaiting_confirmation") {
      await loadUpdateWindows(true);
      setPanelBusy(false);
      updateActionButtons();
      renderCrawlMonitor();
      return;
    }
    await loadUpdateWindows(true);
    await loadWindows();
    await fetchLeaderboardData();
    setStatus("Update completed", "This week is now in the database. If the status is To Be Analyzed, click Run Analysis next.");
    setPanelBusy(false);
    renderCrawlMonitor();
  };
  await tick();
}

async function startUpdateForSelectedWeek() {
  if (hasActiveUpdateJob()) {
    setStatus("Crawl already running", "A weekly crawl is already in progress. Stop it first or wait until it finishes staging.");
    return;
  }
  const week =
    state.calendarSelectedWeek ||
    getSelectedWeeklyUpdateWindow() ||
    sourceWindowsFallback(state.updateWindows) ||
    null;
  if (!week || getUpdateWindowStatus(week) === "future") {
    setStatus("Nothing to update", `No finished weekly window is currently available to crawl for ${getUpdatePlatformLabel()}.`);
    return;
  }
  const query = new URLSearchParams({
    platform: week.platform || state.updatePlatform,
    week_start: week.week_start,
    week_end: week.week_end,
  });
  state.lastUpdateSelection = { week_start: week.week_start, week_end: week.week_end };
  const job = await postJson(`${API_BASE}/update-week?${query.toString()}`);
  state.latestJob = job;
  state.crawlMonitorMinimized = false;
  updateActionButtons();
  syncCalendarSelectionSummary();
  renderCalendar();
  closeCalendar();
  await pollProjectJob(job.job_id);
}

async function cancelLatestUpdateJob() {
  const latestJob = getAnyActiveUpdateJob();
  if (!latestJob || !latestJob.job_id || !new Set(["queued", "running", "cancelling"]).has(latestJob.status)) {
    updateActionButtons();
    return;
  }
  setPanelBusy(true, "Stopping crawl...", `Requesting cancellation for ${formatJobWeekLabel(latestJob)}.`);
  try {
    await postJson(`${API_BASE}/jobs/${encodeURIComponent(latestJob.job_id)}/cancel`);
    state.latestJob = { ...latestJob, status: "cancelling" };
    updateActionButtons();
    renderCrawlMonitor();
    await pollProjectJob(latestJob.job_id);
  } catch (error) {
    setPanelBusy(false);
    setStatus("Cancel failed", error.message);
  }
}

async function confirmImportForSelectedWeek() {
  const week = getImportCandidateWindow();
  if (!week || getUpdateWindowStatus(week) !== "ready_to_import") {
    updateActionButtons();
    return;
  }
  const query = new URLSearchParams({
    platform: week.platform || state.updatePlatform,
    week_start: week.week_start,
    week_end: week.week_end,
  });
  setPanelBusy(true, "Importing staged files...", `Writing staged ${PLATFORM_LABELS[week.platform] || getUpdatePlatformLabel()} files for ${formatWeekLabel(week.week_start, week.week_end)} into the analytics database.`);
  try {
    const result = await postJson(`${API_BASE}/confirm-week-import?${query.toString()}`);
    if (
      state.latestJob?.payload?.platform === state.updatePlatform &&
      state.latestJob?.payload?.week_start === week.week_start &&
      state.latestJob?.payload?.week_end === week.week_end
    ) {
      state.latestJob = { ...state.latestJob, status: "completed", summary: result };
    }
    await Promise.all([loadUpdateWindows(true), loadWindows(), refreshOverviewMeta(true)]);
    await fetchLeaderboardData();
    setStatus("Import completed", `${formatWeekLabel(week.week_start, week.week_end)} has been imported. If needed, run analysis next.`);
  } catch (error) {
    setStatus("Import failed", error.message);
  } finally {
    setPanelBusy(false);
    updateActionButtons();
    renderCrawlMonitor();
  }
}

async function confirmCalendarSelection() {
  if (!state.calendarSelectedWeek) {
    return;
  }

  if (state.calendarMode === "update") {
    await startUpdateForSelectedWeek();
    return;
  }

  setSelectedWeek(state.calendarSelectedWeek);
  renderSnapshotWindowList();
  closeCalendar();
  await fetchLeaderboardData();
}

async function runAnalysisForSelectedWeek() {
  if (!state.selectedWeek || !new Set(["to_be_analyzed", "completed"]).has(state.selectedWeek.status)) {
    updateActionButtons();
    return;
  }
  const query = new URLSearchParams({
    platform: state.platform,
    replace: "true",
  });
  if (isMonthlyMode()) {
    query.set("month_key", state.selectedWeek.month_key);
  } else {
    query.set("week_start", state.selectedWeek.week_start);
    query.set("week_end", state.selectedWeek.week_end);
  }

  setStatus(
    "Running Analysis",
    `Building ${getBoardTypeLabel().toLowerCase()} clusters for ${PLATFORM_LABELS[state.platform]} ${formatSelectedWindowLabel(
      state.selectedWeek
    )}.`
  );
  setPanelBusy(
    true,
    "Analysis in progress...",
    `Checking imported ${isMonthlyMode() ? "monthly" : "weekly"} data for ${PLATFORM_LABELS[state.platform]} and building cluster results.`
  );
  try {
    const result = await postJson(`${API_BASE}/extract-events?${query.toString()}`);
    await loadWindows();
    await loadUpdateWindows();
    await fetchLeaderboardData();
    setStatus(
      "Analysis completed",
      `${formatNumber(result.event_cluster_rows || 0)} event clusters and ${formatNumber(
        result.topic_cluster_rows || 0
      )} topic clusters are now ready for this ${isMonthlyMode() ? "month" : "week"}.`
    );
  } catch (error) {
    setStatus("Analysis failed", error.message);
  } finally {
    setPanelBusy(false);
  }
}

function bindEvents() {
  elements.eventTabButton.addEventListener("click", async () => {
    showInteractionBusy("Switching mode...", "Loading the event leaderboard for the current filters.");
    state.boardType = "event";
    elements.eventTabButton.classList.add("active");
    elements.eventTabButton.classList.remove("secondary");
    elements.topicTabButton.classList.remove("active");
    elements.topicTabButton.classList.add("secondary");
    syncLeaderboardCopy();
    await fetchLeaderboardData();
  });

  elements.topicTabButton.addEventListener("click", async () => {
    showInteractionBusy("Switching mode...", "Loading the topic leaderboard for the current filters.");
    state.boardType = "topic";
    elements.topicTabButton.classList.add("active");
    elements.topicTabButton.classList.remove("secondary");
    elements.eventTabButton.classList.remove("active");
    elements.eventTabButton.classList.add("secondary");
    syncLeaderboardCopy();
    await fetchLeaderboardData();
  });

  elements.platformSelect.addEventListener("change", async (event) => {
    const nextPlatform = event.target.value || "wb";
    showInteractionBusy("Switching platform...", `Refreshing windows and leaderboard for ${PLATFORM_LABELS[nextPlatform]}.`);
    state.platform = event.target.value || "wb";
    syncPlatformControls();
    state.selectedWeek = null;
    await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
    await fetchLeaderboardData();
  });

  const handleUpdatePlatformChange = async (nextPlatform) => {
    if (nextPlatform === state.updatePlatform) {
      return;
    }
    state.updatePlatform = nextPlatform;
    window.sessionStorage?.setItem("fullweb_update_platform", state.updatePlatform);
    syncUpdatePlatformControls();
    state.calendarSelectedWeek = null;
    state.lastUpdateSelection = null;
    if (state.calendarMode === "update") {
      await loadUpdateWindows(true);
    }
  };

  elements.updatePlatformSelect?.addEventListener("change", async (event) => {
    const nextPlatform = event.target.value || "wb";
    await handleUpdatePlatformChange(nextPlatform);
  });

  elements.updatePlatformChoiceInputs.forEach((input) => {
    input.addEventListener("change", async (event) => {
      if (!event.target.checked) {
        return;
      }
      await handleUpdatePlatformChange(event.target.value || "wb");
    });
  });

  elements.platformChoiceInputs.forEach((input) => {
    input.addEventListener("change", async (event) => {
      if (!event.target.checked) {
        return;
      }
      const nextPlatform = event.target.value || "wb";
      showInteractionBusy("Switching platform...", `Refreshing windows and leaderboard for ${PLATFORM_LABELS[nextPlatform]}.`);
      state.platform = event.target.value || "wb";
      syncPlatformControls();
      state.selectedWeek = null;
      await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
      await fetchLeaderboardData();
    });
  });

  elements.windowModeSelect.addEventListener("change", async (event) => {
    const nextMode = event.target.value || "monthly";
    showInteractionBusy("Switching date range...", `Updating ${nextMode} filters and refreshing the leaderboard.`);
    state.windowMode = event.target.value || "monthly";
    syncWindowModeControls();
    state.selectedWeek = null;
    syncLeaderboardCopy();
    if (isMonthlyMode() || isQuarterlyMode()) {
      closeCalendar();
    }
    await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
    await fetchLeaderboardData();
  });

  elements.windowModeButtons.forEach((button) => {
    button.addEventListener("click", async () => {
      const nextMode = button.dataset.windowMode || "monthly";
      if (nextMode === state.windowMode) {
        return;
      }
      showInteractionBusy("Switching date range...", `Updating ${nextMode} filters and refreshing the leaderboard.`);
      state.windowMode = nextMode;
      syncWindowModeControls();
      state.selectedWeek = null;
      syncLeaderboardCopy();
      if (isMonthlyMode() || isQuarterlyMode()) {
        closeCalendar();
      }
      await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
      await fetchLeaderboardData();
    });
  });

  elements.sortMetricSelect.addEventListener("change", async (event) => {
    const nextMetric = event.target.value || "heat_score";
    showInteractionBusy("Reordering leaderboard...", `Sorting current results by ${getSortMetricLabel(nextMetric)}.`);
    state.sortMetric = event.target.value;
    await fetchLeaderboardData();
  });

  elements.updateDatabaseButton.addEventListener("click", async () => {
    if (hasActiveUpdateJob()) {
      setStatus("Crawl already running", "Stop the active crawl before starting another weekly crawl.");
      return;
    }
    setCalendarMode("update");
    openCalendar();
    try {
      await loadUpdateWindows(true);
    } catch (error) {
      setStatus("Update workspace unavailable", error.message);
    }
  });
  elements.crawlMonitorToggleButton?.addEventListener("click", () => {
    state.crawlMonitorMinimized = !state.crawlMonitorMinimized;
    renderCrawlMonitor();
  });
  elements.crawlMonitorChip?.addEventListener("click", () => {
    state.crawlMonitorMinimized = false;
    renderCrawlMonitor();
  });
  elements.openSnapshotCalendarButton.addEventListener("click", () => {
    setCalendarMode("filter");
    openCalendar();
  });
  elements.closeCalendarButton.addEventListener("click", closeCalendar);
  elements.updateCalendarBackdrop.addEventListener("click", closeCalendar);
  elements.confirmUpdateButton.addEventListener("click", confirmCalendarSelection);
  elements.cancelUpdateJobButton.addEventListener("click", cancelLatestUpdateJob);
  elements.confirmImportButton.addEventListener("click", confirmImportForSelectedWeek);
  elements.crawlMonitorStopButton?.addEventListener("click", cancelLatestUpdateJob);
  elements.crawlMonitorImportButton?.addEventListener("click", confirmImportForSelectedWeek);
  elements.sidebarRunAnalysisButton.addEventListener("click", runAnalysisForSelectedWeek);
  elements.openHeatFormulaButton?.addEventListener("click", openHeatFormulaModal);
  elements.closeHeatFormulaButton?.addEventListener("click", closeHeatFormulaModal);
  elements.heatFormulaBackdrop?.addEventListener("click", closeHeatFormulaModal);
  elements.clusterMarkNoiseButton?.addEventListener("click", () => {
    hideClusterContextMenu();
    openClusterNoiseModal();
  });
  elements.clusterMergeButton?.addEventListener("click", () => {
    hideClusterContextMenu();
    openClusterMergeModal();
  });
  elements.closeClusterMergeButton?.addEventListener("click", closeClusterMergeModal);
  elements.cancelClusterMergeButton?.addEventListener("click", closeClusterMergeModal);
  elements.clusterMergeBackdrop?.addEventListener("click", closeClusterMergeModal);
  elements.confirmClusterMergeButton?.addEventListener("click", async () => {
    const target = elements.clusterMergeTargetSelect?.value || "";
    if (!target) {
      return;
    }
    await submitClusterFeedback("merge", target);
  });
  elements.closeClusterNoiseButton?.addEventListener("click", closeClusterNoiseModal);
  elements.cancelClusterNoiseButton?.addEventListener("click", closeClusterNoiseModal);
  elements.clusterNoiseBackdrop?.addEventListener("click", closeClusterNoiseModal);
  elements.confirmClusterNoiseButton?.addEventListener("click", async () => {
    await submitClusterFeedback("noise");
  });
  document.addEventListener("click", () => hideClusterContextMenu());
  window.addEventListener("scroll", () => hideClusterContextMenu(), true);
}

async function bootstrap() {
  requireSession();
  const url = new URL(window.location.href);
  state.platform = url.searchParams.get("platform") || "wb";
  state.updatePlatform = window.sessionStorage?.getItem("fullweb_update_platform") || state.platform;
  state.windowMode = url.searchParams.get("window_mode") || "monthly";
  if (state.windowMode === "quarterly" && url.searchParams.get("quarter_key")) {
    state.selectedWeek = { quarter_key: url.searchParams.get("quarter_key") };
  } else if (state.windowMode === "monthly" && url.searchParams.get("month_key")) {
    state.selectedWeek = { month_key: url.searchParams.get("month_key") };
  } else if (url.searchParams.get("week_start") && url.searchParams.get("week_end")) {
    state.selectedWeek = {
      week_start: url.searchParams.get("week_start"),
      week_end: url.searchParams.get("week_end"),
    };
  }
  syncPlatformControls();
  syncUpdatePlatformControls();
  syncWindowModeControls();
  syncLeaderboardCopy();
  bindEvents();
  elements.heatDbPathLabel.textContent = "Loading analytics database...";
  await Promise.all([loadUpdateWindows(), loadWindows(), refreshOverviewMeta()]);
  const latestJobs = await requestJson(`${API_BASE}/jobs?limit=5`);
  state.latestJob =
    (latestJobs.items || []).find(
      (job) =>
        job.job_type === "update_week" &&
        new Set(["queued", "running", "cancelling"]).has(job.status)
    ) ||
    (latestJobs.items || []).find(
      (job) =>
        job.job_type === "update_week" &&
        job.payload?.platform === state.updatePlatform &&
        new Set(["queued", "running", "cancelling", "awaiting_confirmation", "cancelled"]).has(job.status)
    ) || null;
  if (isMonthlyMode() ? url.searchParams.get("month_key") : url.searchParams.get("week_start") && url.searchParams.get("week_end")) {
    const matched = state.windows.find((item) =>
      isMonthlyMode()
        ? item.month_key === url.searchParams.get("month_key")
        : item.week_start === url.searchParams.get("week_start") && item.week_end === url.searchParams.get("week_end")
    );
    if (matched) {
      setSelectedWeek(matched);
      renderSnapshotWindowList();
    }
  }
  if (state.latestJob && new Set(["queued", "running", "cancelling"]).has(state.latestJob.status)) {
    await pollProjectJob(state.latestJob.job_id);
    return;
  }
  updateActionButtons();
  await fetchLeaderboardData();
}

bootstrap().catch((error) => {
  elements.heatDbPathLabel.textContent = `Load failed: ${error.message}`;
  renderLeaderboard([]);
  setStatus("Load failed", error.message);
});
