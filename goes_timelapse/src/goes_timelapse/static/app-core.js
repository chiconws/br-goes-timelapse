(function (window) {
  "use strict";

  var app = window.GoesTimelapseApp || (window.GoesTimelapseApp = {});
  var SEARCH_DEBOUNCE_MS = 180;
  var STATUS_REFRESH_MS = 15000;
  var ACTIVE_DOWNLOAD_REFRESH_MS = 1500;
  var THEME_STORAGE_KEY = "goes_timelapse_theme";
  var basePath = "/";

  app.constants = {
    SEARCH_DEBOUNCE_MS: SEARCH_DEBOUNCE_MS,
    STATUS_REFRESH_MS: STATUS_REFRESH_MS,
    ACTIVE_DOWNLOAD_REFRESH_MS: ACTIVE_DOWNLOAD_REFRESH_MS,
    THEME_STORAGE_KEY: THEME_STORAGE_KEY,
  };

  app.state = {
    query: "",
    searchResults: [],
    status: null,
    downloads: [],
    tracked: [],
    trackedSignature: "",
    downloadsSignature: "",
    statusSignature: "",
    queuedHelperSignature: "",
    markerDrafts: {},
    markerErrors: {},
    pendingPins: {},
    searchRequestId: 0,
    searchTimer: null,
    refreshTimer: null,
  };

  app.elements = {};

  app.getBasePath = function () {
    return basePath;
  };

  app.setBasePath = function (value) {
    basePath = normalizeBasePath(value);
  };

  app.normalizeBasePath = normalizeBasePath;
  app.buildUrl = buildUrl;
  app.fetchJson = fetchJson;
  app.buildPreviewUrl = buildPreviewUrl;
  app.parseMarkerCoordinates = parseMarkerCoordinates;
  app.formatMarkerCoordinates = formatMarkerCoordinates;
  app.findAreaById = findAreaById;
  app.findActionNode = findActionNode;
  app.trackedIdMap = trackedIdMap;
  app.isTracked = isTracked;
  app.formatNumber = formatNumber;
  app.formatBytes = formatBytes;
  app.formatStatusDateTime = formatStatusDateTime;
  app.describeRawFile = describeRawFile;
  app.parseSlotCaptureDate = parseSlotCaptureDate;
  app.parseGoesCaptureDate = parseGoesCaptureDate;
  app.formatCaptureDate = formatCaptureDate;
  app.hasActiveDownloads = hasActiveDownloads;
  app.safeValue = safeValue;
  app.queuedHelperText = queuedHelperText;
  app.buildQueuedHelperSignature = buildQueuedHelperSignature;
  app.statusLabel = statusLabel;
  app.escapeHtml = escapeHtml;

  function normalizeBasePath(value) {
    var normalized = String(value || "/");
    if (!normalized) {
      return "/";
    }
    if (normalized.charAt(0) !== "/") {
      normalized = "/" + normalized;
    }
    if (normalized.charAt(normalized.length - 1) !== "/") {
      normalized += "/";
    }
    return normalized;
  }

  function buildUrl(path) {
    return basePath + String(path || "").replace(/^\/+/, "");
  }

  function fetchJson(path, options) {
    return window.fetch(buildUrl(path), options || {}).then(function (response) {
      if (response.ok) {
        return response.json();
      }

      return response
        .json()
        .catch(function () {
          return {};
        })
        .then(function (payload) {
          var detail = payload && payload.detail ? payload.detail : "Falha na requisição";
          throw new Error(detail);
        });
    });
  }

  function buildPreviewUrl(area) {
    var previewPath = area && area.preview_url ? area.preview_url : "";
    var url = "";
    var cacheBuster = "";

    if (!previewPath) {
      return "";
    }

    url = buildUrl(previewPath);
    cacheBuster = area.media_version || area.latest_source_timestamp || "";
    if (cacheBuster) {
      url += (url.indexOf("?") === -1 ? "?" : "&") + "v=" + encodeURIComponent(cacheBuster);
    }
    return url;
  }

  function parseMarkerCoordinates(value) {
    var raw = String(value || "").trim();
    var parts;
    var lat;
    var lon;

    if (!raw) {
      return null;
    }

    parts = raw.split(",");
    if (parts.length !== 2) {
      return null;
    }

    lat = Number(parts[0].trim());
    lon = Number(parts[1].trim());
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return null;
    }

    return { lat: lat, lon: lon };
  }

  function formatMarkerCoordinates(lat, lon) {
    return String(lat) + ", " + String(lon);
  }

  function findAreaById(areaId) {
    var index;
    for (index = 0; index < app.state.searchResults.length; index += 1) {
      if (app.state.searchResults[index].area_id === areaId) {
        return app.state.searchResults[index];
      }
    }
    return null;
  }

  function findActionNode(node, attributeName, boundary) {
    var current = node;
    while (current && current !== boundary) {
      if (current.getAttribute && current.getAttribute(attributeName)) {
        return current;
      }
      current = current.parentNode;
    }
    return null;
  }

  function trackedIdMap() {
    var ids = {};
    var index;

    for (index = 0; index < app.state.tracked.length; index += 1) {
      ids[app.state.tracked[index].area_id] = true;
    }

    return ids;
  }

  function isTracked(areaId) {
    var index;
    for (index = 0; index < app.state.tracked.length; index += 1) {
      if (app.state.tracked[index].area_id === areaId) {
        return true;
      }
    }
    return false;
  }

  function formatNumber(value) {
    return String(value).replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  }

  function formatBytes(value) {
    var units = ["B", "KB", "MB", "GB"];
    var size = Number(value || 0);
    var unitIndex = 0;

    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex += 1;
    }

    return size.toFixed(unitIndex === 0 ? 0 : 1) + " " + units[unitIndex];
  }

  function formatStatusDateTime(value) {
    var date;
    var day;
    var month;
    var year;
    var hours;
    var minutes;
    var seconds;

    if (!value) {
      return "Nunca";
    }

    date = new Date(value);
    if (isNaN(date.getTime())) {
      return safeValue(value);
    }

    day = String(date.getDate()).padStart(2, "0");
    month = String(date.getMonth() + 1).padStart(2, "0");
    year = String(date.getFullYear());
    hours = String(date.getHours()).padStart(2, "0");
    minutes = String(date.getMinutes()).padStart(2, "0");
    seconds = String(date.getSeconds()).padStart(2, "0");

    return hours + ":" + minutes + ":" + seconds + " " + day + "/" + month + "/" + year;
  }

  function describeRawFile(filename) {
    var captureDate = parseGoesCaptureDate(filename) || parseSlotCaptureDate(filename);
    var bandMatch = String(filename || "").match(/M6(C\d{2})/);
    var bandLabel = bandMatch ? bandMatch[1] : "";
    var shortName = String(filename || "");

    if (captureDate) {
      return {
        primary: formatCaptureDate(captureDate),
        secondary: (bandLabel ? bandLabel + " • " : "") + shortName,
      };
    }

    return {
      primary: shortName || "Nenhum",
      secondary: bandLabel ? bandLabel : "",
    };
  }

  function parseSlotCaptureDate(filename) {
    var match = String(filename || "").match(/^(\d{4})(\d{3})(\d{2})(\d{2})_/);
    var start;

    if (!match) {
      return null;
    }

    start = new Date(Date.UTC(Number(match[1]), 0, 1, Number(match[3]), Number(match[4]), 0));
    start.setUTCDate(start.getUTCDate() + Number(match[2]) - 1);
    return start;
  }

  function parseGoesCaptureDate(filename) {
    var match = String(filename || "").match(/_s(\d{4})(\d{3})(\d{2})(\d{2})/);
    var start;

    if (!match) {
      return null;
    }

    start = new Date(Date.UTC(Number(match[1]), 0, 1, Number(match[3]), Number(match[4]), 0));
    start.setUTCDate(start.getUTCDate() + Number(match[2]) - 1);
    return start;
  }

  function formatCaptureDate(date) {
    var day = String(date.getDate()).padStart(2, "0");
    var month = String(date.getMonth() + 1).padStart(2, "0");
    var year = String(date.getFullYear());
    var hours = String(date.getHours()).padStart(2, "0");
    var minutes = String(date.getMinutes()).padStart(2, "0");

    return day + "/" + month + "/" + year + " " + hours + ":" + minutes;
  }

  function hasActiveDownloads() {
    var index;
    for (index = 0; index < app.state.downloads.length; index += 1) {
      if (
        app.state.downloads[index].phase === "downloading" ||
        (Array.isArray(app.state.downloads[index].active_downloads) &&
          app.state.downloads[index].active_downloads.length > 0)
      ) {
        return true;
      }
    }
    return false;
  }

  function safeValue(value) {
    if (value === null || value === undefined || value === "") {
      return "-";
    }
    return String(value);
  }

  function queuedHelperText() {
    var status = app.state.status || {};
    var rawCount = Number(status.raw_frame_count || 0);
    var summary = String(status.raw_download_summary || "");
    if (rawCount <= 0) {
      return summary || "Aguardando quadros brutos do GOES vindos da NOAA.";
    }
    return "Na fila para a próxima renderização.";
  }

  function buildQueuedHelperSignature(status) {
    var data = status || {};
    return JSON.stringify({
      raw_frame_count: data.raw_frame_count || 0,
      raw_download_summary: data.raw_download_summary || "",
    });
  }

  function statusLabel(status) {
    var normalized = String(status || "").toLowerCase();
    if (normalized === "disabled") {
      return "desativado";
    }
    if (normalized === "paused") {
      return "pausado";
    }
    if (normalized === "idle") {
      return "ocioso";
    }
    if (normalized === "downloading") {
      return "baixando";
    }
    if (normalized === "partial") {
      return "parcial";
    }
    if (normalized === "ready") {
      return "pronto";
    }
    if (normalized === "processing") {
      return "processando";
    }
    if (normalized === "error") {
      return "erro";
    }
    return "na fila";
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }
})(window);
