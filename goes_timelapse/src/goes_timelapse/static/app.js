(function () {
  "use strict";

  var SEARCH_DEBOUNCE_MS = 180;
  var STATUS_REFRESH_MS = 15000;
  var ACTIVE_DOWNLOAD_REFRESH_MS = 1500;
  var THEME_STORAGE_KEY = "goes_timelapse_theme";
  var basePath = "/";

  var state = {
    query: "",
    searchResults: [],
    status: null,
    downloads: [],
    tracked: [],
    trackedSignature: "",
    downloadsSignature: "",
    statusSignature: "",
    queuedHelperSignature: "",
    pendingPins: {},
    searchRequestId: 0,
    searchTimer: null,
    refreshTimer: null,
  };

  var elements = {};

  document.addEventListener("DOMContentLoaded", function () {
    basePath = normalizeBasePath(
      document.documentElement.getAttribute("data-base-path") || "/"
    );

    elements.statusGrid = document.getElementById("status-grid");
    elements.searchInput = document.getElementById("area-search");
    elements.themeToggle = document.getElementById("theme-toggle");
    elements.searchFeedback = document.getElementById("search-feedback");
    elements.searchResults = document.getElementById("search-results");
    elements.trackedList = document.getElementById("tracked-list");
    elements.downloadsList = document.getElementById("downloads-list");
    if (
      !elements.statusGrid ||
      !elements.searchInput ||
      !elements.themeToggle ||
      !elements.searchResults ||
      !elements.trackedList ||
      !elements.downloadsList
    ) {
      return;
    }

    applyInitialTheme();

    elements.searchInput.addEventListener("input", onSearchInput);
    elements.themeToggle.addEventListener("click", onThemeToggleClick);
    elements.searchResults.addEventListener("click", onSearchResultClick);
    elements.trackedList.addEventListener("click", onTrackedActionClick);

    refreshDashboard();
  });

  function refreshDashboard() {
    return Promise.all([loadStatus(), loadTracked(), loadDownloads()]).finally(scheduleRefresh);
  }

  function scheduleRefresh() {
    if (state.refreshTimer) {
      window.clearTimeout(state.refreshTimer);
    }
    state.refreshTimer = window.setTimeout(
      refreshDashboard,
      hasActiveDownloads() ? ACTIVE_DOWNLOAD_REFRESH_MS : STATUS_REFRESH_MS
    );
  }

  function onSearchInput(event) {
    var value = event.target.value || "";
    state.query = value;

    if (state.searchTimer) {
      window.clearTimeout(state.searchTimer);
    }

    if (!value.trim()) {
      state.searchResults = [];
      renderSearchResults();
      clearFeedback();
      return;
    }

    renderSearchLoading();
    state.searchTimer = window.setTimeout(function () {
      loadSearch(value);
    }, SEARCH_DEBOUNCE_MS);
  }

  function onThemeToggleClick() {
    var currentTheme = getCurrentTheme();
    var nextTheme = currentTheme === "dark" ? "light" : "dark";
    setTheme(nextTheme);
  }

  function onSearchResultClick(event) {
    var button = findActionNode(event.target, "data-area-id", elements.searchResults);
    if (!button || button.disabled) {
      return;
    }

    var areaId = button.getAttribute("data-area-id");
    var area = findAreaById(areaId);
    if (!area) {
      return;
    }

    addTracked(area);
  }

  function onTrackedActionClick(event) {
    var button = findActionNode(event.target, "data-action", elements.trackedList);
    if (!button || button.disabled) {
      return;
    }

    var action = button.getAttribute("data-action");
    var areaId = button.getAttribute("data-area-id");
    if (!areaId) {
      return;
    }

    if (action === "remove") {
      removeTracked(areaId, button.getAttribute("data-display-name") || areaId, button);
      return;
    }

    if (action === "save-marker") {
      saveMarker(areaId, button);
      return;
    }

    if (action === "remove-marker") {
      removeMarker(areaId, button);
    }
  }

  function loadStatus() {
    fetchJson("api/status")
      .then(function (payload) {
        var nextStatusSignature = JSON.stringify(payload || {});
        var nextQueuedHelperSignature = buildQueuedHelperSignature(payload);
        var shouldRenderStatus = nextStatusSignature !== state.statusSignature;
        var shouldRenderTracked = nextQueuedHelperSignature !== state.queuedHelperSignature;

        state.status = payload;
        state.statusSignature = nextStatusSignature;
        state.queuedHelperSignature = nextQueuedHelperSignature;

        if (shouldRenderStatus) {
          renderStatus(payload);
        }

        if (shouldRenderTracked) {
          renderTracked();
        }
      })
      .catch(function () {
        state.status = {
          tracked_count: "-",
          queue_length: "-",
          raw_frame_count: "-",
          raw_download_summary: "Falha ao consultar o status",
          raw_frame_latest: null,
          last_poll_finished_at: "Indisponível",
          last_poll_new_downloads: "-",
          last_poll_error: "Falha ao consultar o status",
        };
        state.statusSignature = JSON.stringify(state.status);
        state.queuedHelperSignature = buildQueuedHelperSignature(state.status);
        renderStatus(state.status);
        renderTracked();
      });
  }

  function loadTracked() {
    return fetchJson("api/tracked")
      .then(function (payload) {
        var normalized = Array.isArray(payload) ? payload : [];
        var nextSignature = JSON.stringify(normalized);
        var shouldRenderTracked = nextSignature !== state.trackedSignature;

        state.tracked = normalized;
        state.trackedSignature = nextSignature;

        if (shouldRenderTracked) {
          renderTracked();
          renderSearchResults();
        }
      })
      .catch(function () {
        elements.trackedList.innerHTML =
          '<p class="empty-state">Os municípios acompanhados estão indisponíveis no momento.</p>';
      });
  }

  function loadDownloads() {
    return fetchJson("api/downloads")
      .then(function (payload) {
        var normalized = payload && Array.isArray(payload.sources) ? payload.sources : [];
        var nextSignature = JSON.stringify(normalized);

        state.downloads = normalized;

        if (nextSignature !== state.downloadsSignature) {
          state.downloadsSignature = nextSignature;
          renderDownloads();
        }
      })
      .catch(function () {
        elements.downloadsList.innerHTML =
          '<p class="empty-state">Os detalhes de download estão indisponíveis no momento.</p>';
      });
  }

  function loadSearch(query) {
    var cleaned = String(query || "").trim();
    var requestId = state.searchRequestId + 1;
    state.searchRequestId = requestId;

    return fetchJson("api/areas?q=" + encodeURIComponent(cleaned))
      .then(function (payload) {
        if (requestId !== state.searchRequestId) {
          return;
        }

        if (cleaned !== String(state.query || "").trim()) {
          return;
        }

        state.searchResults = Array.isArray(payload) ? payload : [];
        renderSearchResults();
      })
      .catch(function (error) {
        if (requestId !== state.searchRequestId) {
          return;
        }

        elements.searchResults.innerHTML =
          '<p class="empty-state">' + escapeHtml(error.message || "Busca indisponível.") + "</p>";
      });
  }

  function addTracked(area) {
    if (!area || state.pendingPins[area.area_id] || isTracked(area.area_id)) {
      return;
    }

    state.pendingPins[area.area_id] = {
      area_id: area.area_id,
      display_name: area.display_name,
      type_label: area.type_label,
      code_label: area.code_label,
    };

    setFeedback("pending", "Adicionando " + area.display_name + "...");
    renderSearchResults();

    fetchJson("api/tracked/" + encodeURIComponent(area.area_id), {
      method: "PUT",
    })
      .then(function () {
        delete state.pendingPins[area.area_id];
        setFeedback("success", area.display_name + " entrou na fila de renderização.");
        return Promise.all([loadTracked(), loadStatus()]);
      })
      .catch(function (error) {
        delete state.pendingPins[area.area_id];
        setFeedback("error", error.message || "Não foi possível adicionar o município.");
        renderSearchResults();
      });
  }

  function removeTracked(areaId, displayName, button) {
    if (button) {
      button.disabled = true;
    }

    setFeedback("pending", "Removendo " + displayName + "...");
    fetchJson("api/tracked/" + encodeURIComponent(areaId), {
      method: "DELETE",
    })
      .then(function () {
        setFeedback("success", displayName + " removida.");
        return Promise.all([loadTracked(), loadStatus()]);
      })
      .catch(function (error) {
        if (button) {
          button.disabled = false;
        }

        setFeedback("error", error.message || "Não foi possível remover o município.");
      });
  }

  function saveMarker(areaId, button) {
    var editor = button.closest(".marker-editor");
    var latInput = editor ? editor.querySelector('[data-marker-field="lat"]') : null;
    var lonInput = editor ? editor.querySelector('[data-marker-field="lon"]') : null;
    var lat = latInput ? Number(latInput.value) : NaN;
    var lon = lonInput ? Number(lonInput.value) : NaN;

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      setFeedback("error", "Informe latitude e longitude válidas.");
      return;
    }

    if (button) {
      button.disabled = true;
    }

    setFeedback("pending", "Salvando ponto...");
    fetchJson("api/tracked/" + encodeURIComponent(areaId) + "/marker", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lat: lat, lon: lon }),
    })
      .then(function () {
        setFeedback("success", "Ponto salvo no município.");
        return Promise.all([loadTracked(), loadStatus()]);
      })
      .catch(function (error) {
        if (button) {
          button.disabled = false;
        }
        setFeedback("error", error.message || "Não foi possível salvar o ponto.");
      });
  }

  function removeMarker(areaId, button) {
    if (button) {
      button.disabled = true;
    }

    setFeedback("pending", "Removendo ponto...");
    fetchJson("api/tracked/" + encodeURIComponent(areaId) + "/marker", {
      method: "DELETE",
    })
      .then(function () {
        setFeedback("success", "Ponto removido.");
        return Promise.all([loadTracked(), loadStatus()]);
      })
      .catch(function (error) {
        if (button) {
          button.disabled = false;
        }
        setFeedback("error", error.message || "Não foi possível remover o ponto.");
      });
  }

  function renderStatus(status) {
    var rows = [
      ["Acompanhadas", safeValue(status.tracked_count)],
      ["Fila", safeValue(status.queue_length)],
      ["Arquivos brutos", safeValue(status.raw_frame_count)],
      ["Timestamps úteis", safeValue(status.raw_timestamp_count)],
      [
        "Cap de retenção",
        safeValue(
          status.raw_history_limit !== null && status.raw_history_limit !== undefined
            ? String(status.raw_history_limit) + " timestamps totais"
            : "Nenhum"
        ),
      ],
      ["Download bruto", safeValue(status.raw_download_summary)],
      ["Uso em disco (raws)", formatBytes(status.raw_disk_usage_bytes || 0)],
      ["Livre em disco", formatBytes(status.disk_free_bytes || 0)],
      ["Último bruto", safeValue(status.raw_frame_latest || "Nenhum")],
      ["Última checagem", formatStatusDateTime(status.last_poll_finished_at)],
      ["Novos downloads", safeValue(status.last_poll_new_downloads)],
      ["Alerta de disco", safeValue(status.disk_warning || "Nenhum")],
      ["Erro", safeValue(status.last_poll_error || "Nenhum")],
    ];
    var markup = [];
    var index;

    for (index = 0; index < rows.length; index += 1) {
      markup.push("<dt>" + escapeHtml(rows[index][0]) + "</dt>");
      markup.push("<dd>" + escapeHtml(rows[index][1]) + "</dd>");
    }

    elements.statusGrid.innerHTML = markup.join("");
  }

  function renderSearchLoading() {
    if (!String(state.query || "").trim()) {
      renderSearchResults();
      return;
    }

    elements.searchResults.innerHTML = '<p class="empty-state">Buscando...</p>';
  }

  function renderSearchResults() {
    var query = String(state.query || "").trim();
    var markup = [];
    var index;

    if (!query) {
      elements.searchResults.innerHTML =
        '<p class="empty-state">Digite um nome ou código para buscar.</p>';
      return;
    }

    if (!state.searchResults.length) {
      elements.searchResults.innerHTML = '<p class="empty-state">Nenhum município encontrado.</p>';
      return;
    }

    for (index = 0; index < state.searchResults.length; index += 1) {
      markup.push(renderSearchResult(state.searchResults[index]));
    }

    elements.searchResults.innerHTML = markup.join("");
  }

  function renderSearchResult(area) {
    var pending = !!state.pendingPins[area.area_id];
    var tracked = isTracked(area.area_id);
    var stateName = pending ? "pending" : tracked ? "tracked" : "";
    var disabledAttr = pending || tracked ? ' disabled="disabled"' : "";
    var populationText = "";
    var extraText = "";

    if (area.population !== null && area.population !== undefined) {
      populationText = " - Pop. " + formatNumber(area.population);
    }

    if (pending) {
      extraText = " - Adicionando...";
    } else if (tracked) {
      extraText = " - Já adicionada";
    }

    return (
      '<button class="search-result" type="button" data-area-id="' +
      escapeHtml(area.area_id) +
      '" data-state="' +
      escapeHtml(stateName) +
      '" aria-disabled="' +
      (pending || tracked ? "true" : "false") +
      '"' +
      disabledAttr +
      '>' +
      '<span class="result-name">' +
      escapeHtml(area.display_name) +
      "</span>" +
      '<span class="result-meta">' +
      escapeHtml(area.type_label + " - " + area.code_label + populationText + extraText) +
      "</span>" +
      "</button>"
    );
  }

  function renderTracked() {
    var markup = [];
    var pendingIds = Object.keys(state.pendingPins);
    var trackedIds = trackedIdMap();
    var index;

    for (index = 0; index < pendingIds.length; index += 1) {
      if (!trackedIds[pendingIds[index]]) {
        markup.push(renderPendingTrackedCard(state.pendingPins[pendingIds[index]]));
      }
    }

    for (index = 0; index < state.tracked.length; index += 1) {
      markup.push(renderTrackedCard(state.tracked[index]));
    }

    if (!markup.length) {
      elements.trackedList.innerHTML = '<p class="empty-state">Nenhum município adicionado ainda.</p>';
      return;
    }

    elements.trackedList.innerHTML = markup.join("");
  }

  function renderDownloads() {
    var markup = [];
    var index;

    if (!state.downloads.length) {
      elements.downloadsList.innerHTML =
        '<p class="empty-state">Nenhuma fonte raw disponível.</p>';
      return;
    }

    for (index = 0; index < state.downloads.length; index += 1) {
      markup.push(renderDownloadSourceCard(state.downloads[index]));
    }

    elements.downloadsList.innerHTML = markup.join("");
  }

  function renderDownloadSourceCard(source) {
    var activeDownloads = Array.isArray(source.active_downloads) ? source.active_downloads : [];
    var filesOnDisk = Array.isArray(source.files_on_disk) ? source.files_on_disk : [];
    var activeMarkup = "";
    var filesMarkup = "";
    var index;

    if (activeDownloads.length) {
      for (index = 0; index < activeDownloads.length; index += 1) {
        activeMarkup += renderActiveDownload(activeDownloads[index]);
      }
    } else {
      activeMarkup = '<p class="download-empty">Nenhum download em andamento.</p>';
    }

    if (filesOnDisk.length) {
      filesMarkup = '<ul class="raw-file-list">';
      for (index = 0; index < filesOnDisk.length; index += 1) {
        filesMarkup += renderRawFile(filesOnDisk[index]);
      }
      filesMarkup += "</ul>";
    } else {
      filesMarkup = '<p class="download-empty">Nenhum arquivo raw em disco.</p>';
    }

    return (
      '<article class="download-card">' +
      '<div class="tracked-header">' +
      "<div>" +
      '<h3 class="tracked-title">' +
      escapeHtml(source.source_label || "Fonte raw") +
      "</h3>" +
      '<p class="tracked-code">' +
      escapeHtml(source.summary || "") +
      "</p>" +
      "</div>" +
      '<span class="status-pill" data-status="' +
      escapeHtml(source.phase || "idle") +
      '">' +
      escapeHtml(source.phase_label || statusLabel(source.phase || "idle")) +
      "</span>" +
      "</div>" +
      '<dl class="download-meta">' +
      "<dt>Relevante</dt><dd>" +
      escapeHtml(source.is_relevant ? "sim" : "não") +
      "</dd>" +
      "<dt>Em disco</dt><dd>" +
      escapeHtml(String(source.file_count || 0)) +
      "</dd>" +
      "<dt>Último baixado</dt><dd>" +
      renderFilenameValue(source.last_downloaded) +
      "</dd>" +
      "<dt>Último disponível</dt><dd>" +
      renderFilenameValue(source.latest_available) +
      "</dd>" +
      "<dt>Tamanho em disco</dt><dd>" +
      escapeHtml(formatBytes(source.disk_usage_bytes || 0)) +
      "</dd>" +
      "</dl>" +
      '<div class="download-card-body">' +
      '<div class="download-column">' +
      '<div class="download-section">' +
      "<h4>Downloads em andamento</h4>" +
      activeMarkup +
      "</div>" +
      "</div>" +
      '<div class="download-column">' +
      '<div class="download-section">' +
      "<h4>Arquivos em disco</h4>" +
      filesMarkup +
      "</div>" +
      "</div>" +
      "</div>" +
      "</article>"
    );
  }

  function renderActiveDownload(item) {
    var fileLabel = describeRawFile(item.filename || "");
    var stage = String(item.stage || "downloading");
    var converting = stage === "converting";
    var percent = converting ? 100 : item.percent;
    var width = percent === null || percent === undefined ? 4 : Math.max(4, Math.min(100, percent));
    var progressText = converting
      ? escapeHtml(formatBytes(item.total_bytes || item.downloaded_bytes || 0)) + " • Convertendo"
      : escapeHtml(formatBytes(item.downloaded_bytes || 0)) +
        " / " +
        escapeHtml(item.total_bytes ? formatBytes(item.total_bytes) : "tamanho desconhecido") +
        (percent === null || percent === undefined ? "" : " (" + escapeHtml(String(percent)) + "%)");
    return (
      '<div class="download-item">' +
      '<div class="download-item-head">' +
      '<div class="download-item-copy">' +
      '<strong class="download-item-name">' +
      escapeHtml(fileLabel.primary) +
      "</strong>" +
      '<span class="download-item-meta">' +
      escapeHtml(fileLabel.secondary) +
      "</span>" +
      "</div>" +
      '<span class="download-item-size">' +
      progressText +
      "</span>" +
      "</div>" +
      '<div class="progress-bar"><span style="width:' +
      escapeHtml(String(width)) +
      '%"></span></div>' +
      "</div>"
    );
  }

  function renderRawFile(item) {
    var fileLabel = describeRawFile(item.filename || "");
    return (
      '<li class="raw-file-item">' +
      '<div class="raw-file-copy">' +
      '<strong class="raw-file-name">' +
      escapeHtml(fileLabel.primary) +
      "</strong>" +
      '<span class="raw-file-meta">' +
      escapeHtml(fileLabel.secondary) +
      "</span>" +
      "</div>" +
      '<span class="raw-file-size">' +
      escapeHtml(formatBytes(item.size_bytes || 0)) +
      "</span>" +
      "</li>"
    );
  }

  function renderPendingTrackedCard(area) {
    return (
      '<article class="tracked-card">' +
      '<div class="tracked-header">' +
      "<div>" +
      '<h3 class="tracked-title">' +
      escapeHtml(area.display_name) +
      "</h3>" +
      '<p class="tracked-code">' +
      escapeHtml(area.type_label + " - " + area.code_label) +
      "</p>" +
      "</div>" +
      '<span class="status-pill" data-status="queued">' + escapeHtml(statusLabel("queued")) + "</span>" +
      "</div>" +
      '<p class="tracked-note">' +
      escapeHtml(queuedHelperText()) +
      "</p>" +
      "</article>"
    );
  }

  function renderTrackedCard(area) {
    var previewMarkup = "";
    var errorMarkup = "";
    var noteMarkup = "";
    var actions = [];
    var markerMarkup = renderMarkerEditor(area);
    var previewUrl = buildPreviewUrl(area);

    if (area.media_exists && previewUrl) {
      previewMarkup =
        '<img class="tracked-preview" src="' +
        escapeHtml(previewUrl) +
        '" alt="' +
        escapeHtml(area.display_name) +
        '" />';
      actions.push(
        '<a class="view-gif" href="' +
          escapeHtml(previewUrl) +
          '" target="_blank" rel="noreferrer">Abrir prévia</a>'
      );
    }

    if (area.last_error) {
      errorMarkup = '<p class="tracked-error">' + escapeHtml(area.last_error) + "</p>";
    } else if (String(area.status || "").toLowerCase() === "queued") {
      noteMarkup = '<p class="tracked-note">' + escapeHtml(queuedHelperText()) + "</p>";
    }

    actions.push(
      '<button class="remove-track" type="button" data-action="remove" data-area-id="' +
        escapeHtml(area.area_id) +
        '" data-display-name="' +
        escapeHtml(area.display_name) +
        '">Remover</button>'
    );

    return (
      '<article class="tracked-card">' +
      '<div class="tracked-header">' +
      "<div>" +
      '<h3 class="tracked-title">' +
      escapeHtml(area.display_name) +
      "</h3>" +
      '<p class="tracked-code">' +
      escapeHtml(area.type_label + " - " + area.code_label) +
      "</p>" +
      "</div>" +
      '<span class="status-pill" data-status="' +
      escapeHtml(area.status || "") +
      '">' +
      escapeHtml(statusLabel(area.status || "queued")) +
      "</span>" +
      "</div>" +
      previewMarkup +
      markerMarkup +
      noteMarkup +
      errorMarkup +
      '<div class="tracked-actions">' +
      actions.join("") +
      "</div>" +
      "</article>"
    );
  }

  function renderMarkerEditor(area) {
    var hasMarker = area.marker_lat !== null && area.marker_lat !== undefined &&
      area.marker_lon !== null && area.marker_lon !== undefined;
    var latValue = hasMarker ? String(area.marker_lat) : "";
    var lonValue = hasMarker ? String(area.marker_lon) : "";

    return (
      '<section class="marker-editor">' +
      '<div class="marker-editor-copy">' +
      '<strong class="marker-editor-title">Ponto opcional no município</strong>' +
      '<span class="marker-editor-note">' +
      escapeHtml(hasMarker ? "Ponto salvo. Você pode atualizar ou remover." : "Informe latitude e longitude para marcar um ponto no timelapse.") +
      "</span>" +
      "</div>" +
      '<div class="marker-editor-fields">' +
      '<label class="marker-field">' +
      "<span>Latitude</span>" +
      '<input type="number" step="0.000001" inputmode="decimal" data-marker-field="lat" value="' +
      escapeHtml(latValue) +
      '" placeholder="-20.3774" />' +
      "</label>" +
      '<label class="marker-field">' +
      "<span>Longitude</span>" +
      '<input type="number" step="0.000001" inputmode="decimal" data-marker-field="lon" value="' +
      escapeHtml(lonValue) +
      '" placeholder="-40.2976" />' +
      "</label>" +
      "</div>" +
      '<div class="marker-editor-actions">' +
      '<button class="marker-save" type="button" data-action="save-marker" data-area-id="' +
      escapeHtml(area.area_id) +
      '">' +
      escapeHtml(hasMarker ? "Atualizar ponto" : "Salvar ponto") +
      "</button>" +
      (hasMarker
        ? '<button class="marker-remove" type="button" data-action="remove-marker" data-area-id="' +
          escapeHtml(area.area_id) +
          '">Remover ponto</button>'
        : "") +
      "</div>" +
      "</section>"
    );
  }

  function setFeedback(tone, message) {
    elements.searchFeedback.hidden = false;
    elements.searchFeedback.setAttribute("data-tone", tone || "success");
    elements.searchFeedback.textContent = message || "";
  }

  function clearFeedback() {
    elements.searchFeedback.hidden = true;
    elements.searchFeedback.removeAttribute("data-tone");
    elements.searchFeedback.textContent = "";
  }

  function fetchJson(path, options) {
    return window
      .fetch(buildUrl(path), options || {})
      .then(function (response) {
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
    if (!previewPath) {
      return "";
    }

    var url = buildUrl(previewPath);
    var cacheBuster = area.media_version || area.latest_source_timestamp || "";
    if (cacheBuster) {
      url += (url.indexOf("?") === -1 ? "?" : "&") + "v=" + encodeURIComponent(cacheBuster);
    }
    return url;
  }

  function buildUrl(path) {
    return basePath + String(path || "").replace(/^\/+/, "");
  }

  function applyInitialTheme() {
    var storedTheme = null;
    try {
      storedTheme = window.localStorage.getItem(THEME_STORAGE_KEY);
    } catch (error) {
      storedTheme = null;
    }

    if (storedTheme !== "light" && storedTheme !== "dark") {
      storedTheme = "dark";
    }

    setTheme(storedTheme);
  }

  function setTheme(theme) {
    var root = document.documentElement;
    var normalizedTheme = theme === "light" ? "light" : "dark";

    root.setAttribute("data-theme", normalizedTheme);
    if (elements.themeToggle) {
      elements.themeToggle.setAttribute(
        "aria-pressed",
        normalizedTheme === "dark" ? "true" : "false"
      );
      elements.themeToggle.setAttribute(
        "aria-label",
        normalizedTheme === "dark" ? "Mudar para tema claro" : "Mudar para tema escuro"
      );
    }

    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, normalizedTheme);
    } catch (error) {
      return;
    }
  }

  function getCurrentTheme() {
    var theme = document.documentElement.getAttribute("data-theme") || "dark";
    return theme === "light" ? "light" : "dark";
  }

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

  function trackedIdMap() {
    var ids = {};
    var index;

    for (index = 0; index < state.tracked.length; index += 1) {
      ids[state.tracked[index].area_id] = true;
    }

    return ids;
  }

  function isTracked(areaId) {
    var index;
    for (index = 0; index < state.tracked.length; index += 1) {
      if (state.tracked[index].area_id === areaId) {
        return true;
      }
    }
    return false;
  }

  function findAreaById(areaId) {
    var index;
    for (index = 0; index < state.searchResults.length; index += 1) {
      if (state.searchResults[index].area_id === areaId) {
        return state.searchResults[index];
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

  function renderFilenameValue(filename) {
    var fileLabel = describeRawFile(filename || "");
    return (
      '<span class="file-meta-value">' +
      '<strong class="file-meta-primary">' +
      escapeHtml(fileLabel.primary) +
      "</strong>" +
      '<span class="file-meta-secondary">' +
      escapeHtml(fileLabel.secondary) +
      "</span>" +
      "</span>"
    );
  }

  function describeRawFile(filename) {
    var captureDate = parseGoesCaptureDate(filename);
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
    for (index = 0; index < state.downloads.length; index += 1) {
      if (
        state.downloads[index].phase === "downloading" ||
        (Array.isArray(state.downloads[index].active_downloads) &&
          state.downloads[index].active_downloads.length > 0)
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
    var status = state.status || {};
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
})();
