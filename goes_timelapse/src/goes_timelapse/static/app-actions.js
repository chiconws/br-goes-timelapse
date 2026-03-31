(function (window, document) {
  "use strict";

  var app = window.GoesTimelapseApp;

  if (!app) {
    return;
  }

  app.initialize = initialize;

  function initialize() {
    app.setBasePath(document.documentElement.getAttribute("data-base-path") || "/");

    app.elements.statusGrid = document.getElementById("status-grid");
    app.elements.searchInput = document.getElementById("area-search");
    app.elements.themeToggle = document.getElementById("theme-toggle");
    app.elements.searchFeedback = document.getElementById("search-feedback");
    app.elements.searchResults = document.getElementById("search-results");
    app.elements.trackedList = document.getElementById("tracked-list");
    app.elements.downloadsList = document.getElementById("downloads-list");
    if (
      !app.elements.statusGrid ||
      !app.elements.searchInput ||
      !app.elements.themeToggle ||
      !app.elements.searchResults ||
      !app.elements.trackedList ||
      !app.elements.downloadsList
    ) {
      return;
    }

    applyInitialTheme();

    app.elements.searchInput.addEventListener("input", onSearchInput);
    app.elements.themeToggle.addEventListener("click", onThemeToggleClick);
    app.elements.searchResults.addEventListener("click", onSearchResultClick);
    app.elements.trackedList.addEventListener("click", onTrackedActionClick);
    app.elements.trackedList.addEventListener("input", onTrackedInputChange);

    refreshDashboard();
  }

  function refreshDashboard() {
    return Promise.all([loadStatus(), loadTracked(), loadDownloads()]).finally(scheduleRefresh);
  }

  function scheduleRefresh() {
    if (app.state.refreshTimer) {
      window.clearTimeout(app.state.refreshTimer);
    }
    app.state.refreshTimer = window.setTimeout(
      refreshDashboard,
      app.hasActiveDownloads()
        ? app.constants.ACTIVE_DOWNLOAD_REFRESH_MS
        : app.constants.STATUS_REFRESH_MS
    );
  }

  function onSearchInput(event) {
    var value = event.target.value || "";
    app.state.query = value;

    if (app.state.searchTimer) {
      window.clearTimeout(app.state.searchTimer);
    }

    if (!value.trim()) {
      app.state.searchResults = [];
      app.renderSearchResults();
      clearFeedback();
      return;
    }

    app.renderSearchLoading();
    app.state.searchTimer = window.setTimeout(function () {
      loadSearch(value);
    }, app.constants.SEARCH_DEBOUNCE_MS);
  }

  function onThemeToggleClick() {
    var currentTheme = getCurrentTheme();
    var nextTheme = currentTheme === "dark" ? "light" : "dark";
    setTheme(nextTheme);
  }

  function onSearchResultClick(event) {
    var button = app.findActionNode(event.target, "data-area-id", app.elements.searchResults);
    var areaId;
    var area;

    if (!button || button.disabled) {
      return;
    }

    areaId = button.getAttribute("data-area-id");
    area = app.findAreaById(areaId);
    if (!area) {
      return;
    }

    addTracked(area);
  }

  function onTrackedActionClick(event) {
    var button = app.findActionNode(event.target, "data-action", app.elements.trackedList);
    var action;
    var areaId;

    if (!button || button.disabled) {
      return;
    }

    action = button.getAttribute("data-action");
    areaId = button.getAttribute("data-area-id");
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

  function onTrackedInputChange(event) {
    var input = app.findActionNode(event.target, "data-marker-field", app.elements.trackedList);
    var areaId;

    if (!input || input.getAttribute("data-marker-field") !== "coordinates") {
      return;
    }

    areaId = input.getAttribute("data-area-id");
    if (!areaId) {
      return;
    }

    app.state.markerDrafts[areaId] = input.value;
    delete app.state.markerErrors[areaId];
  }

  function loadStatus() {
    return app.fetchJson("api/status")
      .then(function (payload) {
        var nextStatusSignature = JSON.stringify(payload || {});
        var nextQueuedHelperSignature = app.buildQueuedHelperSignature(payload);
        var shouldRenderStatus = nextStatusSignature !== app.state.statusSignature;
        var shouldRenderTracked = nextQueuedHelperSignature !== app.state.queuedHelperSignature;

        app.state.status = payload;
        app.state.statusSignature = nextStatusSignature;
        app.state.queuedHelperSignature = nextQueuedHelperSignature;

        if (shouldRenderStatus) {
          app.renderStatus(payload);
        }

        if (shouldRenderTracked) {
          app.renderTracked();
        }
      })
      .catch(function () {
        app.state.status = {
          tracked_count: "-",
          queue_length: "-",
          raw_frame_count: "-",
          raw_download_summary: "Falha ao consultar o status",
          raw_frame_latest: null,
          last_poll_finished_at: "Indisponível",
          last_poll_new_downloads: "-",
          last_poll_error: "Falha ao consultar o status",
        };
        app.state.statusSignature = JSON.stringify(app.state.status);
        app.state.queuedHelperSignature = app.buildQueuedHelperSignature(app.state.status);
        app.renderStatus(app.state.status);
        app.renderTracked();
      });
  }

  function loadTracked() {
    return app.fetchJson("api/tracked")
      .then(function (payload) {
        var normalized = Array.isArray(payload) ? payload : [];
        var nextSignature = JSON.stringify(normalized);
        var shouldRenderTracked = nextSignature !== app.state.trackedSignature;

        app.state.tracked = normalized;
        app.state.trackedSignature = nextSignature;

        if (shouldRenderTracked) {
          app.renderTracked();
          app.renderSearchResults();
        }
      })
      .catch(function () {
        app.elements.trackedList.innerHTML =
          '<p class="empty-state">Os municípios acompanhados estão indisponíveis no momento.</p>';
      });
  }

  function loadDownloads() {
    return app.fetchJson("api/downloads")
      .then(function (payload) {
        var normalized = payload && Array.isArray(payload.sources) ? payload.sources : [];
        var nextSignature = JSON.stringify(normalized);

        app.state.downloads = normalized;

        if (nextSignature !== app.state.downloadsSignature) {
          app.state.downloadsSignature = nextSignature;
          app.renderDownloads();
        }
      })
      .catch(function () {
        app.elements.downloadsList.innerHTML =
          '<p class="empty-state">Os detalhes de download estão indisponíveis no momento.</p>';
      });
  }

  function loadSearch(query) {
    var cleaned = String(query || "").trim();
    var requestId = app.state.searchRequestId + 1;
    app.state.searchRequestId = requestId;

    return app.fetchJson("api/areas?q=" + encodeURIComponent(cleaned))
      .then(function (payload) {
        if (requestId !== app.state.searchRequestId) {
          return;
        }

        if (cleaned !== String(app.state.query || "").trim()) {
          return;
        }

        app.state.searchResults = Array.isArray(payload) ? payload : [];
        app.renderSearchResults();
      })
      .catch(function (error) {
        if (requestId !== app.state.searchRequestId) {
          return;
        }

        app.elements.searchResults.innerHTML =
          '<p class="empty-state">' + app.escapeHtml(error.message || "Busca indisponível.") + "</p>";
      });
  }

  function addTracked(area) {
    if (!area || app.state.pendingPins[area.area_id] || app.isTracked(area.area_id)) {
      return;
    }

    app.state.pendingPins[area.area_id] = {
      area_id: area.area_id,
      display_name: area.display_name,
      type_label: area.type_label,
      code_label: area.code_label,
    };

    setFeedback("pending", "Adicionando " + area.display_name + "...");
    app.renderSearchResults();

    app.fetchJson("api/tracked/" + encodeURIComponent(area.area_id), {
      method: "PUT",
    })
      .then(function () {
        delete app.state.pendingPins[area.area_id];
        setFeedback("success", area.display_name + " entrou na fila de renderização.");
        return Promise.all([loadTracked(), loadStatus()]);
      })
      .catch(function (error) {
        delete app.state.pendingPins[area.area_id];
        setFeedback("error", error.message || "Não foi possível adicionar o município.");
        app.renderSearchResults();
      });
  }

  function removeTracked(areaId, displayName, button) {
    if (button) {
      button.disabled = true;
    }

    setFeedback("pending", "Removendo " + displayName + "...");
    app.fetchJson("api/tracked/" + encodeURIComponent(areaId), {
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
    var coordinatesInput = editor ? editor.querySelector('[data-marker-field="coordinates"]') : null;
    var parsedCoordinates = app.parseMarkerCoordinates(coordinatesInput ? coordinatesInput.value : "");
    var lat = parsedCoordinates ? parsedCoordinates.lat : NaN;
    var lon = parsedCoordinates ? parsedCoordinates.lon : NaN;

    if (!parsedCoordinates) {
      app.state.markerErrors[areaId] =
        "Informe as coordenadas no formato latitude, longitude";
      app.renderTracked();
      setFeedback("error", app.state.markerErrors[areaId]);
      return;
    }

    if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
      app.state.markerErrors[areaId] =
        "Informe coordenadas válidas dentro das faixas de latitude e longitude";
      app.renderTracked();
      setFeedback("error", app.state.markerErrors[areaId]);
      return;
    }

    app.state.markerDrafts[areaId] = app.formatMarkerCoordinates(lat, lon);
    delete app.state.markerErrors[areaId];

    if (button) {
      button.disabled = true;
    }

    setFeedback("pending", "Salvando ponto...");
    app.fetchJson("api/tracked/" + encodeURIComponent(areaId) + "/marker", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lat: lat, lon: lon }),
    })
      .then(function (payload) {
        if (
          payload &&
          payload.marker_lat !== null &&
          payload.marker_lat !== undefined &&
          payload.marker_lon !== null &&
          payload.marker_lon !== undefined
        ) {
          app.state.markerDrafts[areaId] = app.formatMarkerCoordinates(
            payload.marker_lat,
            payload.marker_lon
          );
        } else {
          delete app.state.markerDrafts[areaId];
        }
        delete app.state.markerErrors[areaId];
        setFeedback("success", "Ponto salvo no município.");
        return Promise.all([loadTracked(), loadStatus()]);
      })
      .catch(function (error) {
        if (button) {
          button.disabled = false;
        }
        app.state.markerErrors[areaId] =
          error.message || "Não foi possível salvar o ponto.";
        app.renderTracked();
        setFeedback("error", error.message || "Não foi possível salvar o ponto.");
      });
  }

  function removeMarker(areaId, button) {
    if (button) {
      button.disabled = true;
    }

    setFeedback("pending", "Removendo ponto...");
    app.fetchJson("api/tracked/" + encodeURIComponent(areaId) + "/marker", {
      method: "DELETE",
    })
      .then(function () {
        delete app.state.markerDrafts[areaId];
        delete app.state.markerErrors[areaId];
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

  function setFeedback(tone, message) {
    app.elements.searchFeedback.hidden = false;
    app.elements.searchFeedback.setAttribute("data-tone", tone || "success");
    app.elements.searchFeedback.textContent = message || "";
  }

  function clearFeedback() {
    app.elements.searchFeedback.hidden = true;
    app.elements.searchFeedback.removeAttribute("data-tone");
    app.elements.searchFeedback.textContent = "";
  }

  function applyInitialTheme() {
    var storedTheme = null;
    try {
      storedTheme = window.localStorage.getItem(app.constants.THEME_STORAGE_KEY);
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
    if (app.elements.themeToggle) {
      app.elements.themeToggle.setAttribute(
        "aria-pressed",
        normalizedTheme === "dark" ? "true" : "false"
      );
      app.elements.themeToggle.setAttribute(
        "aria-label",
        normalizedTheme === "dark" ? "Mudar para tema claro" : "Mudar para tema escuro"
      );
    }

    try {
      window.localStorage.setItem(app.constants.THEME_STORAGE_KEY, normalizedTheme);
    } catch (error) {
      return;
    }
  }

  function getCurrentTheme() {
    var theme = document.documentElement.getAttribute("data-theme") || "dark";
    return theme === "light" ? "light" : "dark";
  }
})(window, document);
