(function (window) {
  "use strict";

  var app = window.GoesTimelapseApp;

  if (!app) {
    return;
  }

  app.renderStatus = renderStatus;
  app.renderSearchLoading = renderSearchLoading;
  app.renderSearchResults = renderSearchResults;
  app.renderTracked = renderTracked;
  app.renderDownloads = renderDownloads;

  function renderStatus(status) {
    var rows = [
      ["Acompanhadas", app.safeValue(status.tracked_count)],
      ["Fila", app.safeValue(status.queue_length)],
      ["Arquivos brutos", app.safeValue(status.raw_frame_count)],
      ["Timestamps úteis", app.safeValue(status.raw_timestamp_count)],
      [
        "Cap de retenção",
        app.safeValue(
          status.raw_history_limit !== null && status.raw_history_limit !== undefined
            ? String(status.raw_history_limit) + " timestamps totais"
            : "Nenhum"
        ),
      ],
      ["Download bruto", app.safeValue(status.raw_download_summary)],
      ["Uso em disco (raws)", app.formatBytes(status.raw_disk_usage_bytes || 0)],
      ["Livre no cache", app.formatBytes(status.disk_free_bytes || 0)],
      ["Livre no staging", app.formatBytes(status.staging_free_bytes || 0)],
      ["Path do staging", app.safeValue(status.staging_path || "-")],
      ["Último bruto", app.safeValue(status.raw_frame_latest || "Nenhum")],
      ["Última checagem", app.formatStatusDateTime(status.last_poll_finished_at)],
      ["Novos downloads", app.safeValue(status.last_poll_new_downloads)],
      ["Alerta de storage", app.safeValue(status.disk_warning || "Nenhum")],
      ["Erro", app.safeValue(status.last_poll_error || "Nenhum")],
    ];
    var markup = [];
    var index;

    for (index = 0; index < rows.length; index += 1) {
      markup.push("<dt>" + app.escapeHtml(rows[index][0]) + "</dt>");
      markup.push("<dd>" + app.escapeHtml(rows[index][1]) + "</dd>");
    }

    app.elements.statusGrid.innerHTML = markup.join("");
  }

  function renderSearchLoading() {
    if (!String(app.state.query || "").trim()) {
      renderSearchResults();
      return;
    }

    app.elements.searchResults.innerHTML = '<p class="empty-state">Buscando...</p>';
  }

  function renderSearchResults() {
    var query = String(app.state.query || "").trim();
    var markup = [];
    var index;

    if (!query) {
      app.elements.searchResults.innerHTML =
        '<p class="empty-state">Digite um nome ou código para buscar.</p>';
      return;
    }

    if (!app.state.searchResults.length) {
      app.elements.searchResults.innerHTML =
        '<p class="empty-state">Nenhum município encontrado.</p>';
      return;
    }

    for (index = 0; index < app.state.searchResults.length; index += 1) {
      markup.push(renderSearchResult(app.state.searchResults[index]));
    }

    app.elements.searchResults.innerHTML = markup.join("");
  }

  function renderSearchResult(area) {
    var pending = !!app.state.pendingPins[area.area_id];
    var tracked = app.isTracked(area.area_id);
    var stateName = pending ? "pending" : tracked ? "tracked" : "";
    var disabledAttr = pending || tracked ? ' disabled="disabled"' : "";
    var populationText = "";
    var extraText = "";

    if (area.population !== null && area.population !== undefined) {
      populationText = " - Pop. " + app.formatNumber(area.population);
    }

    if (pending) {
      extraText = " - Adicionando...";
    } else if (tracked) {
      extraText = " - Já adicionada";
    }

    return (
      '<button class="search-result" type="button" data-area-id="' +
      app.escapeHtml(area.area_id) +
      '" data-state="' +
      app.escapeHtml(stateName) +
      '" aria-disabled="' +
      (pending || tracked ? "true" : "false") +
      '"' +
      disabledAttr +
      ">" +
      '<span class="result-name">' +
      app.escapeHtml(area.display_name) +
      "</span>" +
      '<span class="result-meta">' +
      app.escapeHtml(area.type_label + " - " + area.code_label + populationText + extraText) +
      "</span>" +
      "</button>"
    );
  }

  function renderTracked() {
    var markup = [];
    var pendingIds = Object.keys(app.state.pendingPins);
    var trackedIds = app.trackedIdMap();
    var index;

    for (index = 0; index < pendingIds.length; index += 1) {
      if (!trackedIds[pendingIds[index]]) {
        markup.push(renderPendingTrackedCard(app.state.pendingPins[pendingIds[index]]));
      }
    }

    for (index = 0; index < app.state.tracked.length; index += 1) {
      markup.push(renderTrackedCard(app.state.tracked[index]));
    }

    if (!markup.length) {
      app.elements.trackedList.innerHTML =
        '<p class="empty-state">Nenhum município adicionado ainda.</p>';
      return;
    }

    app.elements.trackedList.innerHTML = markup.join("");
  }

  function renderDownloads() {
    var markup = [];
    var index;
    var visibleDownloads = app.state.downloads.filter(function (source) {
      return String(source.source_key || "") !== "lightning";
    });

    if (!visibleDownloads.length) {
      app.elements.downloadsList.innerHTML =
        '<p class="empty-state">Nenhuma fonte raw disponível.</p>';
      return;
    }

    for (index = 0; index < visibleDownloads.length; index += 1) {
      markup.push(renderDownloadSourceCard(visibleDownloads[index]));
    }

    app.elements.downloadsList.innerHTML = markup.join("");
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
      app.escapeHtml(source.source_label || "Fonte raw") +
      "</h3>" +
      '<p class="tracked-code">' +
      app.escapeHtml(source.summary || "") +
      "</p>" +
      "</div>" +
      '<span class="status-pill" data-status="' +
      app.escapeHtml(source.phase || "idle") +
      '">' +
      app.escapeHtml(source.phase_label || app.statusLabel(source.phase || "idle")) +
      "</span>" +
      "</div>" +
      '<dl class="download-meta">' +
      "<dt>Relevante</dt><dd>" +
      app.escapeHtml(source.is_relevant ? "sim" : "não") +
      "</dd>" +
      "<dt>Em disco</dt><dd>" +
      app.escapeHtml(String(source.file_count || 0)) +
      "</dd>" +
      "<dt>Último baixado</dt><dd>" +
      renderFilenameValue(source.last_downloaded) +
      "</dd>" +
      "<dt>Último disponível</dt><dd>" +
      renderFilenameValue(source.latest_available) +
      "</dd>" +
      "<dt>Tamanho em disco</dt><dd>" +
      app.escapeHtml(app.formatBytes(source.disk_usage_bytes || 0)) +
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
    var fileLabel = app.describeRawFile(item.filename || "");
    var stage = String(item.stage || "downloading");
    var queued = stage === "queued";
    var converting = stage === "converting";
    var percent = converting || queued ? 100 : item.percent;
    var width = percent === null || percent === undefined ? 4 : Math.max(4, Math.min(100, percent));
    var progressText = converting
      ? app.escapeHtml(app.formatBytes(item.total_bytes || item.downloaded_bytes || 0)) +
        " • Convertendo"
      : queued
      ? app.escapeHtml(app.formatBytes(item.total_bytes || item.downloaded_bytes || 0)) +
        " • Na fila de conversão"
      : app.escapeHtml(app.formatBytes(item.downloaded_bytes || 0)) +
        " / " +
        app.escapeHtml(item.total_bytes ? app.formatBytes(item.total_bytes) : "tamanho desconhecido") +
        (percent === null || percent === undefined
          ? ""
          : " (" + app.escapeHtml(String(percent)) + "%)");

    return (
      '<div class="download-item">' +
      '<div class="download-item-head">' +
      '<div class="download-item-copy">' +
      '<strong class="download-item-name">' +
      app.escapeHtml(fileLabel.primary) +
      "</strong>" +
      '<span class="download-item-meta">' +
      app.escapeHtml(fileLabel.secondary) +
      "</span>" +
      "</div>" +
      '<span class="download-item-size">' +
      progressText +
      "</span>" +
      "</div>" +
      '<div class="progress-bar" data-stage="' +
      app.escapeHtml(stage) +
      '"><span style="width:' +
      app.escapeHtml(String(width)) +
      '%"></span></div>' +
      "</div>"
    );
  }

  function renderRawFile(item) {
    var fileLabel = app.describeRawFile(item.filename || "");
    return (
      '<li class="raw-file-item">' +
      '<div class="raw-file-copy">' +
      '<strong class="raw-file-name">' +
      app.escapeHtml(fileLabel.primary) +
      "</strong>" +
      '<span class="raw-file-meta">' +
      app.escapeHtml(fileLabel.secondary) +
      "</span>" +
      "</div>" +
      '<span class="raw-file-size">' +
      app.escapeHtml(app.formatBytes(item.size_bytes || 0)) +
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
      app.escapeHtml(area.display_name) +
      "</h3>" +
      '<p class="tracked-code">' +
      app.escapeHtml(area.type_label + " - " + area.code_label) +
      "</p>" +
      "</div>" +
      '<span class="status-pill" data-status="queued">' +
      app.escapeHtml(app.statusLabel("queued")) +
      "</span>" +
      "</div>" +
      '<p class="tracked-note">' +
      app.escapeHtml(app.queuedHelperText()) +
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
    var previewUrl = app.buildPreviewUrl(area);

    if (area.media_exists && previewUrl) {
      previewMarkup =
        '<img class="tracked-preview" src="' +
        app.escapeHtml(previewUrl) +
        '" alt="' +
        app.escapeHtml(area.display_name) +
        '" />';
      actions.push(
        '<a class="view-gif" href="' +
          app.escapeHtml(previewUrl) +
          '" target="_blank" rel="noreferrer">Abrir prévia</a>'
      );
    }

    if (area.last_error) {
      errorMarkup = '<p class="tracked-error">' + app.escapeHtml(area.last_error) + "</p>";
    } else if (String(area.status || "").toLowerCase() === "queued") {
      noteMarkup = '<p class="tracked-note">' + app.escapeHtml(app.queuedHelperText()) + "</p>";
    }

    actions.push(
      '<button class="remove-track" type="button" data-action="remove" data-area-id="' +
        app.escapeHtml(area.area_id) +
        '" data-display-name="' +
        app.escapeHtml(area.display_name) +
        '">Remover</button>'
    );

    return (
      '<article class="tracked-card">' +
      '<div class="tracked-header">' +
      "<div>" +
      '<h3 class="tracked-title">' +
      app.escapeHtml(area.display_name) +
      "</h3>" +
      '<p class="tracked-code">' +
      app.escapeHtml(area.type_label + " - " + area.code_label) +
      "</p>" +
      "</div>" +
      '<span class="status-pill" data-status="' +
      app.escapeHtml(area.status || "") +
      '">' +
      app.escapeHtml(app.statusLabel(area.status || "queued")) +
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
    var hasMarker =
      area.marker_lat !== null &&
      area.marker_lat !== undefined &&
      area.marker_lon !== null &&
      area.marker_lon !== undefined;
    var draftValue = app.state.markerDrafts[area.area_id];
    var fieldValue = draftValue !== undefined
      ? draftValue
      : hasMarker
      ? app.formatMarkerCoordinates(area.marker_lat, area.marker_lon)
      : "";
    var errorMessage = app.state.markerErrors[area.area_id] || "";

    return (
      '<section class="marker-editor">' +
      '<div class="marker-editor-copy">' +
      '<strong class="marker-editor-title">Ponto opcional no município</strong>' +
      '<span class="marker-editor-note">' +
      app.escapeHtml(
        hasMarker
          ? "Ponto salvo. Você pode atualizar ou remover."
          : "Informe coordenadas no formato latitude, longitude."
      ) +
      "</span>" +
      "</div>" +
      '<div class="marker-editor-fields">' +
      '<label class="marker-field">' +
      "<span>Coordenadas</span>" +
      '<input type="text" inputmode="decimal" data-marker-field="coordinates" data-area-id="' +
      app.escapeHtml(area.area_id) +
      '" value="' +
      app.escapeHtml(fieldValue) +
      '" placeholder="-12.345678, -45.678901"' +
      (errorMessage ? ' aria-invalid="true"' : "") +
      " />" +
      "</label>" +
      (errorMessage
        ? '<p class="marker-editor-error">' + app.escapeHtml(errorMessage) + "</p>"
        : "") +
      "</div>" +
      '<div class="marker-editor-actions">' +
      '<button class="marker-save" type="button" data-action="save-marker" data-area-id="' +
      app.escapeHtml(area.area_id) +
      '">' +
      app.escapeHtml(hasMarker ? "Atualizar ponto" : "Salvar ponto") +
      "</button>" +
      (hasMarker
        ? '<button class="marker-remove" type="button" data-action="remove-marker" data-area-id="' +
          app.escapeHtml(area.area_id) +
          '">Remover ponto</button>'
        : "") +
      "</div>" +
      "</section>"
    );
  }

  function renderFilenameValue(filename) {
    var fileLabel = app.describeRawFile(filename || "");
    return (
      '<span class="file-meta-value">' +
      '<strong class="file-meta-primary">' +
      app.escapeHtml(fileLabel.primary) +
      "</strong>" +
      '<span class="file-meta-secondary">' +
      app.escapeHtml(fileLabel.secondary) +
      "</span>" +
      "</span>"
    );
  }
})(window);
