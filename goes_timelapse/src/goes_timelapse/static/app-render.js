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

  var SOURCE_LABELS = { visible: "Visível", infrared: "Infravermelho" };

  function renderStatus(status) {
    var rows = [
      ["Municípios", app.safeValue(status.tracked_count)],
      ["Na fila", app.safeValue(status.queue_length)],
      ["Arquivos em disco", app.safeValue(status.raw_frame_count)],
      ["Espaço usado", app.formatBytes(status.raw_disk_usage_bytes || 0)],
      ["Espaço livre", app.formatBytes(status.disk_free_bytes || 0)],
      ["Última checagem", app.formatStatusDateTime(status.last_poll_finished_at)],
      ["Quadros novos", app.safeValue(status.last_poll_new_downloads)],
    ];
    var alerts = [
      ["Alerta", status.disk_warning],
      ["Erro", status.last_poll_error],
    ];
    var markup = [];
    var index;

    for (index = 0; index < rows.length; index += 1) {
      markup.push("<dt>" + app.escapeHtml(rows[index][0]) + "</dt>");
      markup.push("<dd>" + app.escapeHtml(rows[index][1]) + "</dd>");
    }

    for (index = 0; index < alerts.length; index += 1) {
      if (alerts[index][1]) {
        markup.push("<dt>" + app.escapeHtml(alerts[index][0]) + "</dt>");
        markup.push(
          '<dd class="service-fact-error">' + app.escapeHtml(alerts[index][1]) + "</dd>"
        );
      }
    }

    app.elements.statusGrid.innerHTML = markup.join("");
    renderServiceSummary(status);
  }

  function renderServiceSummary(status) {
    var summary = app.elements.serviceSummary;

    if (status.last_poll_error) {
      summary.setAttribute("data-tone", "error");
      summary.textContent = "Erro no último ciclo: " + status.last_poll_error;
      return;
    }

    if (status.disk_warning) {
      summary.setAttribute("data-tone", "warning");
      summary.textContent = status.disk_warning;
      return;
    }

    summary.removeAttribute("data-tone");
    summary.textContent =
      "Serviço em dia. Última checagem: " +
      app.formatStatusDateTime(status.last_poll_finished_at) +
      ". " +
      app.formatBytes(status.raw_disk_usage_bytes || 0) +
      " em disco.";
  }

  function renderSearchLoading() {
    if (!String(app.state.query || "").trim()) {
      renderSearchResults();
      return;
    }

    showSearchResults("<p>Buscando...</p>");
  }

  function showSearchResults(markup) {
    app.elements.searchResults.hidden = false;
    app.elements.searchResults.innerHTML = markup;
  }

  function renderSearchResults() {
    var query = String(app.state.query || "").trim();
    var markup = [];
    var index;

    if (!query) {
      app.elements.searchResults.hidden = true;
      app.elements.searchResults.innerHTML = "";
      return;
    }

    if (!app.state.searchResults.length) {
      showSearchResults("<p>Nenhum município encontrado para essa busca.</p>");
      return;
    }

    for (index = 0; index < app.state.searchResults.length; index += 1) {
      markup.push(renderSearchResult(app.state.searchResults[index]));
    }

    showSearchResults(markup.join(""));
  }

  function renderSearchResult(area) {
    var pending = !!app.state.pendingPins[area.area_id];
    var tracked = app.isTracked(area.area_id);
    var meta = [area.code_label];

    if (area.population !== null && area.population !== undefined) {
      meta.push("Pop. " + app.formatNumber(area.population));
    }

    if (pending) {
      meta.push("Adicionando...");
    } else if (tracked) {
      meta.push("Já adicionado");
    }

    return (
      '<button class="search-result" type="button" data-area-id="' +
      app.escapeHtml(area.area_id) +
      '"' +
      (pending || tracked ? " disabled" : "") +
      ">" +
      '<span class="result-name">' +
      app.escapeHtml(area.display_name) +
      "</span>" +
      '<span class="result-meta">' +
      app.escapeHtml(meta.join(", ")) +
      "</span>" +
      "</button>"
    );
  }

  function viewerItems() {
    var trackedIds = app.trackedIdMap();
    var pending = Object.keys(app.state.pendingPins)
      .filter(function (areaId) {
        return !trackedIds[areaId];
      })
      .map(function (areaId) {
        return Object.assign({ status: "queued", pending: true }, app.state.pendingPins[areaId]);
      });

    return app.state.tracked.concat(pending);
  }

  function renderTracked() {
    var items = viewerItems();
    var selected = items[0];
    var index;

    if (!items.length) {
      app.elements.trackedList.innerHTML =
        '<p class="viewer-empty">Busque um município acima para começar a acompanhar. ' +
        "A primeira animação fica pronta em alguns minutos.</p>";
      return;
    }

    for (index = 0; index < items.length; index += 1) {
      if (items[index].area_id === app.state.selectedAreaId) {
        selected = items[index];
      }
    }

    app.elements.trackedList.innerHTML =
      renderStage(selected) + (items.length > 1 ? renderRail(items, selected) : "");
  }

  function renderStage(area) {
    var previewUrl = area.media_exists ? app.buildPreviewUrl(area) : "";
    var frame = previewUrl
      ? '<img src="' +
        app.escapeHtml(previewUrl) +
        '" alt="Animação de satélite de ' +
        app.escapeHtml(area.display_name) +
        '" />'
      : "<p>" + app.escapeHtml(stageStatusText(area)) + "</p>";

    return (
      '<section class="stage" aria-label="' +
      app.escapeHtml(area.display_name) +
      '">' +
      '<div class="stage-frame">' +
      frame +
      "</div>" +
      '<div class="stage-notes">' +
      "<div>" +
      '<h2 class="stage-name">' +
      app.escapeHtml(area.display_name) +
      "</h2>" +
      '<p class="stage-code">' +
      app.escapeHtml(area.code_label) +
      "</p>" +
      "</div>" +
      '<p class="stage-status" data-status="' +
      app.escapeHtml(area.status || "") +
      '" >' +
      app.escapeHtml(stageStatusText(area)) +
      "</p>" +
      (area.pending ? "" : renderLegend() + renderMarkerEditor(area) + renderStageActions(area, previewUrl)) +
      "</div>" +
      "</section>"
    );
  }

  function stageStatusText(area) {
    var status = String(area.status || "queued").toLowerCase();
    var slotDate = area.latest_source_timestamp
      ? app.parseSlotCaptureDate(area.latest_source_timestamp + "_")
      : null;

    if (area.last_error) {
      return area.last_error;
    }
    if (status === "ready" && slotDate) {
      return "Último quadro: " + app.formatCaptureDate(slotDate) + ".";
    }
    if (status === "processing") {
      return "Renderizando a animação.";
    }
    if (status === "ready") {
      return "Animação pronta.";
    }
    return app.queuedHelperText();
  }

  function renderLegend() {
    return (
      '<ul class="legend" aria-label="Legenda da animação">' +
      '<li><span class="legend-mark" data-mark="contour"></span>Contorno do município</li>' +
      '<li><span class="legend-mark" style="--mark: var(--lightning)"></span>Raios nos últimos 10 minutos</li>' +
      '<li><span class="legend-mark" style="--mark: var(--marker)"></span>Seu ponto marcado</li>' +
      "</ul>"
    );
  }

  function renderStageActions(area, previewUrl) {
    return (
      '<div class="stage-actions">' +
      (previewUrl
        ? '<a href="' +
          app.escapeHtml(previewUrl) +
          '" target="_blank" rel="noreferrer">Abrir em tamanho real</a>'
        : "") +
      '<button class="link-button" type="button" data-action="remove" data-area-id="' +
      app.escapeHtml(area.area_id) +
      '" data-display-name="' +
      app.escapeHtml(area.display_name) +
      '">Remover município</button>' +
      "</div>"
    );
  }

  function renderRail(items, selected) {
    var markup = '<div class="rail" aria-label="Municípios acompanhados">';
    var index;

    for (index = 0; index < items.length; index += 1) {
      markup += renderRailItem(items[index], items[index].area_id === selected.area_id);
    }

    return markup + "</div>";
  }

  function renderRailItem(area, isSelected) {
    var previewUrl = area.media_exists ? app.buildPreviewUrl(area) : "";

    return (
      '<button class="rail-item" type="button" data-action="select" data-area-id="' +
      app.escapeHtml(area.area_id) +
      '" aria-pressed="' +
      (isSelected ? "true" : "false") +
      '">' +
      '<span class="rail-frame" data-status="' +
      app.escapeHtml(area.status || "") +
      '">' +
      (previewUrl
        ? '<img src="' + app.escapeHtml(previewUrl) + '" alt="" />'
        : app.escapeHtml(app.statusLabel(area.status || "queued"))) +
      "</span>" +
      '<span class="rail-name">' +
      app.escapeHtml(area.display_name) +
      "</span>" +
      "</button>"
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
    var inputId = "marker-" + app.escapeHtml(area.area_id);

    return (
      '<section class="marker-editor">' +
      '<label for="' +
      inputId +
      '">Marcar um ponto</label>' +
      '<div class="marker-row">' +
      '<input id="' +
      inputId +
      '" type="text" inputmode="decimal" data-marker-field="coordinates" data-area-id="' +
      app.escapeHtml(area.area_id) +
      '" value="' +
      app.escapeHtml(fieldValue) +
      '" placeholder="-20.3297, -40.2925"' +
      (errorMessage ? ' aria-invalid="true"' : "") +
      " />" +
      '<button class="button button-primary" type="button" data-action="save-marker" data-area-id="' +
      app.escapeHtml(area.area_id) +
      '">' +
      (hasMarker ? "Atualizar ponto" : "Salvar ponto") +
      "</button>" +
      (hasMarker
        ? '<button class="button" type="button" data-action="remove-marker" data-area-id="' +
          app.escapeHtml(area.area_id) +
          '">Remover ponto</button>'
        : "") +
      "</div>" +
      (errorMessage
        ? '<p class="marker-error">' + app.escapeHtml(errorMessage) + "</p>"
        : '<p class="marker-hint">Latitude e longitude, dentro da área da imagem.</p>') +
      "</section>"
    );
  }

  function renderDownloads() {
    var sources = app.state.downloads;

    if (!sources.length) {
      app.elements.downloadsList.innerHTML =
        '<p class="muted">Nenhuma fonte de dados disponível.</p>';
      return;
    }

    app.elements.downloadsList.innerHTML =
      renderSourcesTable(sources) + renderActiveReads(sources) + renderAreaFiles(sources);
  }

  function renderSourcesTable(sources) {
    var rows = sources
      .map(function (source) {
        return (
          "<tr>" +
          '<th scope="row">' +
          app.escapeHtml(source.source_label || "Fonte") +
          "</th>" +
          "<td>" +
          app.escapeHtml(source.phase_label || app.statusLabel(source.phase || "idle")) +
          '<span class="source-summary">' +
          app.escapeHtml(source.summary || "") +
          "</span>" +
          "</td>" +
          '<td class="number">' +
          app.escapeHtml(String(source.file_count || 0)) +
          "</td>" +
          '<td class="number">' +
          app.escapeHtml(app.formatBytes(source.disk_usage_bytes || 0)) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");

    return (
      '<table class="sources-table">' +
      "<caption>Fontes da NOAA</caption>" +
      '<thead><tr><th scope="col">Fonte</th><th scope="col">Situação</th>' +
      '<th scope="col" class="number">Arquivos</th><th scope="col" class="number">Espaço</th></tr></thead>' +
      "<tbody>" +
      rows +
      "</tbody></table>"
    );
  }

  function areaNameFromFilename(filename) {
    var separator = String(filename || "").indexOf("/");
    var areaId = separator === -1 ? "" : filename.slice(0, separator);
    var index;

    for (index = 0; index < app.state.tracked.length; index += 1) {
      if (app.state.tracked[index].area_id === areaId) {
        return app.state.tracked[index].display_name;
      }
    }

    return areaId;
  }

  function renderActiveReads(sources) {
    var markup = "";

    sources.forEach(function (source) {
      (source.active_downloads || []).forEach(function (item) {
        markup +=
          "<li>" +
          '<div class="active-read-head"><span>' +
          app.escapeHtml(areaNameFromFilename(item.filename) || source.source_label) +
          "</span><span>" +
          app.escapeHtml(app.describeRawFile(item.filename).primary) +
          "</span></div>" +
          '<div class="progress"><span></span></div>' +
          "</li>";
      });
    });

    return markup
      ? '<div><h3>Lendo agora</h3><ul class="active-reads">' + markup + "</ul></div>"
      : "";
  }

  function renderAreaFiles(sources) {
    var totals = {};
    var names = [];

    sources.forEach(function (source) {
      var sourceLabel = SOURCE_LABELS[source.source_key];

      if (!sourceLabel) {
        return;
      }

      (source.files_on_disk || []).forEach(function (file) {
        var name = areaNameFromFilename(file.filename) || "Versões anteriores";
        var total = totals[name] || (totals[name] = { count: 0, bytes: 0 });

        if (!total.count) {
          names.push(name);
        }
        total.count += 1;
        total.bytes += file.size_bytes || 0;
      });
    });

    if (!names.length) {
      return "";
    }

    return (
      '<div><h3>Imagens por município</h3><ul class="area-files">' +
      names
        .sort()
        .map(function (name) {
          return (
            '<li class="area-file"><span>' +
            app.escapeHtml(name) +
            "</span><span>" +
            app.escapeHtml(
              totals[name].count +
                (totals[name].count === 1 ? " arquivo, " : " arquivos, ") +
                app.formatBytes(totals[name].bytes)
            ) +
            "</span></li>"
          );
        })
        .join("") +
      "</ul></div>"
    );
  }
})(window);
