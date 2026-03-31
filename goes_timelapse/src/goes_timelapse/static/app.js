(function (window, document) {
  "use strict";

  var app = window.GoesTimelapseApp;

  if (!app || typeof app.initialize !== "function") {
    return;
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", app.initialize);
    return;
  }

  app.initialize();
})(window, document);
