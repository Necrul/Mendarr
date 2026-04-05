(function () {
  var root = document.documentElement;
  var toggle = document.getElementById("themeToggle");
  var pulse = document.getElementById("scanPulse");
  var pollTimer = null;
  var ACTIVE_SCAN_POLL_MS = 2500;
  var IDLE_SCAN_POLL_MS = 30000;
  var HIDDEN_SCAN_POLL_MS = 60000;

  function setTheme(value) {
    root.setAttribute("data-theme", value);
    if (toggle) {
      toggle.textContent = value === "light" ? "Switch to dark" : "Switch to light";
    }
  }

  var savedTheme = localStorage.getItem("mendarr-theme") || "light";
  setTheme(savedTheme);

  if (toggle) {
    toggle.addEventListener("click", function () {
      var nextTheme = root.getAttribute("data-theme") === "light" ? "dark" : "light";
      setTheme(nextTheme);
      localStorage.setItem("mendarr-theme", nextTheme);
    });
  }

  function basename(path) {
    if (!path) {
      return "";
    }
    var cleaned = String(path).replace(/[\\/]+$/, "");
    var parts = cleaned.split(/[\\/]/);
    return parts[parts.length - 1] || cleaned;
  }

  function showFlashMessage(kind, text) {
    var stack = document.querySelector(".flash-stack");
    if (!stack) {
      return;
    }

    var existing = stack.querySelector("[data-scan-start-flash]");
    if (existing) {
      existing.remove();
    }

    var message = document.createElement("p");
    message.className = kind === "error" ? "flash error" : "flash ok";
    message.setAttribute("data-scan-start-flash", "true");
    message.textContent = text;
    stack.prepend(message);
  }

  function optimisticScanState(scanId) {
    return {
      id: scanId,
      status: "running",
      scope: "library",
      stop_requested: false,
      files_seen: 0,
      suspicious_found: 0,
      notes: {
        scope: "library",
        total_files: 0,
      },
      progress_percent: 0,
    };
  }

  function renderStopScanForms(scan) {
    var forms = document.querySelectorAll("[data-scan-stop-form]");
    forms.forEach(function (form) {
      var button = form.querySelector("[data-scan-stop-button]");
      var isRunningLibraryScan = !!(scan && scan.status === "running" && (scan.scope || (scan.notes || {}).scope || "library") !== "verify");
      if (!isRunningLibraryScan) {
        form.classList.add("hidden");
        if (button) {
          button.disabled = false;
          button.textContent = "Stop library scan";
        }
        return;
      }

      form.classList.remove("hidden");
      if (button) {
        button.disabled = !!scan.stop_requested;
        button.textContent = scan.stop_requested ? "Stopping..." : "Stop library scan";
      }
    });
  }

  function renderResumeScanForms(scan) {
    var forms = document.querySelectorAll("[data-scan-resume-form]");
    forms.forEach(function (form) {
      var notes = (scan && scan.notes) || {};
      var isResumableLibraryScan = !!(
        scan &&
        scan.status === "interrupted" &&
        (scan.scope || notes.scope || "library") !== "verify" &&
        notes.resume_after_file
      );

      if (isResumableLibraryScan) {
        form.classList.remove("hidden");
      } else {
        form.classList.add("hidden");
      }
    });
  }

  function renderLiveScanCards(scan) {
    var cards = document.querySelectorAll("[data-live-scan-card]");
    cards.forEach(function (card) {
      if (!scan || scan.status !== "running") {
        card.classList.add("hidden");
        renderStopScanForms(scan);
        renderResumeScanForms(scan);
        return;
      }

      card.classList.remove("hidden");
      var label = card.querySelector("[data-live-scan-label]");
      var counts = card.querySelector("[data-live-scan-counts]");
      var fill = card.querySelector("[data-live-scan-fill]");
      var meta = card.querySelector("[data-live-scan-meta]");
      var notes = scan.notes || {};
      var percent = scan.progress_percent || 0;
      var scope = notes.scope || "library";
      var currentFile = basename(notes.current_file || "");
      var currentLibrary = basename(notes.current_library || "");

      if (label) {
        if (scope === "verify") {
          label.textContent = "Verifying flagged files";
        } else {
          label.textContent = currentLibrary ? "Scanning " + currentLibrary : "Scan is running";
        }
      }
      if (counts) {
        var totalFiles = notes.total_files || 0;
        counts.textContent = scan.files_seen + " / " + totalFiles + " files";
      }
      if (fill) {
        fill.style.width = percent + "%";
      }
      if (meta) {
        var details = [];
        if (scope === "verify" && notes.target_count) {
          details.push(notes.target_count + " finding(s) selected");
        }
        if (currentFile) {
          details.push("Current file: " + currentFile);
        }
        details.push(scan.suspicious_found + " findings so far");
        meta.textContent = details.join("  |  ");
      }
      renderStopScanForms(scan);
      renderResumeScanForms(scan);
    });
  }

  function renderLiveScanStats(scan) {
    var filesValue = document.querySelector("[data-live-scan-files]");
    var findingsValue = document.querySelector("[data-live-scan-findings]");
    var statusValue = document.querySelector("[data-live-scan-status]");

    if (!filesValue || !findingsValue || !statusValue || !scan) {
      return;
    }

    filesValue.textContent = scan.files_seen || 0;
    findingsValue.textContent = scan.suspicious_found || 0;

    if (scan.status === "running") {
      statusValue.textContent = "Running";
    } else if (scan.status === "completed") {
      statusValue.textContent = "Completed";
    } else if (scan.status === "failed") {
      statusValue.textContent = "Failed";
    } else {
      statusValue.textContent = scan.status || "Open";
    }
  }

  function renderScanPulse(scan) {
    if (!pulse) {
      renderStopScanForms(scan);
      renderResumeScanForms(scan);
      return;
    }

    if (!scan || scan.status !== "running") {
      pulse.classList.add("hidden");
      renderLiveScanCards(scan);
      renderStopScanForms(scan);
      renderResumeScanForms(scan);
      return;
    }

    pulse.classList.remove("hidden");

    var notes = scan.notes || {};
    var currentLibrary = basename(notes.current_library || "");
    var currentFile = basename(notes.current_file || "");
    var totalFiles = notes.total_files || 0;
    var percent = scan.progress_percent || 0;
    var scope = notes.scope || "library";

    var title = document.getElementById("scanPulseTitle");
    var meta = document.getElementById("scanPulseMeta");
    var fill = document.getElementById("scanPulseFill");
    var counts = document.getElementById("scanPulseCounts");
    var findings = document.getElementById("scanPulseFindings");

    if (title) {
      title.textContent = scope === "verify"
        ? "Verifying flagged files"
        : currentLibrary ? "Scanning " + currentLibrary : "Scanning libraries";
    }
    if (meta) {
      if (scope === "verify") {
        meta.textContent = currentFile
          ? "Current file: " + currentFile
          : "Checking only the findings that still need verification.";
      } else {
        meta.textContent = currentFile
          ? "Current file: " + currentFile
          : "Mendarr is working through the current library.";
      }
    }
    if (fill) {
      fill.style.width = percent + "%";
    }
    if (counts) {
      counts.textContent = scan.files_seen + " / " + totalFiles + " files";
    }
    if (findings) {
      findings.textContent = scan.suspicious_found + " findings so far";
    }

    renderLiveScanStats(scan);
    renderLiveScanCards(scan);
    renderStopScanForms(scan);
    renderResumeScanForms(scan);
  }

  async function refreshScanStatus() {
    if (document.visibilityState === "hidden") {
      scheduleNextPoll(HIDDEN_SCAN_POLL_MS);
      return;
    }

    try {
      var response = await fetch("/api/scans/latest", {
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      if (!response.ok) {
        scheduleNextPoll(IDLE_SCAN_POLL_MS);
        return;
      }
      var payload = await response.json();
      var scan = payload.scan || null;
      renderScanPulse(scan);
      scheduleNextPoll(scan && scan.status === "running" ? ACTIVE_SCAN_POLL_MS : IDLE_SCAN_POLL_MS);
    } catch (_error) {
      scheduleNextPoll(IDLE_SCAN_POLL_MS);
    }
  }

  async function startScanInPlace(form) {
    var submit = form.querySelector('button[type="submit"]');
    var originalLabel = submit ? submit.textContent : "";

    if (submit) {
      submit.disabled = true;
      submit.textContent = "Starting...";
    }

    try {
      var response = await fetch(form.action, {
        method: "POST",
        body: new FormData(form),
        headers: { Accept: "application/json" },
      });
      var payload = await response.json().catch(function () {
        return {};
      });

      if (response.ok && payload.started && payload.scan && payload.scan.id) {
        showFlashMessage(
          "ok",
          payload.resumed
            ? "Scan resumed from the last interrupted checkpoint."
            : "Scan started. Activity will update as Mendarr works through the libraries."
        );
        renderScanPulse(optimisticScanState(payload.scan.id));
        refreshScanStatus();
        return;
      }

      if (response.status === 409 && payload.reason === "already_running") {
        showFlashMessage("error", "A scan is already running.");
        refreshScanStatus();
        return;
      }

      if (response.status === 409 && payload.reason === "not_resumable") {
        showFlashMessage("error", "No interrupted library scan is available to resume.");
        refreshScanStatus();
        return;
      }

      throw new Error("unexpected scan start response");
    } catch (_error) {
      if (submit) {
        submit.disabled = false;
        submit.textContent = originalLabel;
      }
      form.submit();
      return;
    }

    if (submit) {
      submit.disabled = false;
      submit.textContent = originalLabel;
    }
  }

  async function stopScanInPlace(form) {
    var submit = form.querySelector('button[type="submit"]');
    var originalLabel = submit ? submit.textContent : "";

    if (submit) {
      submit.disabled = true;
      submit.textContent = "Stopping...";
    }

    try {
      var response = await fetch(form.action, {
        method: "POST",
        body: new FormData(form),
        headers: { Accept: "application/json" },
      });
      var payload = await response.json().catch(function () {
        return {};
      });

      if (response.ok && payload.stopped) {
        showFlashMessage("ok", "Stop requested. Mendarr will finish the current file, then interrupt the library scan.");
        refreshScanStatus();
        return;
      }

      if (response.status === 409 && payload.reason === "not_running") {
        showFlashMessage("error", "No library scan is running right now.");
        renderStopScanForms(null);
        return;
      }

      throw new Error("unexpected scan stop response");
    } catch (_error) {
      if (submit) {
        submit.disabled = false;
        submit.textContent = originalLabel;
      }
      form.submit();
      return;
    }
  }

  function scheduleNextPoll(delay) {
    if (pollTimer) {
      window.clearTimeout(pollTimer);
    }
    pollTimer = window.setTimeout(refreshScanStatus, delay);
  }

  function wireFindingsSelection() {
    var selectAll = document.querySelector("[data-findings-select-all]");
    if (!selectAll) {
      return;
    }

    var findingCheckboxes = Array.prototype.slice.call(
      document.querySelectorAll("[data-finding-select]")
    );

    function syncSelectAllState() {
      if (!findingCheckboxes.length) {
        selectAll.checked = false;
        selectAll.indeterminate = false;
        return;
      }
      var selectedCount = findingCheckboxes.filter(function (checkbox) {
        return checkbox.checked;
      }).length;
      selectAll.checked = selectedCount === findingCheckboxes.length;
      selectAll.indeterminate = selectedCount > 0 && selectedCount < findingCheckboxes.length;
    }

    selectAll.addEventListener("change", function () {
      findingCheckboxes.forEach(function (checkbox) {
        checkbox.checked = selectAll.checked;
      });
      selectAll.indeterminate = false;
    });

    findingCheckboxes.forEach(function (checkbox) {
      checkbox.addEventListener("change", syncSelectAllState);
    });

    syncSelectAllState();
  }

  function wireChangeActions() {
    document.querySelectorAll("[data-submit-on-change]").forEach(function (element) {
      element.addEventListener("change", function () {
        var form = element.closest("form");
        if (form) {
          form.submit();
        }
      });
    });

    document.querySelectorAll("[data-navigate-on-change]").forEach(function (element) {
      element.addEventListener("change", function () {
        if (element.value) {
          window.location.assign(element.value);
        }
      });
    });
  }

  function wireConfirmForms() {
    document.querySelectorAll("form[data-confirm]").forEach(function (form) {
      form.addEventListener("submit", function (event) {
        var message = form.getAttribute("data-confirm");
        if (!message) {
          return;
        }
        if (!window.confirm(message)) {
          event.preventDefault();
        }
      });
    });
  }

  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "visible") {
      refreshScanStatus();
      return;
    }
    scheduleNextPoll(HIDDEN_SCAN_POLL_MS);
  });

  document.querySelectorAll('form[action="/scan/start"]').forEach(function (form) {
    form.addEventListener("submit", function (event) {
      event.preventDefault();
      startScanInPlace(form);
    });
  });

  document.querySelectorAll('form[action="/scan/stop"]').forEach(function (form) {
    form.addEventListener("submit", function (event) {
      event.preventDefault();
      stopScanInPlace(form);
    });
  });

  wireFindingsSelection();
  wireChangeActions();
  wireConfirmForms();
  refreshScanStatus();
})();
