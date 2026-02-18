(function attachExportService(global) {
  function toTitleCase(value) {
    return String(value || "")
      .replace(/_/g, " ")
      .replace(/([a-z])([A-Z])/g, "$1 $2")
      .replace(/\s+/g, " ")
      .trim()
      .replace(/\b\w/g, function (char) {
        return char.toUpperCase();
      });
  }

  function toIsoDate(value) {
    if (!value) return "";
    var date = new Date(String(value).includes("T") ? value : value + "T00:00:00");
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toISOString().slice(0, 10);
  }

  function toFriendlyDate(value) {
    if (!value) return "";
    var date = new Date(String(value).includes("T") ? value : value + "T00:00:00");
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
  }

  function safeNumber(value) {
    var parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function normalizeStatus(status) {
    var normalized = String(status || "Planned").toLowerCase();
    if (normalized === "completed") return "Completed";
    if (normalized === "cancelled") return "Cancelled";
    return "Planned";
  }

  function buildVisitDetailRows(rows) {
    return rows.map(function (row) {
      return {
        event_date: toIsoDate(row.event_date),
        customer_name: row.customer_name || "",
        customer_code: row.cust_code || "",
        territory: row.territory || "",
        status: normalizeStatus(row.status),
      };
    });
  }

  function buildExecutiveSummaryRows(rows) {
    var buckets = {};
    rows.forEach(function (row) {
      var territory = row.territory || "Unassigned";
      if (!buckets[territory]) {
        buckets[territory] = {
          territory: territory,
          total_visits: 0,
          planned_visits: 0,
          completed_visits: 0,
          cancelled_visits: 0,
          completion_rate: "0%",
        };
      }

      var bucket = buckets[territory];
      var status = normalizeStatus(row.status);
      bucket.total_visits += 1;
      if (status === "Completed") bucket.completed_visits += 1;
      else if (status === "Cancelled") bucket.cancelled_visits += 1;
      else bucket.planned_visits += 1;
    });

    return Object.keys(buckets)
      .sort(function (a, b) {
        return a.localeCompare(b);
      })
      .map(function (key) {
        var bucket = buckets[key];
        var rate = bucket.total_visits > 0 ? Math.round((bucket.completed_visits / bucket.total_visits) * 100) : 0;
        bucket.completion_rate = rate + "%";
        return bucket;
      });
  }

  function buildMonthlySummaryRows(rows) {
    var buckets = {};

    rows.forEach(function (row) {
      var date = new Date(String(row.event_date || "").includes("T") ? row.event_date : row.event_date + "T00:00:00");
      if (Number.isNaN(date.getTime())) return;
      var key = date.getFullYear() + "-" + String(date.getMonth() + 1).padStart(2, "0");
      var monthLabel = date.toLocaleDateString(undefined, { year: "numeric", month: "short" });

      if (!buckets[key]) {
        buckets[key] = {
          month: monthLabel,
          total_visits: 0,
          planned_visits: 0,
          completed_visits: 0,
          cancelled_visits: 0,
        };
      }

      var bucket = buckets[key];
      var status = normalizeStatus(row.status);
      bucket.total_visits += 1;
      if (status === "Completed") bucket.completed_visits += 1;
      else if (status === "Cancelled") bucket.cancelled_visits += 1;
      else bucket.planned_visits += 1;
    });

    return Object.keys(buckets)
      .sort()
      .map(function (key) {
        return buckets[key];
      });
  }

  function buildCustomerPerformanceRows(rows) {
    var buckets = {};

    rows.forEach(function (row) {
      var key = (row.cust_code || "-") + "::" + (row.customer_name || "Unknown");
      if (!buckets[key]) {
        buckets[key] = {
          customer_code: row.cust_code || "",
          customer_name: row.customer_name || "",
          territory: row.territory || "",
          total_visits: 0,
          planned_visits: 0,
          completed_visits: 0,
          cancelled_visits: 0,
          completion_rate: "0%",
          last_visit_date: "",
        };
      }

      var bucket = buckets[key];
      var status = normalizeStatus(row.status);
      var date = toIsoDate(row.event_date);

      bucket.total_visits += 1;
      if (status === "Completed") bucket.completed_visits += 1;
      else if (status === "Cancelled") bucket.cancelled_visits += 1;
      else bucket.planned_visits += 1;

      if (date && (!bucket.last_visit_date || date > bucket.last_visit_date)) {
        bucket.last_visit_date = date;
      }
    });

    return Object.keys(buckets)
      .sort(function (a, b) {
        return buckets[a].customer_name.localeCompare(buckets[b].customer_name);
      })
      .map(function (key) {
        var bucket = buckets[key];
        var rate = bucket.total_visits > 0 ? Math.round((bucket.completed_visits / bucket.total_visits) * 100) : 0;
        bucket.completion_rate = rate + "%";
        bucket.last_visit_date = bucket.last_visit_date || "";
        return bucket;
      });
  }

  var REPORT_DEFS = {
    executive_summary: {
      label: "Executive Summary",
      defaultColumns: [
        "territory",
        "total_visits",
        "planned_visits",
        "completed_visits",
        "cancelled_visits",
        "completion_rate",
      ],
      builder: buildExecutiveSummaryRows,
    },
    visit_detail: {
      label: "Visit Detail Report",
      defaultColumns: ["event_date", "customer_name", "customer_code", "territory", "status"],
      builder: buildVisitDetailRows,
    },
    monthly_summary: {
      label: "Monthly Summary",
      defaultColumns: ["month", "total_visits", "planned_visits", "completed_visits", "cancelled_visits"],
      builder: buildMonthlySummaryRows,
    },
    customer_performance: {
      label: "Customer Performance",
      defaultColumns: [
        "customer_code",
        "customer_name",
        "territory",
        "total_visits",
        "planned_visits",
        "completed_visits",
        "completion_rate",
        "last_visit_date",
      ],
      builder: buildCustomerPerformanceRows,
    },
  };

  function getReportLabel(reportType) {
    var report = REPORT_DEFS[reportType] || REPORT_DEFS.visit_detail;
    return report.label;
  }

  function buildReportRows(reportType, rows) {
    var report = REPORT_DEFS[reportType] || REPORT_DEFS.visit_detail;
    return report.builder(rows || []);
  }

  function getAvailableColumns(rows) {
    var columnSet = {};
    (rows || []).forEach(function (row) {
      Object.keys(row || {}).forEach(function (key) {
        columnSet[key] = true;
      });
    });
    return Object.keys(columnSet);
  }

  function getDefaultColumns(reportType, availableColumns) {
    var report = REPORT_DEFS[reportType] || REPORT_DEFS.visit_detail;
    var defaults = report.defaultColumns || [];
    var available = availableColumns || [];
    var filtered = defaults.filter(function (column) {
      return available.includes(column);
    });
    return filtered.length ? filtered : available;
  }

  function getSummaryMetrics(rows) {
    var summary = {
      total: 0,
      planned: 0,
      completed: 0,
      cancelled: 0,
    };

    (rows || []).forEach(function (row) {
      var status = normalizeStatus(row.status);
      summary.total += 1;
      if (status === "Completed") summary.completed += 1;
      else if (status === "Cancelled") summary.cancelled += 1;
      else summary.planned += 1;
    });

    return summary;
  }

  function createFileName(reportType) {
    var stamp = new Date().toISOString().slice(0, 10);
    return "planner-" + String(reportType || "report").replace(/_/g, "-") + "-" + stamp;
  }

  function encodeCsvValue(value) {
    var plain = value == null ? "" : String(value);
    if (plain.includes('"')) {
      plain = plain.replace(/"/g, '""');
    }
    if (plain.includes(",") || plain.includes("\n") || plain.includes('"')) {
      return '"' + plain + '"';
    }
    return plain;
  }

  function downloadBlob(content, mimeType, fileName) {
    var blob = new Blob([content], { type: mimeType });
    var url = URL.createObjectURL(blob);
    var anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = fileName;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(function () {
      URL.revokeObjectURL(url);
    }, 0);
  }

  function exportCsv(options) {
    var lines = [];

    if (options.includeSummary) {
      lines.push(encodeCsvValue(options.title));
      lines.push(["Date Range", options.dateRangeLabel].map(encodeCsvValue).join(","));
      lines.push(["Generated", options.generatedLabel].map(encodeCsvValue).join(","));
      lines.push(["Total Records", options.summary.total].join(","));
      lines.push(["Planned", options.summary.planned].join(","));
      lines.push(["Completed", options.summary.completed].join(","));
      lines.push(["Cancelled", options.summary.cancelled].join(","));
      lines.push("");
    }

    lines.push(
      options.columns
        .map(function (column) {
          return encodeCsvValue(toTitleCase(column));
        })
        .join(",")
    );

    options.rows.forEach(function (row) {
      lines.push(
        options.columns
          .map(function (column) {
            return encodeCsvValue(row[column]);
          })
          .join(",")
      );
    });

    downloadBlob(lines.join("\n"), "text/csv;charset=utf-8", options.fileName + ".csv");
  }

  function exportXlsx(options) {
    if (!global.XLSX) {
      throw new Error("XLSX library is not available.");
    }

    var workbook = global.XLSX.utils.book_new();

    var tableRows = options.rows.map(function (row) {
      var exportRow = {};
      options.columns.forEach(function (column) {
        exportRow[toTitleCase(column)] = row[column] == null ? "" : row[column];
      });
      return exportRow;
    });

    var tableSheet = global.XLSX.utils.json_to_sheet(tableRows);
    global.XLSX.utils.book_append_sheet(workbook, tableSheet, "Report");

    if (options.includeSummary) {
      var summaryData = [
        ["Report", options.title],
        ["Date Range", options.dateRangeLabel],
        ["Generated", options.generatedLabel],
        ["Total Records", options.summary.total],
        ["Planned", options.summary.planned],
        ["Completed", options.summary.completed],
        ["Cancelled", options.summary.cancelled],
      ];
      var summarySheet = global.XLSX.utils.aoa_to_sheet(summaryData);
      global.XLSX.utils.book_append_sheet(workbook, summarySheet, "Summary");
    }

    global.XLSX.writeFile(workbook, options.fileName + ".xlsx");
  }

  function exportPdf(options) {
    if (!(global.jspdf && global.jspdf.jsPDF)) {
      throw new Error("PDF library is not available.");
    }

    var doc = new global.jspdf.jsPDF({ orientation: "landscape", unit: "pt", format: "a4" });
    var startX = 36;
    var y = 40;

    if (options.includeLogo) {
      doc.setFillColor(11, 99, 206);
      doc.roundedRect(startX, 20, 28, 28, 6, 6, "F");
      doc.setTextColor(255, 255, 255);
      doc.setFontSize(13);
      doc.text("P", startX + 14, 39, { align: "center", baseline: "middle" });
      startX += 40;
    }

    doc.setTextColor(15, 23, 42);
    doc.setFontSize(16);
    doc.text(options.title, startX, y);

    y += 18;
    doc.setTextColor(71, 85, 105);
    doc.setFontSize(10);
    doc.text("Date Range: " + options.dateRangeLabel, 36, y);

    y += 14;
    doc.text("Generated: " + options.generatedLabel, 36, y);

    if (options.includeSummary) {
      y += 22;
      doc.setTextColor(15, 23, 42);
      doc.setFontSize(11);
      doc.text("Summary", 36, y);
      y += 16;
      doc.setTextColor(51, 65, 85);
      doc.setFontSize(10);
      doc.text(
        [
          "Total " + options.summary.total,
          "Planned " + options.summary.planned,
          "Completed " + options.summary.completed,
          "Cancelled " + options.summary.cancelled,
        ].join("   |   "),
        36,
        y
      );
    }

    if (typeof doc.autoTable !== "function") {
      throw new Error("PDF table plugin is not available.");
    }

    var head = [
      options.columns.map(function (column) {
        return toTitleCase(column);
      }),
    ];

    var body = options.rows.map(function (row) {
      return options.columns.map(function (column) {
        return row[column] == null ? "" : String(row[column]);
      });
    });

    doc.autoTable({
      startY: options.includeSummary ? y + 14 : y + 14,
      head: head,
      body: body,
      margin: { left: 36, right: 36 },
      styles: {
        fontSize: 8.8,
        cellPadding: 4,
        lineColor: [225, 232, 240],
      },
      headStyles: {
        fillColor: [11, 99, 206],
        textColor: [255, 255, 255],
        fontStyle: "bold",
      },
      alternateRowStyles: {
        fillColor: [247, 250, 255],
      },
      theme: "grid",
      didDrawPage: function () {
        var pageSize = doc.internal.pageSize;
        var pageWidth = pageSize.getWidth ? pageSize.getWidth() : pageSize.width;
        var pageHeight = pageSize.getHeight ? pageSize.getHeight() : pageSize.height;
        doc.setFontSize(8.5);
        doc.setTextColor(148, 163, 184);
        doc.text("Planner Dashboard Report", 36, pageHeight - 18);
        doc.text("Page " + doc.internal.getNumberOfPages(), pageWidth - 80, pageHeight - 18);
      },
    });

    doc.save(options.fileName + ".pdf");
  }

  function exportReport(options) {
    var reportType = options.reportType || "visit_detail";
    var format = String(options.format || "csv").toLowerCase();
    var sourceRows = options.rows || [];
    var reportRows = buildReportRows(reportType, sourceRows);
    var availableColumns = getAvailableColumns(reportRows);
    var selectedColumns = (options.selectedColumns || []).filter(function (column) {
      return availableColumns.includes(column);
    });
    var columns = selectedColumns.length ? selectedColumns : getDefaultColumns(reportType, availableColumns);
    var summary = getSummaryMetrics(sourceRows);
    var reportLabel = getReportLabel(reportType);

    var payload = {
      title: reportLabel,
      rows: reportRows,
      columns: columns,
      summary: summary,
      fileName: createFileName(reportType),
      includeSummary: Boolean(options.includeSummary),
      includeLogo: Boolean(options.includeLogo),
      dateRangeLabel: options.dateRangeLabel || "Current Filter",
      generatedLabel: options.generatedLabel || toFriendlyDate(new Date().toISOString()),
    };

    if (!payload.rows.length) {
      throw new Error("No records match the current export settings.");
    }

    if (!payload.columns.length) {
      throw new Error("Select at least one column to export.");
    }

    if (format === "xlsx") {
      exportXlsx(payload);
      return payload;
    }

    if (format === "pdf") {
      exportPdf(payload);
      return payload;
    }

    exportCsv(payload);
    return payload;
  }

  global.ExportService = {
    reportDefs: REPORT_DEFS,
    getReportLabel: getReportLabel,
    buildReportRows: buildReportRows,
    getAvailableColumns: getAvailableColumns,
    getDefaultColumns: getDefaultColumns,
    getSummaryMetrics: getSummaryMetrics,
    toTitleCase: toTitleCase,
    exportReport: exportReport,
  };
})(window);
