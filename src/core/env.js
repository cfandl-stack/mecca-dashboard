const fs = require("node:fs");
const path = require("node:path");

function parseDotEnv(content) {
  const entries = {};

  for (const rawLine of String(content || "").split(/\r?\n/)) {
    const line = rawLine.trim();

    if (!line || line.startsWith("#")) {
      continue;
    }

    const separatorIndex = line.indexOf("=");
    if (separatorIndex < 1) {
      continue;
    }

    const key = line.slice(0, separatorIndex).trim();
    let value = line.slice(separatorIndex + 1).trim();

    if (
      (value.startsWith("\"") && value.endsWith("\"")) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    entries[key] = value;
  }

  return entries;
}

function loadEnvironmentFiles(options = {}) {
  const cwd = path.resolve(options.cwd || process.cwd());
  const files = Array.isArray(options.files) && options.files.length > 0
    ? options.files
    : [".env.local", ".env"];
  const loadedFiles = [];

  for (const relativeFile of files) {
    const filePath = path.resolve(cwd, relativeFile);

    if (!fs.existsSync(filePath)) {
      continue;
    }

    const content = fs.readFileSync(filePath, "utf8");
    const parsed = parseDotEnv(content);

    for (const [key, value] of Object.entries(parsed)) {
      if (!(key in process.env)) {
        process.env[key] = value;
      }
    }

    loadedFiles.push(filePath);
  }

  return loadedFiles;
}

module.exports = {
  loadEnvironmentFiles,
  parseDotEnv
};
