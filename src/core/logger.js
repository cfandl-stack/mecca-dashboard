/**
 * Sehr leichter Logger mit Zeitstempel.
 * Für dieses Projekt reicht stdout/stderr vollkommen aus.
 */
function createLogger() {
  function format(level, message, context) {
    const timestamp = new Date().toISOString();
    const suffix = context ? ` ${JSON.stringify(context)}` : "";
    return `[${timestamp}] [${level}] ${message}${suffix}`;
  }

  return {
    info(message, context) {
      console.log(format("INFO", message, context));
    },
    warn(message, context) {
      console.warn(format("WARN", message, context));
    },
    error(message, context) {
      console.error(format("ERROR", message, context));
    }
  };
}

module.exports = {
  createLogger
};
