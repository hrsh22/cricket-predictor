import winston from "winston";

import { loadAppConfig } from "./config/index.js";

const config = loadAppConfig();

const logger = winston.createLogger({
  level: config.logLevel,
  format: winston.format.combine(
    winston.format.colorize(),
    winston.format.timestamp(),
    winston.format.simple(),
  ),
  defaultMeta: { service: "cric-predictor-node" },
  transports: [new winston.transports.Console()],
  exceptionHandlers: [new winston.transports.Console()],
});

export default logger;
