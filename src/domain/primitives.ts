export type JsonPrimitive = string | number | boolean | null;

export interface JsonObject {
  [key: string]: JsonValue;
}

export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];

export type UnknownRecord = Record<string, unknown>;

export interface ValidationIssue {
  path: string;
  message: string;
}

export class DomainValidationError extends Error {
  constructor(public readonly issues: ValidationIssue[]) {
    super(
      issues.length === 0 ? "Invalid domain payload" : issues.map((issue) => `${issue.path}: ${issue.message}`).join("; "),
    );
    this.name = "DomainValidationError";
  }
}

export function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isPlainJsonObject(value: unknown): value is JsonObject {
  return isRecord(value);
}

export function collectUnknownKeys(record: UnknownRecord, allowedKeys: readonly string[]): ValidationIssue[] {
  const allowed = new Set(allowedKeys);
  return Object.keys(record)
    .filter((key) => !allowed.has(key))
    .map((key) => ({ path: key, message: "unexpected field" }));
}

export function parseRecord(value: unknown, path: string, issues: ValidationIssue[]): UnknownRecord | null {
  if (!isRecord(value)) {
    issues.push({ path, message: "must be a plain object" });
    return null;
  }

  return value;
}

export function parseString(value: unknown, path: string, issues: ValidationIssue[]): string | null {
  if (typeof value !== "string" || value.length === 0) {
    issues.push({ path, message: "must be a non-empty string" });
    return null;
  }

  return value;
}

export function parseNullableString(value: unknown, path: string, issues: ValidationIssue[]): string | null {
  if (value === null) {
    return null;
  }

  return parseString(value, path, issues);
}

export function parseEnumValue<T extends string>(
  value: unknown,
  allowedValues: readonly T[],
  path: string,
  issues: ValidationIssue[],
): T | null {
  if (typeof value !== "string" || !allowedValues.includes(value as T)) {
    issues.push({ path, message: `must be one of: ${allowedValues.join(", ")}` });
    return null;
  }

  return value as T;
}

export function parseNullableEnumValue<T extends string>(
  value: unknown,
  allowedValues: readonly T[],
  path: string,
  issues: ValidationIssue[],
): T | null {
  if (value === null) {
    return null;
  }

  return parseEnumValue(value, allowedValues, path, issues);
}

export function parsePositiveInteger(value: unknown, path: string, issues: ValidationIssue[]): number | null {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    issues.push({ path, message: "must be a positive integer" });
    return null;
  }

  return value;
}

export function parseNonNegativeInteger(value: unknown, path: string, issues: ValidationIssue[]): number | null {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    issues.push({ path, message: "must be a non-negative integer" });
    return null;
  }

  return value;
}

export function parseNullablePositiveInteger(value: unknown, path: string, issues: ValidationIssue[]): number | null {
  if (value === null) {
    return null;
  }

  return parsePositiveInteger(value, path, issues);
}

export function parseNullableNonNegativeInteger(value: unknown, path: string, issues: ValidationIssue[]): number | null {
  if (value === null) {
    return null;
  }

  return parseNonNegativeInteger(value, path, issues);
}

export function parseFiniteNumber(value: unknown, path: string, issues: ValidationIssue[]): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    issues.push({ path, message: "must be a finite number" });
    return null;
  }

  return value;
}

export function parseNullableFiniteNumber(value: unknown, path: string, issues: ValidationIssue[]): number | null {
  if (value === null) {
    return null;
  }

  return parseFiniteNumber(value, path, issues);
}

export function parseProbability(value: unknown, path: string, issues: ValidationIssue[]): number | null {
  const parsed = parseFiniteNumber(value, path, issues);

  if (parsed === null) {
    return null;
  }

  if (parsed < 0 || parsed > 1) {
    issues.push({ path, message: "must be between 0 and 1" });
    return null;
  }

  return parsed;
}

export function parseNullableProbability(value: unknown, path: string, issues: ValidationIssue[]): number | null {
  if (value === null) {
    return null;
  }

  return parseProbability(value, path, issues);
}

export function parseProbabilityRecord(value: unknown, path: string, issues: ValidationIssue[]): Record<string, number> | null {
  const record = parseRecord(value, path, issues);
  if (record === null) {
    return null;
  }

  const parsed: Record<string, number> = {};

  for (const [key, entry] of Object.entries(record)) {
    const probability = parseProbability(entry, `${path}.${key}`, issues);
    if (probability !== null) {
      parsed[key] = probability;
    }
  }

  return parsed;
}

export function parseTimestamptzString(value: unknown, path: string, issues: ValidationIssue[]): string | null {
  const parsed = parseString(value, path, issues);
  if (parsed === null) {
    return null;
  }

  if (Number.isNaN(Date.parse(parsed))) {
    issues.push({ path, message: "must be a valid timestamp string" });
    return null;
  }

  return parsed;
}

export function parseNullableTimestamptzString(value: unknown, path: string, issues: ValidationIssue[]): string | null {
  if (value === null) {
    return null;
  }

  return parseTimestamptzString(value, path, issues);
}

export function parseJsonObject(value: unknown, path: string, issues: ValidationIssue[]): JsonObject | null {
  const record = parseRecord(value, path, issues);
  if (record === null) {
    return null;
  }

  return record as JsonObject;
}

export function rejectStateLeakageFlags(value: JsonObject, path: string, issues: ValidationIssue[]): void {
  const leakageFlags = ["dlsApplied", "isDls", "noResult", "superOver", "incomplete"] as const;

  for (const flag of leakageFlags) {
    if (value[flag] === true) {
      issues.push({ path: `${path}.${flag}`, message: "future-only state is not allowed at this checkpoint" });
    }
  }
}
