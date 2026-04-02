import {
  collectUnknownKeys,
  DomainValidationError,
  parseNullableString,
  parseRecord,
  parseString,
  type ValidationIssue,
} from "./primitives.js";

export interface Team {
  name: string;
  shortName: string | null;
}

export function parseTeam(value: unknown): Team {
  const issues: ValidationIssue[] = [];
  const record = parseRecord(value, "team", issues);

  if (record === null) {
    throw new DomainValidationError(issues);
  }

  issues.push(...collectUnknownKeys(record, ["name", "shortName"]));

  const name = parseString(record["name"], "team.name", issues);
  const shortName = parseNullableString(record["shortName"], "team.shortName", issues);

  if (issues.length > 0 || name === null) {
    throw new DomainValidationError(issues);
  }

  return { name, shortName };
}
