"use strict";

(function installFeaturePage(root) {
  const contract = root.StockAgentFeaturePageConstraints;
  const revision = new RegExp(contract.revision_pattern);
  /** @param {unknown} value @returns {value is Record<string, unknown>} */
  const record = value => value !== null && typeof value === "object" && !Array.isArray(value);
  /** @param {unknown} value @param {number} minimum @param {number} maximum
   * @returns {value is number} */
  const boundedInteger = (value, minimum, maximum) => typeof value === "number"
    && Number.isSafeInteger(value) && value >= minimum && value <= maximum;

  /** Validate the actual response before narrowing to the generated API type.
   * The response and row metadata keep their original values and identities.
   * @param {unknown} value @param {number} offset @param {number} limit
   * @returns {import('./api-contracts').FeaturePagePayload} */
  function validateFeaturePage(value, offset, limit) {
    if (!record(value)
        || !boundedInteger(offset, contract.offset.minimum, contract.offset.maximum)
        || !boundedInteger(limit, contract.limit.minimum, contract.limit.maximum)
        || !contract.schema_version.includes(Number(value.schema_version)) || value.schema_version !== 1
        || value.read_only !== contract.read_only
        || value.production_control_possible !== contract.production_control_possible
        || !Array.isArray(value.rows) || !value.rows.every(record)
        || !record(value.filters) || !record(value.summary)
        || !(value.generated_at_utc === null || typeof value.generated_at_utc === "string")
        || typeof value.revision !== "string" || !revision.test(value.revision)
        || typeof value.reset_required !== "boolean"
        || !boundedInteger(value.matching_total, contract.matching_total.minimum, contract.matching_total.maximum)) {
      throw new Error("Invalid feature page");
    }
    const actualOffset = value.reset_required ? 0 : offset;
    if (value.offset !== actualOffset || value.limit !== limit
        || value.rows.length !== Math.min(limit, Math.max(0, value.matching_total-actualOffset))
        || value.has_more !== (actualOffset+value.rows.length < value.matching_total)) {
      throw new Error("Invalid feature page");
    }
    return /** @type {import('./api-contracts').FeaturePagePayload} */ (/** @type {unknown} */ (value));
  }
  root.StockAgentFeaturePage = Object.freeze({validateFeaturePage});
})(typeof window === "object" ? window : globalThis);
