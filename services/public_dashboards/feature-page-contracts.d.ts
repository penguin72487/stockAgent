import type {FeaturePagePayload} from './api-contracts';
export interface FeaturePageAPI {
  validateFeaturePage(value: unknown, offset: number, limit: number): FeaturePagePayload;
}
declare global {
  var StockAgentFeaturePage: Readonly<FeaturePageAPI>;
  var StockAgentFeaturePageConstraints: Readonly<{
    schema_version: readonly number[];
    read_only: true;
    production_control_possible: false;
    offset: {minimum: number; maximum: number};
    limit: {minimum: number; maximum: number};
    matching_total: {minimum: number; maximum: number};
    revision_pattern: string;
  }>;
}
