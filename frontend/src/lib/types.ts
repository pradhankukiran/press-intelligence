export type Tone = "up" | "down" | "neutral";

export interface KPI {
  label: string;
  value: string;
  delta: string;
  tone: Tone;
}

export interface TrendPoint {
  date: string;
  value: number;
}

export interface SectionPoint {
  date: string;
  world: number;
  politics: number;
  business: number;
  culture: number;
  climate: number;
  technology: number;
}

export interface TagPoint {
  tag: string;
  count: number;
  momentum: string;
}

export interface PipelineRun {
  run_id: string;
  dag_id: string;
  status: "success" | "failed" | "running" | "queued" | "scheduled";
  trigger: "system" | "manual";
  started_at: string;
  finished_at: string | null;
  window: string;
  error_summary: string | null;
}

export interface DataQualityCheck {
  name: string;
  status: "pass" | "warn" | "fail";
  observed_value: string;
  threshold: string;
  detail: string;
}

export interface OverviewResponse {
  range: string;
  kpis: KPI[];
  daily_volume: TrendPoint[];
  freshness: {
    last_sync_at: string;
    watermark: string;
    lag: string;
  };
  top_sections: { section: string; count: number }[];
}

export interface SectionsResponse {
  range: string;
  series: SectionPoint[];
  leaders: { section: string; count: number }[];
}

export interface TagsResponse {
  range: string;
  tags: TagPoint[];
}

export interface PublishingVolumeResponse {
  range: string;
  granularity: "day" | "hour";
  series: TrendPoint[];
}

export interface OpsStatusResponse {
  environment: string;
  mode: string;
  last_sync_at: string;
  freshness_lag: string;
  watermark: string;
  dags: { id: string; status: string }[];
  checks: DataQualityCheck[];
}

export interface RunsResponse {
  runs: PipelineRun[];
}

export interface BackfillResponse {
  run_id: string;
  dag_id: string;
  status: "queued" | "running" | "success" | "failed";
  message: string;
}
