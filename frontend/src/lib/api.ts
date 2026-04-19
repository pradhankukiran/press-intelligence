import type {
  BackfillResponse,
  OpsStatusResponse,
  OverviewResponse,
  PublishingVolumeResponse,
  RunsResponse,
  SectionsResponse,
  TagsResponse,
} from "@/lib/types";

export type DashboardData = {
  overview: OverviewResponse;
  sections: SectionsResponse;
  tags: TagsResponse;
  publishing: PublishingVolumeResponse;
  opsStatus: OpsStatusResponse;
  runs: RunsResponse;
};

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") ?? "http://localhost:8000";

const EMPTY_OVERVIEW: OverviewResponse = {
  range: "No data",
  kpis: [],
  daily_volume: [],
  freshness: {
    last_sync_at: "Unavailable",
    watermark: "Unavailable",
    lag: "Unavailable",
  },
  top_sections: [],
};

const EMPTY_SECTIONS: SectionsResponse = {
  range: "No data",
  series: [],
  leaders: [],
};

const EMPTY_TAGS: TagsResponse = {
  range: "No data",
  tags: [],
};

const EMPTY_PUBLISHING: PublishingVolumeResponse = {
  range: "No data",
  granularity: "day",
  series: [],
};

const EMPTY_OPS_STATUS: OpsStatusResponse = {
  environment: "development",
  mode: "Unavailable",
  last_sync_at: "Unavailable",
  freshness_lag: "Unavailable",
  watermark: "Unavailable",
  dags: [],
  checks: [],
};

const EMPTY_RUNS: RunsResponse = {
  runs: [],
};

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    cache: "no-store",
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Request failed for ${path}`);
  }

  return (await response.json()) as T;
}

async function requestJsonOrFallback<T>(path: string, fallback: T, init?: RequestInit): Promise<T> {
  try {
    return await requestJson<T>(path, init);
  } catch {
    return fallback;
  }
}

export async function fetchOverviewData(): Promise<DashboardData> {
  const [overview, opsStatus, runs] = await Promise.all([
    requestJsonOrFallback<OverviewResponse>("/api/analytics/overview", EMPTY_OVERVIEW),
    requestJsonOrFallback<OpsStatusResponse>("/api/ops/status", EMPTY_OPS_STATUS),
    requestJsonOrFallback<RunsResponse>("/api/ops/runs?limit=8", EMPTY_RUNS),
  ]);

  return {
    overview,
    sections: EMPTY_SECTIONS,
    tags: EMPTY_TAGS,
    publishing: EMPTY_PUBLISHING,
    opsStatus,
    runs,
  };
}

export async function fetchAnalyticsData(): Promise<DashboardData> {
  const [sections, tags, publishing, overview] = await Promise.all([
    requestJsonOrFallback<SectionsResponse>("/api/analytics/sections", EMPTY_SECTIONS),
    requestJsonOrFallback<TagsResponse>("/api/analytics/tags?limit=8", EMPTY_TAGS),
    requestJsonOrFallback<PublishingVolumeResponse>(
      "/api/analytics/publishing-volume",
      EMPTY_PUBLISHING,
    ),
    requestJsonOrFallback<OverviewResponse>("/api/analytics/overview", EMPTY_OVERVIEW),
  ]);

  return {
    overview,
    sections,
    tags,
    publishing,
    opsStatus: EMPTY_OPS_STATUS,
    runs: EMPTY_RUNS,
  };
}

export async function fetchOperationsData(): Promise<DashboardData> {
  const [opsStatus, runs] = await Promise.all([
    requestJsonOrFallback<OpsStatusResponse>("/api/ops/status", EMPTY_OPS_STATUS),
    requestJsonOrFallback<RunsResponse>("/api/ops/runs?limit=8", EMPTY_RUNS),
  ]);

  return {
    overview: EMPTY_OVERVIEW,
    sections: EMPTY_SECTIONS,
    tags: EMPTY_TAGS,
    publishing: EMPTY_PUBLISHING,
    opsStatus,
    runs,
  };
}

export async function triggerBackfill(
  startDate: string,
  endDate: string,
): Promise<BackfillResponse> {
  return requestJson<BackfillResponse>("/api/ops/backfills", {
    method: "POST",
    body: JSON.stringify({
      start_date: startDate,
      end_date: endDate,
    }),
  });
}
