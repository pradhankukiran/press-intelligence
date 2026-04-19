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

export type ApiError = {
  status: number;
  code: string;
  message: string;
  requestId?: string;
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
    const requestId = response.headers.get("X-Request-ID") ?? undefined;
    let code = "http_error";
    let message = `Request failed for ${path}`;
    try {
      const body = await response.json();
      if (body && typeof body === "object") {
        if (typeof body.code === "string") code = body.code;
        if (typeof body.message === "string") message = body.message;
      }
    } catch {
      // non-JSON body; fall through with defaults
    }
    const err: ApiError = {
      status: response.status,
      code,
      message,
      requestId,
    };
    throw err;
  }

  return (await response.json()) as T;
}

export async function fetchOverviewData(): Promise<DashboardData> {
  const [overview, opsStatus, runs] = await Promise.all([
    requestJson<OverviewResponse>("/api/analytics/overview"),
    requestJson<OpsStatusResponse>("/api/ops/status"),
    requestJson<RunsResponse>("/api/ops/runs?limit=8"),
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
    requestJson<SectionsResponse>("/api/analytics/sections"),
    requestJson<TagsResponse>("/api/analytics/tags?limit=8"),
    requestJson<PublishingVolumeResponse>("/api/analytics/publishing-volume"),
    requestJson<OverviewResponse>("/api/analytics/overview"),
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
    requestJson<OpsStatusResponse>("/api/ops/status"),
    requestJson<RunsResponse>("/api/ops/runs?limit=8"),
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
