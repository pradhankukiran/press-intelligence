"use client";

import Link from "next/link";
import { FormEvent, ReactNode, startTransition, useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  ApiError,
  type DashboardData,
  type DateRange,
  fetchAnalyticsData,
  fetchOperationsData,
  fetchOverviewData,
  triggerBackfill,
} from "@/lib/api";
import type {
  OpsStatusResponse,
  PipelineRun,
  SectionPoint,
  SectionsResponse,
  TagsResponse,
  TrendPoint,
} from "@/lib/types";

type RouteKey = "overview" | "analytics" | "ops";

const STATUS_OK =
  "border-[color:var(--status-ok)] bg-[color:var(--status-ok-soft)] text-[color:var(--status-ok)]";
const STATUS_WARN =
  "border-[color:var(--status-warn)] bg-[color:var(--status-warn-soft)] text-[color:var(--status-warn)]";
const STATUS_BAD =
  "border-[color:var(--status-bad)] bg-[color:var(--status-bad-soft)] text-[color:var(--status-bad)]";
const STATUS_NEUTRAL =
  "border-[color:var(--status-neutral)] bg-[color:var(--status-neutral-soft)] text-[color:var(--status-neutral)]";

const statusTone: Record<string, string> = {
  healthy: STATUS_OK,
  pass: STATUS_OK,
  success: STATUS_OK,
  warn: STATUS_WARN,
  queued: STATUS_WARN,
  running: STATUS_WARN,
  scheduled: STATUS_WARN,
  fail: STATUS_BAD,
  failed: STATUS_BAD,
  degraded: STATUS_BAD,
  idle: STATUS_NEUTRAL,
};

const sectionStyles = [
  { key: "world", label: "World", opacity: 1 },
  { key: "politics", label: "Politics", opacity: 0.84 },
  { key: "business", label: "Business", opacity: 0.68 },
  { key: "culture", label: "Culture", opacity: 0.52 },
  { key: "climate", label: "Climate", opacity: 0.36 },
  { key: "technology", label: "Technology", opacity: 0.22 },
] as const;

const routes: Array<{ key: RouteKey | "articles"; label: string; href: string }> = [
  { key: "overview", label: "Overview", href: "/" },
  { key: "analytics", label: "Analytics", href: "/analytics" },
  { key: "articles", label: "Articles", href: "/articles" },
  { key: "ops", label: "Operations", href: "/ops" },
];

async function fetchDataForRoute(
  route: RouteKey,
  range?: DateRange,
): Promise<DashboardData> {
  return route === "overview"
    ? fetchOverviewData(range)
    : route === "analytics"
      ? fetchAnalyticsData(range)
      : fetchOperationsData();
}

export function OverviewApp() {
  const [range, setRange] = useState<DateRange>({});
  const { data, error, isLoading, refresh } = useDashboardData("overview", range);

  return (
    <DashboardShell
      activeRoute="overview"
      title="Guardian newsroom overview across publishing volume, topic mix, and pipeline health."
      description="This summary route shows what has been ingested from the Guardian Content API, how fresh the warehouse is, and where to go for deeper analysis or operations."
      sidePanel={
        <SummaryPanel
          mode={data?.opsStatus.mode}
          lastSyncAt={data?.opsStatus.last_sync_at}
          watermark={data?.opsStatus.watermark}
          freshnessLag={data?.opsStatus.freshness_lag}
          failedRuns={data?.runs.runs.filter((run) => run.status === "failed").length}
        />
      }
      error={error}
      isLoading={isLoading}
      onRetry={refresh}
    >
      <DateRangeFilter range={range} onChange={setRange} />
      {data ? (
        <>
          <section className="grid gap-5">
            <SectionHeading
              eyebrow="Overview"
              title="Warehouse state and the top-line Guardian editorial signals."
              description={`Reporting range: ${data.overview.range}. Source: The Guardian Content API.`}
            />

            <div className="grid gap-4 lg:grid-cols-4">
              {data.overview.kpis.map((kpi) => (
                <KpiCard key={kpi.label} {...kpi} />
              ))}
            </div>

            <div className="grid gap-5 lg:grid-cols-[1.35fr_0.65fr]">
              <Panel
              title="Publishing volume"
              kicker="Daily output"
              detail="Daily Guardian article flow from the current reporting range."
            >
                <TrendChart points={data.overview.daily_volume} />
              </Panel>
              <Panel
              title="Top sections"
              kicker="Editorial leaders"
              detail="Most active Guardian sections in the current warehouse snapshot."
            >
                <TopSectionList items={data.overview.top_sections} />
              </Panel>
            </div>
          </section>

          <section className="grid gap-5 lg:grid-cols-2">
            <Panel
              title="Go to Analytics"
              kicker="Dedicated route"
              detail="Open the Guardian analysis workspace for sections, topics, and publishing distribution."
            >
              <QuickRouteCard
                href="/analytics"
                label="Open analytics workspace"
                body="Section composition, tag momentum, section leaders, and Guardian publishing charts live on the dedicated analytics route."
              />
            </Panel>
            <Panel
              title="Go to Operations"
              kicker="Dedicated route"
              detail="Open the admin surface for Guardian ingest backfills, DAG state, data quality, and run history."
            >
              <QuickRouteCard
                href="/ops"
                label="Open operations workspace"
                body="Backfill control, quality checks, and Guardian pipeline history live on the dedicated operations route."
              />
            </Panel>
          </section>
        </>
      ) : null}
    </DashboardShell>
  );
}

export function AnalyticsApp() {
  const [range, setRange] = useState<DateRange>({});
  const { data, error, isLoading, refresh } = useDashboardData("analytics", range);

  return (
    <DashboardShell
      activeRoute="analytics"
      title="Guardian analytics workspace for section mix, topic movement, and publishing consistency."
      description="This route is focused on Guardian article metadata only: sections, tags, and publishing output. Operational tooling lives on the separate ops route."
      sidePanel={
        <SummaryPanel
          mode="Guardian"
          lastSyncAt={data?.overview.freshness.last_sync_at}
          watermark={data?.overview.freshness.watermark}
          freshnessLag={data?.overview.freshness.lag}
        />
      }
      error={error}
      isLoading={isLoading}
      onRetry={refresh}
    >
      <DateRangeFilter range={range} onChange={setRange} />
      {data ? (
        <>
          <section className="grid gap-5">
            <SectionHeading
              eyebrow="Analytics"
              title="Guardian section composition, tag momentum, and publishing distribution."
              description={`Source window: ${data.sections.range}. Source: The Guardian Content API.`}
            />

            <div className="grid gap-5 lg:grid-cols-[1.2fr_0.8fr]">
              <Panel
              title="Section composition"
              kicker="Desk mix"
              detail="Relative Guardian section share across the most recent publishing windows."
            >
                <SectionCompositionChart series={data.sections.series} />
              </Panel>
              <Panel
              title="Tag momentum"
              kicker="Topic watch"
              detail="Top Guardian tags and their recent movement."
            >
                <TagBoard tags={data.tags.tags} />
              </Panel>
            </div>

            <div className="grid gap-5 lg:grid-cols-[0.9fr_1.1fr]">
              <Panel
              title="Publishing volume"
              kicker="Distribution"
              detail={`Granularity: ${data.publishing.granularity}. Counts reflect Guardian article publication volume.`}
            >
                <PublishingBarChart points={data.publishing.series} />
              </Panel>
              <Panel
              title="Section leaders"
              kicker="Share"
              detail="Total Guardian stories by leading section."
            >
                <LeaderTable leaders={data.sections.leaders} />
              </Panel>
            </div>
          </section>
        </>
      ) : null}
    </DashboardShell>
  );
}

export function OperationsApp() {
  const {
    data,
    error,
    isLoading,
    isSubmitting,
    backfillMessage,
    startDate,
    endDate,
    setStartDate,
    setEndDate,
    handleBackfill,
    refresh,
  } = useDashboardData("ops");

  return (
    <DashboardShell
      activeRoute="ops"
      title="Guardian operations workspace for backfills, DAG health, and quality checks."
      description="This route is for admin and operational work on the Guardian ingestion pipeline. The editorial analysis surface lives separately on the analytics route."
      sidePanel={
        <SummaryPanel
          mode={data?.opsStatus.mode}
          lastSyncAt={data?.opsStatus.last_sync_at}
          watermark={data?.opsStatus.watermark}
          freshnessLag={data?.opsStatus.freshness_lag}
          failedRuns={data?.runs.runs.filter((run) => run.status === "failed").length}
        />
      }
      error={error}
      isLoading={isLoading}
      onRetry={refresh}
    >
      {data ? (
        <>
          <section className="grid gap-5">
            <SectionHeading
              eyebrow="Operations"
              title="Operational control for Guardian backfills, DAG status, and data quality."
              description="This is the admin surface for the Guardian Content API ingestion and warehouse refresh jobs."
            />

            <div className="grid gap-5 lg:grid-cols-[0.85fr_1.15fr]">
              <Panel
              title="Backfill control"
              kicker="Manual trigger"
              detail="Submit a date range to queue a historical Guardian content replay."
            >
                <form className="grid gap-4" onSubmit={handleBackfill}>
                  <div className="grid gap-4 sm:grid-cols-2">
                    <Field label="Start date">
                      <input
                        type="date"
                        value={startDate}
                        onChange={(event) => setStartDate(event.target.value)}
                        className="w-full border border-black bg-white px-4 py-3 text-black outline-none focus:border-[color:var(--accent)]"
                      />
                    </Field>
                    <Field label="End date">
                      <input
                        type="date"
                        value={endDate}
                        onChange={(event) => setEndDate(event.target.value)}
                        className="w-full border border-black bg-white px-4 py-3 text-black outline-none focus:border-[color:var(--accent)]"
                      />
                    </Field>
                  </div>
                  <button
                    type="submit"
                    disabled={isSubmitting}
                    className="inline-flex items-center justify-center border border-[color:var(--accent)] bg-[color:var(--accent)] px-5 py-3 text-sm font-semibold text-black transition hover:bg-[color:var(--accent-soft)] disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {isSubmitting ? "Queueing backfill..." : "Trigger backfill"}
                  </button>
                  {backfillMessage ? (
                    <p className="border border-[color:var(--accent)] bg-[color:var(--accent-soft)] px-4 py-3 text-sm text-black">
                      {backfillMessage}
                    </p>
                  ) : null}
                </form>
              </Panel>

              <Panel
              title="Pipeline health"
              kicker="DAG status"
              detail="Live operational surface for the Guardian ingest, transform, and quality DAGs."
            >
                <div className="grid gap-3 sm:grid-cols-2">
                  {data.opsStatus.dags.map((dag) => (
                    <div key={dag.id} className="border border-black bg-white px-4 py-4">
                      <div className="mb-3 flex items-start justify-between gap-3">
                        <div>
                          <p className="text-sm font-semibold text-black">{dag.id}</p>
                          <p className="text-xs text-black/60">{data.opsStatus.last_sync_at}</p>
                        </div>
                        <span
                          className={`border px-3 py-1 text-xs font-medium ${statusTone[dag.status] ?? statusTone.idle}`}
                        >
                          {dag.status}
                        </span>
                      </div>
                      <p className="text-sm leading-6 text-black/70">
                        Current freshness lag is {data.opsStatus.freshness_lag}. Watermark is{" "}
                        {data.opsStatus.watermark}.
                      </p>
                    </div>
                  ))}
                </div>
              </Panel>
            </div>

            <div className="grid gap-5 lg:grid-cols-[0.85fr_1.15fr]">
              <Panel
              title="Data quality"
              kicker="Integrity checks"
              detail="Latest quality checks across Guardian raw ingestion and transformed reporting data."
            >
                <QualityList checks={data.opsStatus.checks} />
              </Panel>
              <Panel
              title="Recent runs"
              kicker="Execution history"
              detail="System and manual Guardian pipeline runs persisted by the backend."
            >
                <RunsTable runs={data.runs.runs} />
              </Panel>
            </div>
          </section>
        </>
      ) : null}
    </DashboardShell>
  );
}

type DashboardError = { message: string; requestId?: string };

function toDashboardError(loadError: unknown): DashboardError {
  if (loadError instanceof ApiError) {
    return { message: loadError.message, requestId: loadError.requestId };
  }
  if (loadError instanceof Error) {
    return { message: loadError.message };
  }
  return { message: "Failed to load dashboard." };
}

function DateRangeFilter({
  range,
  onChange,
}: {
  range: DateRange;
  onChange: (next: DateRange) => void;
}) {
  return (
    <section className="border border-black bg-white px-6 py-4">
      <div className="flex flex-wrap items-center gap-4 text-sm">
        <div className="text-xs font-semibold uppercase tracking-[0.2em] text-black/60">
          Date range
        </div>
        <label className="flex items-center gap-2">
          <span className="text-black/70">From</span>
          <input
            type="date"
            value={range.fromDate ?? ""}
            onChange={(e) =>
              onChange({ ...range, fromDate: e.target.value || undefined })
            }
            className="border border-black bg-white px-3 py-2 text-black outline-none focus:border-[color:var(--accent)]"
          />
        </label>
        <label className="flex items-center gap-2">
          <span className="text-black/70">To</span>
          <input
            type="date"
            value={range.toDate ?? ""}
            onChange={(e) =>
              onChange({ ...range, toDate: e.target.value || undefined })
            }
            className="border border-black bg-white px-3 py-2 text-black outline-none focus:border-[color:var(--accent)]"
          />
        </label>
        {(range.fromDate || range.toDate) && (
          <button
            type="button"
            onClick={() => onChange({})}
            className="ml-auto inline-flex items-center justify-center border border-black bg-[color:var(--background)] px-3 py-2 text-xs font-semibold uppercase tracking-wide text-black transition hover:bg-[color:var(--accent-soft)]"
          >
            Clear
          </button>
        )}
      </div>
    </section>
  );
}


function useDashboardData(route: RouteKey, range?: DateRange) {
  const [data, setData] = useState<DashboardData | null>(null);
  const [error, setError] = useState<DashboardError | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [backfillMessage, setBackfillMessage] = useState<string | null>(null);
  const [startDate, setStartDate] = useState("2026-03-01");
  const [endDate, setEndDate] = useState("2026-03-03");

  const fromDate = range?.fromDate ?? null;
  const toDate = range?.toDate ?? null;

  useEffect(() => {
    let isActive = true;

    async function loadForRoute() {
      setIsLoading(true);
      setError(null);

      try {
        const next = await fetchDataForRoute(route, {
          fromDate: fromDate ?? undefined,
          toDate: toDate ?? undefined,
        });
        if (isActive) {
          startTransition(() => setData(next));
        }
      } catch (loadError) {
        if (isActive) {
          setError(toDashboardError(loadError));
        }
      } finally {
        if (isActive) {
          setIsLoading(false);
        }
      }
    }

    void loadForRoute();

    return () => {
      isActive = false;
    };
  }, [route, fromDate, toDate]);

  async function refreshDashboard(selectedRoute: RouteKey = route) {
    setIsLoading(true);
    setError(null);

    try {
      const next = await fetchDataForRoute(selectedRoute, {
        fromDate: fromDate ?? undefined,
        toDate: toDate ?? undefined,
      });
      startTransition(() => setData(next));
    } catch (loadError) {
      setError(toDashboardError(loadError));
    } finally {
      setIsLoading(false);
    }
  }

  async function handleBackfill(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsSubmitting(true);
    setBackfillMessage(null);

    try {
      const response = await triggerBackfill(startDate, endDate);
      setBackfillMessage(response.message);
      await refreshDashboard(route);
    } catch (submitError) {
      setBackfillMessage(
        submitError instanceof Error ? submitError.message : "Backfill trigger failed.",
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  return {
    data,
    error,
    isLoading,
    isSubmitting,
    backfillMessage,
    startDate,
    endDate,
    setStartDate,
    setEndDate,
    handleBackfill,
    refresh: () => refreshDashboard(route),
  };
}

function DashboardShell({
  activeRoute,
  title,
  description,
  sidePanel,
  error,
  isLoading,
  onRetry,
  children,
}: {
  activeRoute: RouteKey;
  title: string;
  description: string;
  sidePanel: ReactNode;
  error: DashboardError | null;
  isLoading: boolean;
  onRetry?: () => void;
  children: ReactNode;
}) {
  return (
    <main className="news-shell min-h-screen bg-[color:var(--background)] px-4 py-6 text-black sm:px-6 lg:px-8">
      <div className="mx-auto flex w-[92vw] max-w-[1600px] flex-col gap-6">
        <header className="border border-black bg-white">
          <div className="grid gap-6 px-6 py-6 lg:grid-cols-[1.45fr_0.8fr] lg:px-8">
            <div className="flex flex-col gap-5">
              <div className="flex flex-wrap items-center gap-3 text-xs font-semibold uppercase tracking-[0.22em] text-black/70">
                <span className="border border-[color:var(--accent)] bg-[color:var(--accent-soft)] px-3 py-1">
                  Press Intelligence
                </span>
                <span>Guardian Editorial Analytics and Operations</span>
              </div>
              <div className="grid gap-3">
                <h1 className="max-w-4xl text-3xl font-semibold tracking-[-0.03em] text-black sm:text-4xl">
                  {title}
                </h1>
                <p className="max-w-3xl text-sm leading-7 text-black/70 sm:text-base">
                  {description}
                </p>
              </div>
              <nav className="flex flex-wrap gap-3 text-sm font-medium">
                {routes.map((item) => {
                  const isActive = item.key === activeRoute;
                  return (
                    <Link
                      key={item.href}
                      href={item.href}
                      className={`border px-4 py-2 transition ${
                        isActive
                          ? "border-[color:var(--accent)] bg-[color:var(--accent-soft)]"
                          : "border-black bg-[color:var(--background)] hover:border-[color:var(--accent)] hover:bg-[color:var(--accent-soft)]"
                      }`}
                    >
                      {item.label}
                    </Link>
                  );
                })}
              </nav>
            </div>
            {sidePanel}
          </div>
        </header>

        {isLoading ? <LoadingPanel /> : null}

        {error ? (
          <section
            className={`border px-6 py-4 text-sm ${STATUS_BAD}`}
            role="alert"
          >
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <p className="font-semibold">Could not load this view.</p>
                <p className="mt-1">{error.message}</p>
                {error.requestId ? (
                  <p className="mt-2 text-xs opacity-80">Request ID: {error.requestId}</p>
                ) : null}
              </div>
              {onRetry ? (
                <button
                  type="button"
                  onClick={onRetry}
                  className="inline-flex items-center justify-center border border-current px-4 py-2 text-xs font-semibold uppercase tracking-wide transition hover:bg-white/40"
                >
                  Retry
                </button>
              ) : null}
            </div>
          </section>
        ) : null}

        {children}
      </div>
    </main>
  );
}

function SummaryPanel({
  mode,
  lastSyncAt,
  watermark,
  freshnessLag,
  failedRuns,
}: {
  mode?: string;
  lastSyncAt?: string;
  watermark?: string;
  freshnessLag?: string;
  failedRuns?: number;
}) {
  const failureValue = failedRuns === undefined ? "N/A" : String(failedRuns);

  return (
    <section className="border border-black bg-[color:var(--background)] px-5 py-5">
      <div className="mb-4 flex items-center justify-between gap-3 border-b border-black pb-3">
        <span className="text-xs font-semibold uppercase tracking-[0.2em] text-black/70">
          Freshness Monitor
        </span>
        <span className="border border-[color:var(--accent)] bg-white px-3 py-1 text-xs font-medium">
          {mode ?? "mock"}
        </span>
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <MetricInset label="Last sync" value={lastSyncAt ?? "--"} />
        <MetricInset label="Watermark" value={watermark ?? "--"} />
        <MetricInset label="Freshness lag" value={freshnessLag ?? "--"} />
        <MetricInset label="Recent failures" value={failureValue} />
      </div>
    </section>
  );
}

function QuickRouteCard({
  href,
  label,
  body,
}: {
  href: string;
  label: string;
  body: string;
}) {
  return (
    <div className="grid gap-4">
      <p className="text-sm leading-7 text-black/70">{body}</p>
      <Link
        href={href}
        className="inline-flex w-fit items-center justify-center border border-[color:var(--accent)] bg-[color:var(--accent)] px-5 py-3 text-sm font-semibold text-black transition hover:bg-[color:var(--accent-soft)]"
      >
        {label}
      </Link>
    </div>
  );
}

function SectionHeading({
  eyebrow,
  title,
  description,
}: {
  eyebrow: string;
  title: string;
  description: string;
}) {
  return (
    <div className="grid gap-2 border-b border-black pb-3">
      <span className="text-xs font-semibold uppercase tracking-[0.2em] text-black/70">
        {eyebrow}
      </span>
      <div className="grid gap-2 lg:grid-cols-[1fr_0.8fr] lg:items-end">
        <h2 className="text-2xl font-semibold tracking-[-0.03em] text-black sm:text-3xl">
          {title}
        </h2>
        <p className="text-sm leading-7 text-black/70">{description}</p>
      </div>
    </div>
  );
}

function Panel({
  title,
  kicker,
  detail,
  children,
}: {
  title: string;
  kicker: string;
  detail: string;
  children: ReactNode;
}) {
  return (
    <section className="border border-black bg-white p-6">
      <div className="mb-5 grid gap-2 border-b border-black pb-4">
        <span className="text-xs font-semibold uppercase tracking-[0.2em] text-black/70">
          {kicker}
        </span>
        <div className="grid gap-2 lg:grid-cols-[0.75fr_1.25fr] lg:items-end">
          <h3 className="text-xl font-semibold text-black">{title}</h3>
          <p className="text-sm leading-6 text-black/70">{detail}</p>
        </div>
      </div>
      {children}
    </section>
  );
}

function KpiCard({
  label,
  value,
  delta,
}: {
  label: string;
  value: string;
  delta: string;
  tone: "up" | "down" | "neutral";
}) {
  return (
    <div className="border border-black bg-white px-5 py-5">
      <p className="text-sm text-black/70">{label}</p>
      <div className="mt-3 flex items-end justify-between gap-4">
        <span className="text-3xl font-semibold tracking-[-0.03em] text-black">{value}</span>
        <span className="border border-[color:var(--accent)] bg-[color:var(--accent-soft)] px-3 py-1 text-xs font-medium text-black">
          {delta}
        </span>
      </div>
    </div>
  );
}

function MetricInset({ label, value }: { label: string; value: string }) {
  return (
    <div className="border border-black bg-white px-4 py-4">
      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-black/70">{label}</p>
      <p className="mt-2 text-sm leading-6 text-black">{value}</p>
    </div>
  );
}

function TrendChart({ points }: { points: TrendPoint[] }) {
  if (points.length === 0) {
    return <EmptyChartState label="No publishing volume available for the selected range." />;
  }

  return (
    <div className="grid gap-4">
      <div className="h-72 w-full border border-black bg-[color:var(--background)] p-2">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={points} margin={{ top: 12, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid stroke="rgba(0,0,0,0.14)" strokeDasharray="0" vertical={false} />
            <XAxis
              dataKey="date"
              stroke="#000000"
              tick={{ fontSize: 11, fill: "#000000" }}
              tickFormatter={(value: string) => value.slice(5)}
            />
            <YAxis stroke="#000000" tick={{ fontSize: 11, fill: "#000000" }} width={36} />
            <Tooltip
              contentStyle={{
                background: "#ffffff",
                border: "1px solid #000000",
                borderRadius: 0,
                fontSize: 12,
              }}
              labelStyle={{ color: "#000000" }}
            />
            <Line
              type="monotone"
              dataKey="value"
              stroke="var(--accent)"
              strokeWidth={3}
              dot={{ fill: "var(--accent)", r: 4 }}
              activeDot={{ r: 5 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <div className="grid grid-cols-3 gap-3 text-xs text-black/70 sm:grid-cols-6">
        {points.slice(-6).map((point) => (
          <div key={point.date} className="border border-black bg-[color:var(--background)] px-3 py-2">
            <div>{point.date.slice(5)}</div>
            <div className="mt-1 font-semibold text-black">{point.value}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function TopSectionList({ items }: { items: { section: string; count: number }[] }) {
  if (items.length === 0) {
    return <EmptyChartState label="No section leaders available yet." />;
  }

  const max = Math.max(...items.map((item) => item.count), 1);

  return (
    <div className="grid gap-4">
      {items.map((item) => (
        <div key={item.section} className="grid gap-2">
          <div className="flex items-center justify-between gap-3 text-sm text-black">
            <span className="font-medium">{item.section}</span>
            <span className="text-black/70">{item.count.toLocaleString()}</span>
          </div>
          <div className="h-3 border border-black bg-[color:var(--background)]">
            <div
              className="h-full bg-[color:var(--accent)]"
              style={{ width: `${(item.count / max) * 100}%` }}
            />
          </div>
        </div>
      ))}
    </div>
  );
}

function SectionCompositionChart({ series }: { series: SectionPoint[] }) {
  if (series.length === 0) {
    return <EmptyChartState label="No section mix data available yet." />;
  }

  const chartData = series.slice(-10).map((point) => ({
    date: point.date,
    ...sectionStyles.reduce<Record<string, number>>((acc, section) => {
      acc[section.key] = point[section.key];
      return acc;
    }, {}),
  }));

  return (
    <div className="grid gap-4">
      <div className="h-72 w-full border border-black bg-[color:var(--background)] p-2">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} margin={{ top: 12, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid stroke="rgba(0,0,0,0.14)" strokeDasharray="0" vertical={false} />
            <XAxis
              dataKey="date"
              stroke="#000000"
              tick={{ fontSize: 11, fill: "#000000" }}
              tickFormatter={(value: string) => value.slice(5)}
            />
            <YAxis stroke="#000000" tick={{ fontSize: 11, fill: "#000000" }} width={36} />
            <Tooltip
              contentStyle={{
                background: "#ffffff",
                border: "1px solid #000000",
                borderRadius: 0,
                fontSize: 12,
              }}
              labelStyle={{ color: "#000000" }}
            />
            {sectionStyles.map((section) => (
              <Bar
                key={section.key}
                dataKey={section.key}
                stackId="sections"
                fill={`rgba(47, 107, 255, ${section.opacity})`}
                stroke="#000000"
                strokeWidth={0}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="flex flex-wrap gap-3 text-xs text-black/70">
        {sectionStyles.map((section) => (
          <div key={section.key} className="flex items-center gap-2 border border-black bg-white px-3 py-2">
            <span
              className="h-3 w-3 border border-black"
              style={{ backgroundColor: `rgba(47, 107, 255, ${section.opacity})` }}
            />
            {section.label}
          </div>
        ))}
      </div>
    </div>
  );
}

function TagBoard({ tags }: { tags: TagsResponse["tags"] }) {
  if (tags.length === 0) {
    return <EmptyChartState label="No tags were returned for this range." />;
  }

  return (
    <div className="grid gap-3">
      {tags.map((tag, index) => (
        <div key={tag.tag} className="border border-black bg-[color:var(--background)] px-4 py-4">
          <div className="mb-3 flex items-center justify-between gap-4">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-black/60">
                Rank {index + 1}
              </p>
              <p className="mt-2 text-lg font-semibold text-black">{tag.tag}</p>
            </div>
            <span className="border border-[color:var(--accent)] bg-white px-3 py-1 text-xs font-medium text-black">
              {tag.momentum}
            </span>
          </div>
          <p className="text-sm text-black/70">{tag.count} mentions</p>
        </div>
      ))}
    </div>
  );
}

function PublishingBarChart({ points }: { points: TrendPoint[] }) {
  if (points.length === 0) {
    return <EmptyChartState label="No publishing bars to render yet." />;
  }

  return (
    <div className="grid gap-4">
      <div className="h-64 w-full border border-black bg-[color:var(--background)] p-2">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={points} margin={{ top: 12, right: 16, bottom: 8, left: 0 }}>
            <CartesianGrid stroke="rgba(0,0,0,0.14)" strokeDasharray="0" vertical={false} />
            <XAxis
              dataKey="date"
              stroke="#000000"
              tick={{ fontSize: 11, fill: "#000000" }}
              tickFormatter={(value: string) => value.slice(5)}
            />
            <YAxis stroke="#000000" tick={{ fontSize: 11, fill: "#000000" }} width={36} />
            <Tooltip
              contentStyle={{
                background: "#ffffff",
                border: "1px solid #000000",
                borderRadius: 0,
                fontSize: 12,
              }}
              labelStyle={{ color: "#000000" }}
            />
            <Bar dataKey="value" fill="var(--accent)" stroke="#000000" strokeWidth={0} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function LeaderTable({ leaders }: { leaders: SectionsResponse["leaders"] }) {
  return (
    <div className="border border-black bg-white">
      <div className="grid grid-cols-[1.6fr_0.6fr] gap-3 border-b border-black px-4 py-3 text-xs font-semibold uppercase tracking-[0.18em] text-black/70">
        <span>Section</span>
        <span className="text-right">Stories</span>
      </div>
      {leaders.map((leader) => (
        <div
          key={leader.section}
          className="grid grid-cols-[1.6fr_0.6fr] gap-3 border-b border-black px-4 py-4 text-sm text-black last:border-b-0"
        >
          <span>{leader.section}</span>
          <span className="text-right font-semibold">{leader.count.toLocaleString()}</span>
        </div>
      ))}
    </div>
  );
}

function QualityList({ checks }: { checks: OpsStatusResponse["checks"] }) {
  return (
    <div className="grid gap-3">
      {checks.map((check) => (
        <div key={check.name} className="border border-black bg-[color:var(--background)] px-4 py-4">
          <div className="mb-3 flex items-center justify-between gap-3">
            <h4 className="font-semibold text-black">{check.name}</h4>
            <span
              className={`border px-3 py-1 text-xs font-medium ${statusTone[check.status]}`}
            >
              {check.status}
            </span>
          </div>
          <div className="grid gap-2 text-sm leading-6 text-black/70">
            <p>
              Observed <span className="font-semibold text-black">{check.observed_value}</span>
            </p>
            <p>
              Threshold <span className="font-semibold text-black">{check.threshold}</span>
            </p>
            <p>{check.detail}</p>
          </div>
        </div>
      ))}
    </div>
  );
}

function RunsTable({ runs }: { runs: PipelineRun[] }) {
  return (
    <div className="border border-black bg-white">
      <div className="grid grid-cols-[1.1fr_0.8fr_0.7fr_1.4fr] gap-3 border-b border-black px-4 py-3 text-xs font-semibold uppercase tracking-[0.18em] text-black/70">
        <span>DAG</span>
        <span>Status</span>
        <span>Trigger</span>
        <span>Window</span>
      </div>
      {runs.map((run) => (
        <div
          key={run.run_id}
          className="grid grid-cols-[1.1fr_0.8fr_0.7fr_1.4fr] gap-3 border-b border-black px-4 py-4 text-sm text-black last:border-b-0"
        >
          <div className="min-w-0">
            <p className="truncate font-semibold">{run.dag_id}</p>
            <p className="truncate text-xs text-black/60">{run.run_id}</p>
          </div>
          <span
            className={`inline-flex w-fit border px-3 py-1 text-xs font-medium ${statusTone[run.status]}`}
          >
            {run.status}
          </span>
          <span className="capitalize text-black/70">{run.trigger}</span>
          <div className="min-w-0">
            <p className="truncate">{run.window}</p>
            <p className="truncate text-xs text-black/60">{run.error_summary ?? run.started_at}</p>
          </div>
        </div>
      ))}
    </div>
  );
}

function Field({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <label className="grid gap-2 text-sm font-medium text-black">
      <span>{label}</span>
      {children}
    </label>
  );
}

function LoadingPanel() {
  return (
    <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {Array.from({ length: 4 }).map((_, index) => (
        <div key={index} className="h-32 animate-pulse border border-black bg-white" />
      ))}
    </section>
  );
}

function EmptyChartState({ label }: { label: string }) {
  return (
    <div className="border border-dashed border-black bg-[color:var(--background)] px-4 py-10 text-center text-sm text-black/70">
      {label}
    </div>
  );
}
