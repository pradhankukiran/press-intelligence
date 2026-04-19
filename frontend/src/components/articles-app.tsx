"use client";

import Link from "next/link";
import { FormEvent, startTransition, useEffect, useState } from "react";

import { ApiError, fetchArticle, fetchArticles } from "@/lib/api";
import type { ArticleDetail, ArticleSearchResponse, ArticleSummary } from "@/lib/types";

type FormState = {
  query: string;
  section: string;
  tag: string;
  fromDate: string;
  toDate: string;
};

const initialForm: FormState = {
  query: "",
  section: "",
  tag: "",
  fromDate: "",
  toDate: "",
};

const PAGE_SIZE = 20;

export function ArticlesApp() {
  const [form, setForm] = useState<FormState>(initialForm);
  const [applied, setApplied] = useState<FormState>(initialForm);
  const [offset, setOffset] = useState(0);
  const [data, setData] = useState<ArticleSearchResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<ArticleDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    async function run() {
      setIsLoading(true);
      setError(null);
      try {
        const result = await fetchArticles({
          query: applied.query || undefined,
          section: applied.section || undefined,
          tag: applied.tag || undefined,
          fromDate: applied.fromDate || undefined,
          toDate: applied.toDate || undefined,
          limit: PAGE_SIZE,
          offset,
        });
        if (!active) return;
        startTransition(() => setData(result));
      } catch (err) {
        if (!active) return;
        setError(err instanceof ApiError ? err.message : "Could not load articles.");
      } finally {
        if (active) setIsLoading(false);
      }
    }
    void run();
    return () => {
      active = false;
    };
  }, [applied, offset]);

  useEffect(() => {
    if (selectedId === null) {
      setDetail(null);
      return;
    }
    const id: string = selectedId;
    let active = true;
    async function run() {
      setDetailLoading(true);
      setDetailError(null);
      try {
        const result = await fetchArticle(id);
        if (!active) return;
        setDetail(result);
      } catch (err) {
        if (!active) return;
        setDetailError(err instanceof ApiError ? err.message : "Could not load article.");
      } finally {
        if (active) setDetailLoading(false);
      }
    }
    void run();
    return () => {
      active = false;
    };
  }, [selectedId]);

  function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setOffset(0);
    setApplied({ ...form });
  }

  function onReset() {
    setForm(initialForm);
    setApplied(initialForm);
    setOffset(0);
  }

  const totalPages = data ? Math.max(1, Math.ceil(data.total / PAGE_SIZE)) : 1;
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1;

  return (
    <main className="min-h-screen bg-[color:var(--background)] px-4 py-6 text-black sm:px-6 lg:px-8">
      <div className="mx-auto flex w-[92vw] max-w-[1600px] flex-col gap-6">
        <header className="border border-black bg-white">
          <div className="grid gap-6 px-6 py-6 lg:grid-cols-[1.45fr_0.8fr] lg:px-8">
            <div className="flex flex-col gap-5">
              <div className="flex flex-wrap items-center gap-3 text-xs font-semibold uppercase tracking-[0.22em] text-black/70">
                <span className="border border-[color:var(--accent)] bg-[color:var(--accent-soft)] px-3 py-1">
                  Press Intelligence
                </span>
                <span>Guardian Article Search</span>
              </div>
              <div className="grid gap-3">
                <h1 className="max-w-4xl text-3xl font-semibold tracking-[-0.03em] text-black sm:text-4xl">
                  Search Guardian articles by keyword, section, or tag.
                </h1>
                <p className="max-w-3xl text-sm leading-7 text-black/70 sm:text-base">
                  Articles come from `analytics.articles_latest` in real mode or the
                  seeded mock set otherwise. Click any row to inspect the full payload.
                </p>
              </div>
              <nav className="flex flex-wrap gap-3 text-sm font-medium">
                <Link
                  href="/"
                  className="border border-black bg-[color:var(--background)] px-4 py-2 transition hover:border-[color:var(--accent)] hover:bg-[color:var(--accent-soft)]"
                >
                  Overview
                </Link>
                <Link
                  href="/analytics"
                  className="border border-black bg-[color:var(--background)] px-4 py-2 transition hover:border-[color:var(--accent)] hover:bg-[color:var(--accent-soft)]"
                >
                  Analytics
                </Link>
                <span className="border border-[color:var(--accent)] bg-[color:var(--accent-soft)] px-4 py-2">
                  Articles
                </span>
                <Link
                  href="/ops"
                  className="border border-black bg-[color:var(--background)] px-4 py-2 transition hover:border-[color:var(--accent)] hover:bg-[color:var(--accent-soft)]"
                >
                  Operations
                </Link>
              </nav>
            </div>
          </div>
        </header>

        <section className="border border-black bg-white p-6">
          <form className="grid gap-4" onSubmit={onSubmit}>
            <div className="grid gap-4 lg:grid-cols-[1.4fr_0.8fr_0.8fr_0.8fr_0.8fr]">
              <Field label="Keyword">
                <input
                  type="text"
                  value={form.query}
                  onChange={(e) => setForm({ ...form, query: e.target.value })}
                  placeholder="e.g. climate"
                  className="w-full border border-black bg-white px-4 py-3 text-black outline-none focus:border-[color:var(--accent)]"
                />
              </Field>
              <Field label="Section">
                <input
                  type="text"
                  value={form.section}
                  onChange={(e) => setForm({ ...form, section: e.target.value })}
                  placeholder="e.g. politics"
                  className="w-full border border-black bg-white px-4 py-3 text-black outline-none focus:border-[color:var(--accent)]"
                />
              </Field>
              <Field label="Tag">
                <input
                  type="text"
                  value={form.tag}
                  onChange={(e) => setForm({ ...form, tag: e.target.value })}
                  placeholder="e.g. climate"
                  className="w-full border border-black bg-white px-4 py-3 text-black outline-none focus:border-[color:var(--accent)]"
                />
              </Field>
              <Field label="From date">
                <input
                  type="date"
                  value={form.fromDate}
                  onChange={(e) => setForm({ ...form, fromDate: e.target.value })}
                  className="w-full border border-black bg-white px-4 py-3 text-black outline-none focus:border-[color:var(--accent)]"
                />
              </Field>
              <Field label="To date">
                <input
                  type="date"
                  value={form.toDate}
                  onChange={(e) => setForm({ ...form, toDate: e.target.value })}
                  className="w-full border border-black bg-white px-4 py-3 text-black outline-none focus:border-[color:var(--accent)]"
                />
              </Field>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <button
                type="submit"
                className="inline-flex items-center justify-center border border-[color:var(--accent)] bg-[color:var(--accent)] px-5 py-3 text-sm font-semibold text-black transition hover:bg-[color:var(--accent-soft)]"
              >
                Search
              </button>
              <button
                type="button"
                onClick={onReset}
                className="inline-flex items-center justify-center border border-black bg-white px-5 py-3 text-sm font-semibold text-black transition hover:bg-[color:var(--accent-soft)]"
              >
                Reset
              </button>
              {data ? (
                <span className="text-xs text-black/60">
                  {data.total} match{data.total === 1 ? "" : "es"} · page {currentPage}/{totalPages}
                </span>
              ) : null}
            </div>
          </form>
        </section>

        {error ? (
          <section className="border border-[color:var(--status-bad)] bg-[color:var(--status-bad-soft)] px-6 py-4 text-sm text-[color:var(--status-bad)]">
            {error}
          </section>
        ) : null}

        <section className="grid gap-5 lg:grid-cols-[1.5fr_1fr]">
          <div className="border border-black bg-white">
            <div className="grid grid-cols-[2fr_0.7fr_0.9fr] gap-3 border-b border-black px-4 py-3 text-xs font-semibold uppercase tracking-[0.18em] text-black/70">
              <span>Title</span>
              <span>Section</span>
              <span>Published</span>
            </div>
            {isLoading ? (
              <div className="px-4 py-6 text-sm text-black/60">Loading articles...</div>
            ) : data && data.articles.length > 0 ? (
              data.articles.map((article) => (
                <button
                  key={article.guardian_id}
                  type="button"
                  onClick={() => setSelectedId(article.guardian_id)}
                  className={`grid w-full grid-cols-[2fr_0.7fr_0.9fr] gap-3 border-b border-black px-4 py-4 text-left text-sm text-black transition last:border-b-0 hover:bg-[color:var(--accent-soft)] ${
                    selectedId === article.guardian_id
                      ? "bg-[color:var(--accent-soft)]"
                      : ""
                  }`}
                >
                  <span className="font-medium">{article.web_title}</span>
                  <span className="text-black/70">{article.section_name ?? "—"}</span>
                  <span className="text-black/70">
                    {article.published_at.slice(0, 10)}
                  </span>
                </button>
              ))
            ) : (
              <div className="px-4 py-6 text-sm text-black/60">
                No articles matched your filters.
              </div>
            )}
          </div>

          <ArticleDetailPanel
            article={detail}
            isLoading={detailLoading}
            error={detailError}
            selected={selectedId !== null}
          />
        </section>

        <nav className="flex items-center justify-between border border-black bg-white px-6 py-4 text-sm">
          <button
            type="button"
            disabled={offset === 0 || isLoading}
            onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
            className="inline-flex items-center justify-center border border-black bg-white px-4 py-2 text-xs font-semibold uppercase tracking-wide text-black transition hover:bg-[color:var(--accent-soft)] disabled:cursor-not-allowed disabled:opacity-50"
          >
            Previous
          </button>
          <span className="text-black/70">
            Page {currentPage} of {totalPages}
          </span>
          <button
            type="button"
            disabled={!data || offset + PAGE_SIZE >= data.total || isLoading}
            onClick={() => setOffset(offset + PAGE_SIZE)}
            className="inline-flex items-center justify-center border border-black bg-white px-4 py-2 text-xs font-semibold uppercase tracking-wide text-black transition hover:bg-[color:var(--accent-soft)] disabled:cursor-not-allowed disabled:opacity-50"
          >
            Next
          </button>
        </nav>
      </div>
    </main>
  );
}

function ArticleDetailPanel({
  article,
  isLoading,
  error,
  selected,
}: {
  article: ArticleDetail | null;
  isLoading: boolean;
  error: string | null;
  selected: boolean;
}) {
  if (!selected) {
    return (
      <aside className="border border-dashed border-black bg-[color:var(--background)] px-6 py-6 text-sm text-black/70">
        Select an article to see its details here.
      </aside>
    );
  }

  if (isLoading) {
    return (
      <aside className="border border-black bg-white px-6 py-6 text-sm text-black/70">
        Loading article detail...
      </aside>
    );
  }

  if (error) {
    return (
      <aside className="border border-[color:var(--status-bad)] bg-[color:var(--status-bad-soft)] px-6 py-4 text-sm text-[color:var(--status-bad)]">
        {error}
      </aside>
    );
  }

  if (!article) {
    return (
      <aside className="border border-black bg-white px-6 py-6 text-sm text-black/70">
        Article was not found.
      </aside>
    );
  }

  return (
    <aside className="border border-black bg-white px-6 py-6">
      <p className="text-xs font-semibold uppercase tracking-[0.2em] text-black/60">
        {article.pillar_name ?? ""} · {article.section_name ?? ""}
      </p>
      <h2 className="mt-2 text-xl font-semibold tracking-[-0.02em] text-black">
        {article.web_title}
      </h2>
      <p className="mt-2 text-sm text-black/70">
        Published {article.published_at}
      </p>
      {article.web_url ? (
        <a
          href={article.web_url}
          target="_blank"
          rel="noreferrer"
          className="mt-4 inline-flex items-center border border-[color:var(--accent)] bg-[color:var(--accent)] px-4 py-2 text-xs font-semibold uppercase tracking-wide text-black transition hover:bg-[color:var(--accent-soft)]"
        >
          Open on theguardian.com
        </a>
      ) : null}
      {article.tags.length > 0 ? (
        <div className="mt-5 flex flex-wrap gap-2">
          {article.tags.map((tag) => (
            <span
              key={tag}
              className="border border-black bg-[color:var(--background)] px-3 py-1 text-xs text-black"
            >
              {tag}
            </span>
          ))}
        </div>
      ) : null}
      <p className="mt-4 text-xs text-black/60">Guardian ID: {article.guardian_id}</p>
    </aside>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="grid gap-2 text-sm font-medium text-black">
      <span>{label}</span>
      {children}
    </label>
  );
}
