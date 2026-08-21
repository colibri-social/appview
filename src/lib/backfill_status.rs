//! Backfill visibility: telling the operator whether repos are still being
//! backfilled.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rocket::tokio::time::{MissedTickBehavior, interval};
use sea_orm::sea_query::ExprTrait;
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, sea_query,
};

use crate::models::repos;

/// Records ingested from tap that it flagged historical (`live: false`), i.e.
/// backfill traffic.
static BACKFILL_RECORDS: AtomicU64 = AtomicU64::new(0);
/// Records ingested from tap's live firehose (`live: true`).
static LIVE_RECORDS: AtomicU64 = AtomicU64::new(0);

/// Records one record ingested from tap. Called by the tap dispatcher on the hot
/// path, so it does exactly one relaxed increment and no allocation.
pub fn note_record(live: bool) {
    let counter = if live {
        &LIVE_RECORDS
    } else {
        &BACKFILL_RECORDS
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Cumulative `(backfill, live)` record counts since process start.
fn record_totals() -> (u64, u64) {
    (
        BACKFILL_RECORDS.load(Ordering::Relaxed),
        LIVE_RECORDS.load(Ordering::Relaxed),
    )
}

/// Default reporting interval. Matches [`crate::lib::hum_client`]'s reconcile
/// cadence: long enough that the aggregate query is free, short enough that a
/// multi-minute backfill produces several progress lines.
const DEFAULT_REPORT_SECS: u64 = 30;

/// How many not-caught-up repos the debug sample names.
const OUTSTANDING_SAMPLE: u64 = 20;

/// Reporting interval from `BACKFILL_REPORT_SECS`. `0` disables reporting
/// entirely, which the spawn site in `main.rs` handles by not spawning the
/// reporter at all, hence this deliberately doesn't filter out `0` the way
/// `tap_worker_count` does.
pub fn report_interval_secs() -> u64 {
    std::env::var("BACKFILL_REPORT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_REPORT_SECS)
}

/// What one of tap's `repos.state` values means for backfill progress.
#[derive(Debug, PartialEq, Eq)]
enum RepoState {
    /// Backfill queued, needed, or actively running (`pending`,
    /// `desynchronized`, `resyncing`).
    Waiting,
    /// Backfilled and following the live firehose (`active`).
    Ready,
    /// Backfill failed (`error`). Tap retries on a growing `retry_after` backoff
    /// but never gives up, so this is deliberately *not* treated as in-progress
    Errored,
    /// A state this build doesn't know about. Counted as [`Self::Waiting`] so a
    /// tap schema change degrades into "still working" rather than a wrong
    /// "concluded", and echoed verbatim into the log line.
    Unknown,
}

fn classify_state(state: &str) -> RepoState {
    match state.trim().to_ascii_lowercase().as_str() {
        "pending" | "desynchronized" | "resyncing" => RepoState::Waiting,
        "active" => RepoState::Ready,
        "error" => RepoState::Errored,
        _ => RepoState::Unknown,
    }
}

pub async fn repo_caught_up(db: &DatabaseConnection, did: &str) -> bool {
    match repos::Entity::find_by_id(did.to_string()).one(db).await {
        Ok(Some(repo)) => classify_state(&repo.state) != RepoState::Waiting,
        Ok(None) => true,
        Err(e) => {
            log::warn!("backfill state lookup failed for {did}: {e}");
            true
        }
    }
}

#[derive(FromQueryResult)]
struct StateCount {
    state: String,
    /// How many times tap has already failed on these repos
    retry_count: i64,
    repo_count: i64,
}

impl StateCount {
    /// Whether tap has already failed on these repos at least once.
    fn retrying(&self) -> bool {
        self.retry_count > 0
    }
}

/// The repos tap tracks, bucketed by what their state means for backfill.
#[derive(Debug, Default, PartialEq, Eq)]
struct RepoSnapshot {
    /// Awaiting (or undergoing) a first-attempt backfill.
    waiting: u64,
    /// Caught up and following the live firehose.
    ready: u64,
    /// Failing: in `error`, or being retried after an earlier failure. Reported
    /// but never counted as progress.
    stalled: u64,
    /// `(state, count)` for every state string this build doesn't recognise,
    /// sorted so the log line is stable tick to tick.
    unknown: Vec<(String, u64)>,
}

impl RepoSnapshot {
    fn from_rows(rows: Vec<StateCount>) -> Self {
        let mut snapshot = Self::default();
        for row in rows {
            // `std::cmp::max` rather than the method: `ExprTrait` is in scope for
            // the query below and shadows `Ord::max`. Same workaround as
            // `notifications::unseen_counts_for_channels`.
            let count = std::cmp::max(row.repo_count, 0) as u64;
            let state = classify_state(&row.state);

            // A repo tap has already failed on is churn, not progress, whichever
            // half of the retry cycle this tick catches it in.
            if state == RepoState::Errored || (row.retrying() && state != RepoState::Ready) {
                snapshot.stalled += count;
                continue;
            }

            match state {
                RepoState::Waiting => snapshot.waiting += count,
                RepoState::Ready => snapshot.ready += count,
                RepoState::Unknown => snapshot.unknown.push((row.state, count)),
                RepoState::Errored => unreachable!("handled above"),
            }
        }
        snapshot.unknown.sort();
        snapshot
    }

    /// Repos not yet caught up: first-attempt backfills plus anything in a state
    /// this build doesn't recognise. Excludes [`Self::stalled`].
    fn outstanding(&self) -> u64 {
        self.waiting + self.unknown.iter().map(|(_, n)| n).sum::<u64>()
    }

    /// The unrecognised-state suffix, empty when every state was recognised.
    fn unknown_suffix(&self) -> String {
        if self.unknown.is_empty() {
            return String::new();
        }
        let states = self
            .unknown
            .iter()
            .map(|(state, count)| format!("{state}={count}"))
            .collect::<Vec<_>>()
            .join(" ");
        format!(", unrecognised tap repo state(s): {states} (counted as awaiting backfill)")
    }

    /// The failing-repo suffix, empty when no repo is stalled.
    fn stalled_suffix(&self) -> String {
        if self.stalled == 0 {
            return String::new();
        }
        format!(
            ", {} repo(s) failing backfill (tap retries with backoff)",
            self.stalled
        )
    }
}

/// Runs `SELECT state, retry_count, count(did) FROM repos GROUP BY state, retry_count`
/// against tap's table. Grouping on the raw `retry_count` yields a handful of extra
/// rows over grouping on `retry_count > 0`, and unlike the expression form it
/// produces SQL Postgres accepts.
async fn repo_snapshot(db: &DatabaseConnection) -> Result<RepoSnapshot, DbErr> {
    let rows = repos::Entity::find()
        .select_only()
        .column(repos::Column::State)
        .column(repos::Column::RetryCount)
        .column_as(
            sea_query::Expr::col(repos::Column::Did).count(),
            "repo_count",
        )
        .group_by(repos::Column::State)
        .group_by(repos::Column::RetryCount)
        .into_model::<StateCount>()
        .all(db)
        .await?;

    Ok(RepoSnapshot::from_rows(rows))
}

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    Running,
}

/// What a single tick should say.
#[derive(Debug, PartialEq, Eq)]
enum Report {
    /// Idle, and idle last tick too. Say nothing, so a caught-up instance's
    /// logs stay quiet.
    Silent,
    /// Idle -> running.
    Started,
    /// The first observed tick was already mid-backfill (a restart, or a backfill
    /// that predates this process), so it did not start here.
    AlreadyRunning,
    /// Running -> running.
    Ongoing,
    /// Running -> idle.
    Concluded,
}

struct Reporter {
    phase: Phase,
    /// Whether any tick has been observed; distinguishes [`Report::Started`] from
    /// [`Report::AlreadyRunning`].
    observed: bool,
    started_at: Option<Instant>,
    /// Backfill records ingested since the current run began.
    records_this_run: u64,
    /// Cumulative counter values as of the last *processed* tick
    last_backfill_total: u64,
    last_live_total: u64,
    /// Errored-repo count as of the last tick that mentioned it, so an idle
    /// instance still surfaces a change without logging every interval.
    last_reported_stalled: u64,
}

impl Reporter {
    fn new() -> Self {
        Self {
            phase: Phase::Idle,
            observed: false,
            started_at: None,
            records_this_run: 0,
            last_backfill_total: 0,
            last_live_total: 0,
            last_reported_stalled: 0,
        }
    }

    /// Advances one tick. Backfill is happening if repos are outstanding *or*
    /// backfill records arrived during this window
    fn step(&mut self, outstanding: u64, records: u64, now: Instant) -> Report {
        let busy = outstanding > 0 || records > 0;
        let first = !self.observed;
        self.observed = true;

        match (&self.phase, busy) {
            (Phase::Idle, false) => Report::Silent,
            (Phase::Idle, true) => {
                self.phase = Phase::Running;
                self.started_at = Some(now);
                self.records_this_run = records;
                if first {
                    Report::AlreadyRunning
                } else {
                    Report::Started
                }
            }
            (Phase::Running, true) => {
                self.records_this_run += records;
                Report::Ongoing
            }
            (Phase::Running, false) => {
                self.phase = Phase::Idle;
                Report::Concluded
            }
        }
    }

    /// How long the current (or just-concluded) run has been going. `started_at`
    /// is intentionally not cleared on [`Report::Concluded`] so the caller can
    /// still read this after [`Self::step`] returns.
    fn elapsed(&self, now: Instant) -> Duration {
        self.started_at
            .map(|start| now.duration_since(start))
            .unwrap_or_default()
    }
}

/// Renders the operator-facing line for a tick, or `None` when the tick is
/// silent. Kept separate from the logging call so the exact wording is
/// assertable without a database or log capture.
fn format_report(
    report: &Report,
    snapshot: &RepoSnapshot,
    records: u64,
    records_this_run: u64,
    window: Duration,
    elapsed: Duration,
) -> Option<String> {
    let head = match report {
        Report::Silent => return None,
        Report::Started => format!(
            "backfill started: {} repo(s) awaiting backfill, {} caught up",
            snapshot.outstanding(),
            snapshot.ready
        ),
        Report::AlreadyRunning => format!(
            "backfill already in progress at startup: {} repo(s) awaiting backfill, {} caught up",
            snapshot.outstanding(),
            snapshot.ready
        ),
        Report::Ongoing => format!(
            "backfill in progress: {} repo(s) awaiting backfill, {} caught up, {records} record(s) in the last {}, running for {}",
            snapshot.outstanding(),
            snapshot.ready,
            format_elapsed(window),
            format_elapsed(elapsed)
        ),
        Report::Concluded => format!(
            "backfill concluded: all {} repo(s) caught up after {} and {records_this_run} backfilled record(s)",
            snapshot.ready,
            format_elapsed(elapsed)
        ),
    };

    Some(format!(
        "{head}{}{}",
        snapshot.stalled_suffix(),
        snapshot.unknown_suffix()
    ))
}

/// Compact human duration: `45s`, `4m12s`, `1h03m`.
fn format_elapsed(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Polls tap's `repos` table on an interval and logs when backfilling starts,
/// while it is ongoing, and when it concludes. Silent while every repo is caught
/// up. Never returns; spawned once from `main.rs`.
pub async fn run_backfill_reporter(db: DatabaseConnection) {
    // `.max(1)`: `interval` panics on a zero period, and `BACKFILL_REPORT_SECS=0`
    // (disabled) is handled by the spawn site not spawning this at all.
    let window = Duration::from_secs(std::cmp::max(report_interval_secs(), 1));
    let mut ticker = interval(window);
    // A slow tick must not be followed by an immediate catch-up tick: the
    // progress line reports records *per window*, and tokio's default `Burst`
    // would bill a full window's records to a near-zero window.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut reporter = Reporter::new();
    let mut failures: u32 = 0;

    loop {
        ticker.tick().await;

        // Query before touching the counters: a failed tick must neither report a
        // false "concluded" nor consume a window's records.
        let snapshot = match repo_snapshot(&db).await {
            Ok(snapshot) => {
                if failures > 0 {
                    log::info!(
                        "backfill reporter: repos query recovered after {failures} failed tick(s)"
                    );
                    failures = 0;
                }
                snapshot
            }
            Err(e) => {
                failures += 1;
                if failures == 1 {
                    log::warn!(
                        "backfill reporter: repos query failed: {e} (tap owns that table and may \
                         not have created it yet; further consecutive failures log at debug)"
                    );
                } else {
                    log::debug!("backfill reporter: repos query failed ({failures} in a row): {e}");
                }
                continue;
            }
        };

        let (backfill_total, live_total) = record_totals();
        let records = backfill_total.saturating_sub(reporter.last_backfill_total);
        let live = live_total.saturating_sub(reporter.last_live_total);
        reporter.last_backfill_total = backfill_total;
        reporter.last_live_total = live_total;

        let now = Instant::now();
        let report = reporter.step(snapshot.outstanding(), records, now);
        let elapsed = reporter.elapsed(now);

        if let Some(line) = format_report(
            &report,
            &snapshot,
            records,
            reporter.records_this_run,
            window,
            elapsed,
        ) {
            log::info!("{line}");
        } else if snapshot.stalled != reporter.last_reported_stalled {
            // Otherwise a caught-up instance never mentions permanently-errored
            // repos at all. Only fires on change, so it can't spam.
            log::warn!(
                "backfill: {} repo(s) in error and not caught up (tap retries with backoff)",
                snapshot.stalled
            );
        }
        reporter.last_reported_stalled = snapshot.stalled;

        log::debug!(
            "backfill reporter: outstanding={} ready={} stalled={} backfill_records={records} live_records={live}",
            snapshot.outstanding(),
            snapshot.ready,
            snapshot.stalled
        );

        // Which repos are holding things up. Gated on debug being enabled so the
        // extra query costs nothing in production.
        if !matches!(report, Report::Silent | Report::Concluded)
            && log::log_enabled!(log::Level::Debug)
        {
            log_outstanding_repos(&db).await;
        }
    }
}

#[derive(FromQueryResult)]
struct OutstandingRepo {
    did: String,
    state: String,
    retry_count: i64,
    error_msg: Option<String>,
}

/// Names up to [`OUTSTANDING_SAMPLE`] repos that aren't caught up. `select_only`
/// so the sample never drags `rev`/`prev_data` along, and filtered on the leading
/// column of `idx_repos_state_retry`. Diagnostic only: its own failures are
/// swallowed at debug.
async fn log_outstanding_repos(db: &DatabaseConnection) {
    let rows = repos::Entity::find()
        .select_only()
        .column(repos::Column::Did)
        .column(repos::Column::State)
        .column(repos::Column::RetryCount)
        .column(repos::Column::ErrorMsg)
        .filter(repos::Column::State.ne("active"))
        .order_by_asc(repos::Column::Did)
        .limit(OUTSTANDING_SAMPLE)
        .into_model::<OutstandingRepo>()
        .all(db)
        .await;

    match rows {
        Ok(rows) => {
            for row in rows {
                log::debug!(
                    "backfill outstanding: {} state={} retries={} error={}",
                    row.did,
                    row.state,
                    row.retry_count,
                    row.error_msg.as_deref().unwrap_or("-")
                );
            }
        }
        Err(e) => log::debug!("backfill reporter: outstanding-repo sample failed: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::tokio;
    use sea_orm::{DatabaseBackend, MockDatabase};

    fn repo_row(state: &str) -> repos::Model {
        repos::Model {
            did: String::from("did:plc:community"),
            state: state.to_string(),
            status: String::from("ok"),
            handle: None,
            rev: None,
            prev_data: None,
            error_msg: None,
            retry_count: 0,
            retry_after: 0,
        }
    }

    #[tokio::test]
    async fn a_repo_still_backfilling_is_not_caught_up() {
        for state in ["pending", "resyncing", "desynchronized"] {
            let db = MockDatabase::new(DatabaseBackend::Postgres)
                .append_query_results([vec![repo_row(state)]])
                .into_connection();

            assert!(!repo_caught_up(&db, "did:plc:community").await, "{state}");
        }
    }

    #[tokio::test]
    async fn an_active_repo_is_caught_up() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([vec![repo_row("active")]])
            .into_connection();

        assert!(repo_caught_up(&db, "did:plc:community").await);
    }

    #[tokio::test]
    async fn an_untracked_repo_does_not_block() {
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([Vec::<repos::Model>::new()])
            .into_connection();

        assert!(repo_caught_up(&db, "did:plc:community").await);
    }

    /// A first-attempt group: `retry_count = 0`.
    fn row(state: &str, count: i64) -> StateCount {
        StateCount {
            state: state.to_string(),
            retry_count: 0,
            repo_count: count,
        }
    }

    /// A group tap has already failed on at least once.
    fn retry_row(state: &str, count: i64) -> StateCount {
        StateCount {
            state: state.to_string(),
            retry_count: 3,
            repo_count: count,
        }
    }

    fn snapshot(waiting: u64, ready: u64, stalled: u64) -> RepoSnapshot {
        RepoSnapshot {
            waiting,
            ready,
            stalled,
            unknown: Vec::new(),
        }
    }

    #[test]
    fn classify_state_buckets_taps_states() {
        assert_eq!(classify_state("pending"), RepoState::Waiting);
        assert_eq!(classify_state("desynchronized"), RepoState::Waiting);
        assert_eq!(classify_state("resyncing"), RepoState::Waiting);
        assert_eq!(classify_state("active"), RepoState::Ready);
        assert_eq!(classify_state("error"), RepoState::Errored);
        // Tolerant of whitespace/casing drift in tap's column.
        assert_eq!(classify_state("ACTIVE "), RepoState::Ready);
        // Anything new tap might introduce.
        assert_eq!(classify_state("tombstoned"), RepoState::Unknown);
        assert_eq!(classify_state(""), RepoState::Unknown);
    }

    #[test]
    fn snapshot_counts_unknown_states_as_outstanding_but_not_failures() {
        let snapshot = RepoSnapshot::from_rows(vec![
            row("active", 519),
            row("pending", 8),
            row("desynchronized", 4),
            row("error", 14),
            row("brand-new", 1),
        ]);

        assert_eq!(snapshot.waiting, 12);
        assert_eq!(snapshot.ready, 519);
        assert_eq!(snapshot.stalled, 14);
        // Unknown states count as still working, failing ones never do.
        assert_eq!(snapshot.outstanding(), 13);
        // Sorted, so the log line doesn't reshuffle between ticks.
        assert_eq!(snapshot.unknown, vec![("brand-new".to_string(), 1)]);
    }

    #[test]
    fn snapshot_treats_a_repo_under_retry_as_stalled_not_progress() {
        // Tap cycles a failing repo error -> resyncing -> error. Whichever half a
        // tick catches, the count must stay put, or the reporter flaps between
        // "started" and "concluded" forever on a stuck repo.
        let mid_retry = RepoSnapshot::from_rows(vec![
            row("active", 531),
            retry_row("error", 13),
            retry_row("resyncing", 1),
        ]);
        let between_retries =
            RepoSnapshot::from_rows(vec![row("active", 531), retry_row("error", 14)]);

        assert_eq!(mid_retry.outstanding(), 0);
        assert_eq!(between_retries.outstanding(), 0);
        assert_eq!(mid_retry.stalled, 14);
        assert_eq!(between_retries.stalled, 14);
        assert_eq!(mid_retry.stalled_suffix(), between_retries.stalled_suffix());
    }

    #[test]
    fn snapshot_still_counts_a_first_attempt_resync_as_outstanding() {
        // A genuine new backfill (`retry_count = 0`) in flight *is* progress.
        let snapshot = RepoSnapshot::from_rows(vec![row("active", 400), row("resyncing", 2)]);
        assert_eq!(snapshot.outstanding(), 2);
        assert_eq!(snapshot.stalled, 0);
    }

    #[test]
    fn snapshot_clamps_negative_counts() {
        let snapshot = RepoSnapshot::from_rows(vec![row("pending", -1)]);
        assert_eq!(snapshot.waiting, 0);
        assert_eq!(snapshot.outstanding(), 0);
    }

    #[test]
    fn reporter_walks_idle_started_ongoing_concluded() {
        let mut reporter = Reporter::new();
        let now = Instant::now();

        assert_eq!(reporter.step(0, 0, now), Report::Silent);
        assert_eq!(reporter.step(5, 0, now), Report::Started);
        assert_eq!(reporter.step(3, 120, now), Report::Ongoing);
        assert_eq!(reporter.step(0, 0, now), Report::Concluded);
        assert_eq!(reporter.step(0, 0, now), Report::Silent);
    }

    #[test]
    fn reporter_flags_a_backfill_already_running_at_startup() {
        let mut reporter = Reporter::new();
        let now = Instant::now();

        assert_eq!(reporter.step(12, 0, now), Report::AlreadyRunning);
        assert_eq!(reporter.step(9, 0, now), Report::Ongoing);
    }

    #[test]
    fn reporter_surfaces_a_backfill_that_finished_between_ticks() {
        let mut reporter = Reporter::new();
        let now = Instant::now();

        // No repo is ever seen outstanding
        assert_eq!(reporter.step(0, 0, now), Report::Silent);
        assert_eq!(reporter.step(0, 500, now), Report::Started);
        assert_eq!(reporter.step(0, 0, now), Report::Concluded);
    }

    #[test]
    fn reporter_never_latches_on_failing_repos() {
        let mut reporter = Reporter::new();
        let now = Instant::now();
        let snapshot = snapshot(0, 531, 14);

        for _ in 0..3 {
            assert_eq!(
                reporter.step(snapshot.outstanding(), 0, now),
                Report::Silent
            );
        }
    }

    #[test]
    fn reporter_accumulates_records_across_a_run() {
        let mut reporter = Reporter::new();
        let now = Instant::now();

        reporter.step(0, 0, now);
        reporter.step(4, 100, now);
        reporter.step(2, 250, now);
        assert_eq!(reporter.records_this_run, 350);

        reporter.step(0, 0, now);
        reporter.step(1, 7, now);
        assert_eq!(reporter.records_this_run, 7);
    }

    #[test]
    fn format_report_renders_each_phase() {
        let clean = snapshot(12, 519, 0);
        let window = Duration::from_secs(30);

        assert_eq!(
            format_report(&Report::Silent, &clean, 0, 0, window, Duration::ZERO),
            None
        );
        assert_eq!(
            format_report(&Report::Started, &clean, 0, 0, window, Duration::ZERO).unwrap(),
            "backfill started: 12 repo(s) awaiting backfill, 519 caught up"
        );
        assert_eq!(
            format_report(
                &Report::AlreadyRunning,
                &clean,
                0,
                0,
                window,
                Duration::ZERO
            )
            .unwrap(),
            "backfill already in progress at startup: 12 repo(s) awaiting backfill, 519 caught up"
        );
        assert_eq!(
            format_report(
                &Report::Ongoing,
                &snapshot(7, 524, 0),
                1483,
                18422,
                window,
                Duration::from_secs(150)
            )
            .unwrap(),
            "backfill in progress: 7 repo(s) awaiting backfill, 524 caught up, 1483 record(s) in the last 30s, running for 2m30s"
        );
        assert_eq!(
            format_report(
                &Report::Concluded,
                &snapshot(0, 531, 0),
                0,
                18422,
                window,
                Duration::from_secs(252)
            )
            .unwrap(),
            "backfill concluded: all 531 repo(s) caught up after 4m12s and 18422 backfilled record(s)"
        );
    }

    #[test]
    fn format_report_appends_failure_and_unknown_suffixes() {
        let messy = RepoSnapshot {
            waiting: 8,
            ready: 519,
            stalled: 14,
            unknown: vec![("tombstoned".to_string(), 3)],
        };

        assert_eq!(
            format_report(
                &Report::Started,
                &messy,
                0,
                0,
                Duration::from_secs(30),
                Duration::ZERO
            )
            .unwrap(),
            "backfill started: 11 repo(s) awaiting backfill, 519 caught up, \
             14 repo(s) failing backfill (tap retries with backoff), \
             unrecognised tap repo state(s): tombstoned=3 (counted as awaiting backfill)"
        );
    }

    #[test]
    fn format_elapsed_scales_by_magnitude() {
        assert_eq!(format_elapsed(Duration::from_secs(0)), "0s");
        assert_eq!(format_elapsed(Duration::from_secs(45)), "45s");
        assert_eq!(format_elapsed(Duration::from_secs(60)), "1m00s");
        assert_eq!(format_elapsed(Duration::from_secs(252)), "4m12s");
        assert_eq!(format_elapsed(Duration::from_secs(3600)), "1h00m");
        assert_eq!(format_elapsed(Duration::from_secs(3780)), "1h03m");
    }

    #[tokio::test]
    async fn repo_snapshot_groups_by_state_and_retry() {
        use std::collections::BTreeMap;

        let rows: Vec<BTreeMap<&str, sea_orm::Value>> = vec![
            BTreeMap::from([
                ("state", sea_orm::Value::from("active")),
                ("retry_count", sea_orm::Value::from(0i64)),
                ("repo_count", sea_orm::Value::from(519i64)),
            ]),
            BTreeMap::from([
                ("state", sea_orm::Value::from("pending")),
                ("retry_count", sea_orm::Value::from(0i64)),
                ("repo_count", sea_orm::Value::from(12i64)),
            ]),
            BTreeMap::from([
                ("state", sea_orm::Value::from("error")),
                ("retry_count", sea_orm::Value::from(4i64)),
                ("repo_count", sea_orm::Value::from(14i64)),
            ]),
        ];
        let db = MockDatabase::new(DatabaseBackend::Postgres)
            .append_query_results([rows])
            .into_connection();

        let snapshot = repo_snapshot(&db).await.unwrap();

        assert_eq!(snapshot.ready, 519);
        assert_eq!(snapshot.waiting, 12);
        assert_eq!(snapshot.stalled, 14);
        assert_eq!(snapshot.outstanding(), 12);

        let stmt = format!("{:?}", db.into_transaction_log()[0]);
        assert!(
            stmt.contains("GROUP BY"),
            "expected a grouped query: {stmt}"
        );
    }
}
