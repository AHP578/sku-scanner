export default async function handler(req, res) {
  const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
  const REPO = "AHP578/sku-scanner";
  const headers = {
    Accept: "application/vnd.github.v3+json",
  };
  if (GITHUB_TOKEN) {
    headers.Authorization = `Bearer ${GITHUB_TOKEN}`;
  }

  const debug = { hasToken: !!GITHUB_TOKEN, errors: [] };

  try {
    // Fetch checkpoint.json from raw URL (contents API has 1MB limit; checkpoint exceeds it)
    const checkpointRes = await fetch(
      `https://raw.githubusercontent.com/${REPO}/master/checkpoint.json`,
      { headers: GITHUB_TOKEN ? { Authorization: `Bearer ${GITHUB_TOKEN}` } : {} }
    );

    let checkpoint = {};
    let totalSkus = 14104;

    if (checkpointRes.ok) {
      const text = await checkpointRes.text();
      try {
        checkpoint = JSON.parse(text);
      } catch (e) {
        debug.errors.push(`checkpoint parse: ${e.message} (len=${text.length})`);
      }
    } else {
      debug.errors.push(`checkpoint: ${checkpointRes.status} ${checkpointRes.statusText}`);
    }

    // Count statuses
    const stats = { MATCHED: 0, UNMATCHED: 0, ERROR: 0, SKIPPED: 0 };
    const recentLookups = [];

    for (const [barcode, result] of Object.entries(checkpoint)) {
      const status = result.STATUS || "UNKNOWN";
      if (stats[status] !== undefined) stats[status]++;

      if (status === "MATCHED" || status === "UNMATCHED") {
        recentLookups.push({
          barcode,
          status,
          name: result.FULL_NAME_FOUND || "",
          brand: result.BRAND || "",
          category: result.CATEGORY || "",
        });
      }
    }

    const completed = stats.MATCHED + stats.UNMATCHED + stats.ERROR + stats.SKIPPED;
    const remaining = totalSkus - completed;

    // Check if running — lock file (local) OR active GitHub Actions workflow
    const lockRes = await fetch(
      `https://api.github.com/repos/${REPO}/contents/running.lock`,
      { headers }
    );
    const hasLockFile = lockRes.ok;

    let actionsRunning = false;
    let runSource = null;
    const actionsRes = await fetch(
      `https://api.github.com/repos/${REPO}/actions/runs?status=in_progress&per_page=1`,
      { headers }
    );
    if (actionsRes.ok) {
      const actionsData = await actionsRes.json();
      actionsRunning = actionsData.total_count > 0;
    } else {
      debug.errors.push(`actions: ${actionsRes.status} ${actionsRes.statusText}`);
    }

    const isRunning = hasLockFile || actionsRunning;
    if (hasLockFile) runSource = "local";
    else if (actionsRunning) runSource = "github";

    // Get last commit on checkpoint.json for "last run" time
    const commitsRes = await fetch(
      `https://api.github.com/repos/${REPO}/commits?path=checkpoint.json&per_page=1`,
      { headers }
    );

    let lastRunTime = null;
    if (commitsRes.ok) {
      const commits = await commitsRes.json();
      if (commits.length > 0) {
        lastRunTime = commits[0].commit.committer.date;
      }
    } else {
      debug.errors.push(`commits: ${commitsRes.status} ${commitsRes.statusText}`);
    }

    // Calculate next scheduled run (every hour from cron)
    let nextScheduledRun = null;
    if (!isRunning) {
      const now = new Date();
      const next = new Date(now);
      next.setUTCHours(now.getUTCHours() + 1, 0, 0, 0);
      if (next <= now) next.setUTCHours(next.getUTCHours() + 1);
      nextScheduledRun = next.toISOString();
    }

    res.setHeader("Cache-Control", "s-maxage=60, stale-while-revalidate=30");
    res.status(200).json({
      totalSkus,
      completed,
      remaining,
      stats,
      isRunning,
      runSource,
      lastRunTime,
      nextScheduledRun,
      recentLookups: recentLookups.slice(-20).reverse(),
      debug,
    });
  } catch (error) {
    res.status(500).json({ error: error.message, stack: error.stack, debug });
  }
}
