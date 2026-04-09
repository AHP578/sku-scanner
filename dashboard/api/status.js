export default async function handler(req, res) {
  const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
  const REPO = "AHP578/sku-scanner";
  const headers = {
    Authorization: `Bearer ${GITHUB_TOKEN}`,
    Accept: "application/vnd.github.v3+json",
  };

  try {
    // Fetch checkpoint.json contents
    const checkpointRes = await fetch(
      `https://api.github.com/repos/${REPO}/contents/checkpoint.json`,
      { headers }
    );

    let checkpoint = {};
    let totalSkus = 3273;

    if (checkpointRes.ok) {
      const data = await checkpointRes.json();
      const content = Buffer.from(data.content, "base64").toString("utf-8");
      checkpoint = JSON.parse(content);
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

    const actionsRes = await fetch(
      `https://api.github.com/repos/${REPO}/actions/runs?status=in_progress&per_page=1`,
      { headers }
    );
    let actionsRunning = false;
    let runSource = null;
    if (actionsRes.ok) {
      const actionsData = await actionsRes.json();
      actionsRunning = actionsData.total_count > 0;
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
    }

    // Calculate next scheduled run (every 3 hours from cron)
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
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}
