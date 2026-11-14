#!/usr/bin/env bash
# One command, all CLI: creates a public GitHub repo, pushes this folder, sets Actions
# permissions, turns on GitHub Pages from /docs, and starts the Kalshi backfill.
#
#   ./setup.sh                      # repo named kalshi-monitor
#   ./setup.sh my-repo-name         # pick a name
#   BACKFILL_START=2026-01-01 ./setup.sh   # shorter backfill
#
# Needs git and the GitHub CLI (https://cli.github.com), logged in with `gh auth login`.
set -euo pipefail

REPO_NAME="${1:-kalshi-monitor}"
BACKFILL_START="${BACKFILL_START:-2025-01-01}"
cd "$(dirname "$0")"

say() { printf '\n==> %s\n' "$*"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing $1. $2"; exit 1; }; }

need git "Install git first."
need gh  "Install the GitHub CLI from https://cli.github.com, then run: gh auth login"
gh auth status >/dev/null 2>&1 || { echo "Not logged in. Run: gh auth login"; exit 1; }

# Pushing files under .github/workflows needs the 'workflow' scope on the gh token.
if ! gh auth status 2>&1 | grep -q "'workflow'"; then
  say "Adding the 'workflow' scope to your gh login (needed to push the workflow file)"
  gh auth refresh -h github.com -s workflow
fi

OWNER="$(gh api user --jq .login)"
REPO="$OWNER/$REPO_NAME"

say "Checking that no deck figures are about to be published"
if git ls-files --others --cached --exclude-standard 2>/dev/null | grep -qi deck \
   || ls -1 | grep -qi deck; then
  echo "A file with 'deck' in its name is in this folder. Move it out before publishing."; exit 1
fi

say "Committing this folder"
[ -d .git ] || git init -q
git config user.email >/dev/null 2>&1 || git config user.email "$OWNER@users.noreply.github.com"
git config user.name  >/dev/null 2>&1 || git config user.name "$OWNER"
git add -A
git diff --cached --quiet || git commit -q -m "Kalshi volume monitor"
git branch -M main

if gh repo view "$REPO" >/dev/null 2>&1; then
  say "Repo $REPO exists; pushing to it"
  git remote get-url origin >/dev/null 2>&1 || git remote add origin "https://github.com/$REPO.git"
  git push -u origin main
else
  say "Creating public repo $REPO"
  gh repo create "$REPO" --public --source . --remote origin --push \
    --description "Kalshi volume, fees and share, rebuilt nightly from Kalshi's public trade tape"
fi

say "Letting workflows write to the repo"
gh api -X PUT "repos/$REPO/actions/permissions/workflow" \
  -f default_workflow_permissions=write -F can_approve_pull_request_reviews=false >/dev/null \
  || echo "Couldn't change the default (the workflow requests write access itself, so this is optional)."

say "Turning on GitHub Pages from main /docs"
PAGES='{"source":{"branch":"main","path":"/docs"}}'
if gh api "repos/$REPO/pages" >/dev/null 2>&1; then
  gh api -X PUT "repos/$REPO/pages" --input - <<<"$PAGES" >/dev/null
else
  gh api -X POST "repos/$REPO/pages" --input - <<<"$PAGES" >/dev/null
fi

say "Starting the backfill from $BACKFILL_START"
started=""
for _ in $(seq 1 12); do            # GitHub can take a few seconds to register a new workflow
  if gh workflow run refresh.yml -R "$REPO" -f start="$BACKFILL_START" >/dev/null 2>&1; then
    started=1; break
  fi
  sleep 5
done
[ -n "$started" ] || { echo "Couldn't start the workflow yet. Try: gh workflow run refresh.yml -R $REPO -f start=$BACKFILL_START"; exit 1; }

URL="$(gh api "repos/$REPO/pages" --jq .html_url 2>/dev/null || echo "https://$OWNER.github.io/$REPO_NAME/")"
cat <<EOF

Done.
  Site:      $URL   (first Pages build takes a minute or two)
  Backfill:  runs in ~5.5-hour chunks and restarts itself until every day is fetched
  Watch:     gh run watch -R $REPO
  History:   gh run list -R $REPO --workflow refresh.yml
EOF
