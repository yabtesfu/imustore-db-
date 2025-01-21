#!/bin/bash

# Usage:
# ./push_project.sh [project_dir]

PROJECT_DIR="${1:-.}"
REPO_URL="https://github.com/yabtesfu/imustore-db-.git"

if [ ! -d "$PROJECT_DIR" ]; then
  echo "Project directory not found: $PROJECT_DIR"
  exit 1
fi

cd "$PROJECT_DIR" || { echo "Failed to cd into $PROJECT_DIR"; exit 1; }

if [ ! -d ".git" ]; then
  git init
fi
git branch -M main

git config user.name "yabtesfu"
git config user.email "yabtesfu@gmail.com"

START_DATE="2025-01-21"
END_DATE="2026-07-08"

HOLIDAYS=(
  "2025-02-17" "2025-05-26" "2025-06-19" "2025-07-04"
  "2025-09-01" "2025-10-13" "2025-11-11" "2025-11-27"
  "2025-12-25" "2026-01-01" "2026-01-19" "2026-02-16"
  "2026-05-25" "2026-06-19" "2026-07-03"
)

WEEKEND_DAYS=(
  "2025-02-08" "2025-03-16" "2025-04-12" "2025-05-18"
  "2025-06-14" "2025-07-19" "2025-08-23" "2025-09-14"
  "2025-10-25" "2025-11-22" "2025-12-06" "2026-01-10"
  "2026-02-28" "2026-03-22" "2026-04-18" "2026-05-31"
  "2026-06-27"
)

MESSAGES=(
  "Add append-only storage layer"
  "Implement immutable tree index"
  "Add lazy value references"
  "Wire atomic root commit"
  "Add JSON codec support"
  "Build dictionary-style API"
  "Add CLI database commands"
  "Track storage statistics"
  "Implement database compaction"
  "Add prefix scan support"
  "Add range scan support"
  "Add transaction context"
  "Add integrity audit report"
  "Add update helper"
  "Document storage format"
  "Expand architecture notes"
  "Cover binary tree deletion"
  "Add CLI regression tests"
  "Improve file lock handling"
  "Polish README examples"
)

is_holiday() {
  local day="$1"
  for holiday in "${HOLIDAYS[@]}"; do
    if [ "$day" = "$holiday" ]; then
      return 0
    fi
  done
  return 1
}

is_weekend_session() {
  local day="$1"
  for weekend_day in "${WEEKEND_DAYS[@]}"; do
    if [ "$day" = "$weekend_day" ]; then
      return 0
    fi
  done
  return 1
}

next_day() {
  date -j -v+1d -f "%Y-%m-%d" "$1" "+%Y-%m-%d" 2>/dev/null ||
    date -d "$1 +1 day" "+%Y-%m-%d"
}

day_of_week() {
  date -j -f "%Y-%m-%d" "$1" "+%u" 2>/dev/null ||
    date -d "$1" "+%u"
}

commits_for_day() {
  case "$1" in
    "2025-02-03"|"2025-04-16"|"2025-07-22"|"2025-11-03"|"2026-02-24"|"2026-06-10") echo $((RANDOM % 3 + 5)) ;;
    "2025-03-10"|"2025-09-23"|"2025-12-16"|"2026-04-07") echo $((RANDOM % 2 + 4)) ;;
    *) echo 0 ;;
  esac
}

CURRENT="$START_DATE"
END_NEXT=$(next_day "$END_DATE")

while [ "$CURRENT" != "$END_NEXT" ]; do
  DOW=$(day_of_week "$CURRENT")

  if is_holiday "$CURRENT"; then
    CURRENT=$(next_day "$CURRENT")
    continue
  fi

  if { [ "$DOW" = "6" ] || [ "$DOW" = "7" ]; } && ! is_weekend_session "$CURRENT"; then
    CURRENT=$(next_day "$CURRENT")
    continue
  fi

  BURST=$(commits_for_day "$CURRENT")
  if [ "$BURST" -gt 0 ]; then
    COMMITS_TODAY="$BURST"
  elif is_weekend_session "$CURRENT"; then
    COMMITS_TODAY=1
  else
    if [ $((RANDOM % 100)) -lt 10 ]; then
      CURRENT=$(next_day "$CURRENT")
      continue
    fi

    if [ $((RANDOM % 100)) -lt 10 ]; then
      COMMITS_TODAY=3
    elif [ $((RANDOM % 100)) -lt 42 ]; then
      COMMITS_TODAY=2
    else
      COMMITS_TODAY=1
    fi
  fi

  for ((i=0; i<COMMITS_TODAY; i++)); do
    HOUR=$(printf "%02d" $((RANDOM % 10 + 8)))
    MINUTE=$(printf "%02d" $((RANDOM % 60)))
    SECOND=$(printf "%02d" $((RANDOM % 60)))
    COMMIT_DATE="${CURRENT}T${HOUR}:${MINUTE}:${SECOND}+03:00"
    MSG="${MESSAGES[$((RANDOM % ${#MESSAGES[@]}))]}"

    echo "${COMMIT_DATE} - ${MSG}" >> history.txt

    git add .
    GIT_AUTHOR_DATE="$COMMIT_DATE" GIT_COMMITTER_DATE="$COMMIT_DATE" \
      git commit --allow-empty -m "$MSG"
  done

  CURRENT=$(next_day "$CURRENT")
done

if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$REPO_URL"
else
  git remote add origin "$REPO_URL"
fi

git push -u origin main
