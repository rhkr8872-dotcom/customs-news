
# 7.run_gti_pipeline.py
# -*- coding: utf-8 -*-
# GTI FINAL CORE v6 - Full pipeline
"""
GTI LAW1/NEWSREST split pipeline.

Flow:
1. Site crawler creates 1-1 official regulation only; non-LAW1 rows become news/reference candidates.
2. Naver, Google, RSS collectors create external news raw files.
3. Separate merge jobs build regulation and news summaries.
4. Separate AI analysis job

s build regulation and news analysis files.
5. A combined mail input is generated and passed to the mail engine.
"""

from __future__ import annotations

import argparse
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd


BASE_DIR = Path(os.getenv("GTI_BASE_DIR", r"C:\Temp"))
PYTHON_EXE = Path(os.getenv("GTI_PYTHON_EXE", r"C:\Users\KCH\AppData\Local\Programs\Python\Python312\python.exe"))

LOG_DIR = BASE_DIR / "logs"
ARCHIVE_DIR = BASE_DIR / "archive"
LOG_FILE = LOG_DIR / "gti_pipeline_run.log"

MAIL_INPUT_FILE = BASE_DIR / "4.gti_mail_input.xlsx"
MAIL_OUTPUT_DIR = BASE_DIR / "12345" / "c_type_outputs"

# Pipeline-level defaults. Individual scripts still allow explicit environment overrides.
PIPELINE_ENV_DEFAULTS = {
    # Core rule: daily news and normal regulation sensing are based on the latest 24 hours.
    # Official regulation protection/review logic remains inside Step1/Step4 so important law items are not lost.
    "GTI_STEP1_HOURS_BACK": "24",
    "GTI_LOOKBACK_HOURS": "24",
    "GTI_STEP3_RECENT_HOURS": "24",
    "GTI_STEP4_NEWS_MAX_AGE_HOURS": "24",
    "GTI_MAIL_MAX_AGE_DAYS": "45",
    "GTI_MAIL_MAX_AGE_DAYS_REG": "45",
    "GTI_MAIL_MAX_AGE_DAYS_NEWS": "1",
    "GTI_GEMINI_MODEL": "gemini-2.5-flash-lite",
    "GTI_GEMINI_TIMEOUT": "20",
    "GTI_ARTICLE_FETCH_TIMEOUT": "12",
    "GTI_RSS_FETCH_TIMEOUT": "15",
    # News must be Samsung customs-impact Top 30, with customs/trade signal in the original title.
    "GTI_STEP4_NEWS_TARGET_MAX": "30",
    "GTI_STRICT_NEWS_TARGET_MAX": "30",
    "GTI_STRICT_FINAL_ENABLED": "1",
    "GTI_NEWS_TITLE_KEYWORD_REQUIRED": "Y",
    # Keep original URL/title accuracy high; Google broad collection defers final URL checks to Step3/Step4.
    "GTI_STEP2_RESOLVE_ORIGINAL_URL": "N",
    "GTI_STEP2_URL_RESOLVE_LIMIT": "0",
    "GTI_ORIGINAL_URL_SEARCH_ENABLED": "1",
    "GTI_SELENIUM_GOOGLE_RESOLVE": "1",
    "GTI_SELENIUM_GOOGLE_TIMEOUT": "20",
}


@dataclass(frozen=True)
class Step:
    name: str
    script: str
    required: bool = True
    expected_outputs: tuple[str, ...] = field(default_factory=tuple)
    args: tuple[str, ...] = field(default_factory=tuple)
    timeout_sec: int | None = None


STAGE_1 = [
    Step(
        "STEP1_SITE_CRAWLER",
        "1.site_crawler.py",
        required=True,
        expected_outputs=("1-1.regulation_raw.xlsx",),
        timeout_sec=1800,
    ),
]

STAGE_2 = [
    Step("STEP2_NAVER", "2-1.NAVER_news_collector.py", required=False, expected_outputs=("2-1.naver_news_raw.xlsx",), timeout_sec=900),
    Step("STEP2_GOOGLE", "2-2.google_news_collector.py", required=False, expected_outputs=("2-2.google_news_raw.xlsx",), timeout_sec=1200),
    Step("STEP2_RSS", "2-3.rss_news_raw.py", required=False, expected_outputs=("2-3.rss_news_raw.xlsx",), timeout_sec=900),
]

STAGE_3 = [
    Step(
        "STEP3_2_NEWS_MERGE",
        "3-2.news_merge.py",
        required=True,
        expected_outputs=("3-2.news_summary.xlsx", "3-2.news_cumulative.xlsx"),
        timeout_sec=1800,
    ),
]

STAGE_3_ARTICLE = [
    Step(
        "STEP3_ARTICLE_SUMMARY",
        "3-1.regulation_merge.py",
        required=True,
        expected_outputs=(
            "3-1.regulation_article_summary.xlsx",
            "3-2.news_article_summary.xlsx",
            "3-2.news_article_cluster_audit.xlsx",
        ),
        timeout_sec=14400,
    ),
]

STAGE_4 = [
    Step(
        "STEP4_1_REGULATION_AI",
        "4-1.regulation_ai_analysis.py",
        required=True,
        expected_outputs=("4-1.regulation_ai_summary.xlsx", "4-1.regulation_ai_cumulative.xlsx"),
        timeout_sec=1800,
    ),
    Step(
        "STEP4_2_NEWS_AI",
        "4-2.news_ai_analysis.py",
        required=True,
        expected_outputs=("4-2.news_ai_summary.xlsx", "4-2.news_ai_cumulative.xlsx"),
        timeout_sec=1800,
    ),
]

STAGE_5 = [
    Step(
        "STEP5_MAIL_ENGINE",
        "5.GTI_Mail_Engine.py",
        required=True,
        expected_outputs=(),
        args=(
            "--regulation-input",
            str(BASE_DIR / "4-1.regulation_ai_summary.xlsx"),
            "--news-input",
            str(BASE_DIR / "4-2.news_ai_summary.xlsx"),
            "--output-dir",
            str(MAIL_OUTPUT_DIR),
        ),
        timeout_sec=1200,
    ),
]

ARCHIVE_TARGETS = [
    "1.site_news_raw.xlsx",
    "1.site_news_audit.xlsx",
    "1-1.regulation_raw.xlsx",
    "1-2.site_news_raw.xlsx",
    "2-1.naver_news_raw.xlsx",
    "2-2.google_news_raw.xlsx",
    "2-3.rss_news_raw.xlsx",
    "3-1.regulation_summary.xlsx",
    "3-1.regulation_cumulative.xlsx",
    "3-2.news_summary.xlsx",
    "3-2.news_cumulative.xlsx",
    "3-1.regulation_article_summary.xlsx",
    "3-2.news_article_summary.xlsx",
    "4-1.regulation_ai_summary.xlsx",
    "4-1.regulation_ai_cumulative.xlsx",
    "4-2.news_ai_summary.xlsx",
    "4-2.news_ai_cumulative.xlsx",
    "4-2.news_ai_audit_candidates.xlsx",
    "4-2.news_ai_excluded.xlsx",
    "4-1.regulation_ai_excluded.xlsx",
    "3-2.news_article_before_cluster.xlsx",
    "1.site_news_reject_debug.xlsx",
    "1.site_news_final_excluded.xlsx",
    "1-1.regulation_review_raw.xlsx",
    "1-1.regulation_new_raw.xlsx",
    "4.gti_mail_input.xlsx",
    "GTI_Radar.xlsx",
    "mail_cumulative.xlsx",
]


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    MAIL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def apply_pipeline_env_defaults() -> None:
    for key, value in PIPELINE_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)


def log(message: str = "") -> None:
    line = f"[{now()}] {message}"
    print(line)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except PermissionError:
        pass


def get_python_exe() -> str:
    candidates = [
        str(PYTHON_EXE),
        sys.executable,
        shutil.which("python"),
        shutil.which("py"),
    ]
    for exe in candidates:
        if exe and Path(exe).exists():
            return exe
    return sys.executable


def file_has_rows(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    if path.suffix.lower() not in {".xlsx", ".xls"}:
        return True
    try:
        df = pd.read_excel(path, nrows=2)
        return len(df) > 0 or len(df.columns) > 0
    except Exception:
        return False


def mail_run_date() -> str:
    return os.getenv("GTI_RUN_DATE", datetime.now().strftime("%Y-%m-%d"))


def validate_mail_outputs() -> tuple[bool, list[str]]:
    run_date = mail_run_date()
    expected = [
        MAIL_OUTPUT_DIR / f"[GTI Radar] Global Trade Intelligence({run_date}).html",
        MAIL_OUTPUT_DIR / f"[GTI Radar] Global Trade Intelligence({run_date}).xlsx",
    ]
    missing_or_empty = [str(path) for path in expected if not file_has_rows(path)]
    return not missing_or_empty, missing_or_empty


def validate_outputs(step: Step) -> tuple[bool, list[str]]:
    if step.name == "STEP5_MAIL_ENGINE":
        return validate_mail_outputs()

    missing_or_empty: list[str] = []
    for filename in step.expected_outputs:
        path = BASE_DIR / filename
        if not file_has_rows(path):
            missing_or_empty.append(filename)
    return not missing_or_empty, missing_or_empty


def run_script(step: Step, python_exe: str, dry_run: bool = False) -> str:
    script_path = BASE_DIR / step.script
    log("=" * 80)
    log(f"{step.name} START : {step.script}")
    log("=" * 80)

    if not script_path.exists():
        log(f"FILE NOT FOUND : {step.script}")
        return "FAILED" if step.required else "SKIPPED"

    command = [python_exe, str(script_path), *step.args]
    log("COMMAND : " + " ".join(f'"{x}"' if " " in x else x for x in command))

    if dry_run:
        log(f"DRY RUN SKIPPED : {step.script}")
        return "DRY_RUN"

    start = time.time()
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    if step.timeout_sec:
        log(f"TIMEOUT : {step.timeout_sec} sec")

    proc = subprocess.Popen(
        command,
        cwd=str(BASE_DIR),
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    assert proc.stdout is not None

    output_queue: queue.Queue[str | None] = queue.Queue()

    def _reader() -> None:
        try:
            for out_line in proc.stdout:
                output_queue.put(out_line)
        finally:
            output_queue.put(None)

    reader = threading.Thread(target=_reader, daemon=True)
    reader.start()
    reader_done = False
    timed_out = False

    while True:
        try:
            out_line = output_queue.get(timeout=0.5)
            if out_line is None:
                reader_done = True
            else:
                log(f"  {out_line.rstrip()}")
        except queue.Empty:
            pass

        if step.timeout_sec and proc.poll() is None and (time.time() - start) > step.timeout_sec:
            timed_out = True
            log(f"{step.name} TIMEOUT : exceeded {step.timeout_sec} sec; terminating process tree")
            try:
                if os.name == "nt":
                    subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                else:
                    proc.kill()
            except Exception as exc:
                log(f"{step.name} TIMEOUT KILL WARN : {exc}")
            break

        if proc.poll() is not None:
            if reader_done:
                break
            reader.join(timeout=2)
            if not reader.is_alive():
                reader_done = True
                break
            log(f"{step.name} OUTPUT READER WARN : process ended but output reader is still waiting; continuing")
            break

    try:
        reader.join(timeout=2)
    except RuntimeError:
        pass

    while not output_queue.empty():
        out_line = output_queue.get_nowait()
        if out_line:
            log(f"  {out_line.rstrip()}")

    return_code = proc.wait()
    elapsed = round(time.time() - start, 2)

    if timed_out:
        log(f"{step.name} FAILED : timeout / {elapsed} sec")
        return "FAILED" if step.required else "WARNING"

    if return_code != 0:
        log(f"{step.name} FAILED : return_code={return_code} / {elapsed} sec")
        return "FAILED" if step.required else "WARNING"

    ok, bad_outputs = validate_outputs(step)
    if not ok:
        log(f"{step.name} OUTPUT CHECK FAILED : {', '.join(bad_outputs)}")
        return "FAILED" if step.required else "WARNING"

    log(f"{step.name} COMPLETE : {elapsed} sec")
    return "OK"


def archive_outputs() -> None:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target_dir = ARCHIVE_DIR / stamp
    target_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    for filename in ARCHIVE_TARGETS:
        src = BASE_DIR / filename
        if src.exists():
            try:
                shutil.copy2(src, target_dir / filename)
                copied += 1
                log(f"ARCHIVE OK : {filename}")
            except Exception as exc:
                log(f"ARCHIVE FAIL : {filename} / {exc}")

    log(f"ARCHIVE COMPLETE : {copied} files -> {target_dir}")


def collapse_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Merge duplicate column names created by normalization aliases."""
    if df.columns.is_unique:
        return df

    merged: list[pd.Series] = []
    names = list(dict.fromkeys(df.columns))
    for name in names:
        same_name = df.loc[:, df.columns == name]
        if same_name.shape[1] == 1:
            series = same_name.iloc[:, 0]
        else:
            series = same_name.replace("", pd.NA).bfill(axis=1).iloc[:, 0].fillna("")
        series.name = name
        merged.append(series)

    return pd.concat(merged, axis=1)


def normalize_mail_columns(df: pd.DataFrame, category: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    rename_map = {
        "Title": "Headline",
        "title": "Headline",
        "headline": "Headline",
        "Link": "URL",
        "link": "URL",
        "url": "URL",
        "Publisher": "Source",
        "publisher": "Source",
        "Agency": "agency",
        "Risk": "risk",
        "NewsType": "news_type",
        "AI_Analysis": "AI Analysis",
        "Action": "Action Plan",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df = collapse_duplicate_columns(df)

    required_cols = ["Date", "CollectedAt", "Headline", "URL", "Source", "agency", "risk", "score", "news_type"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    if "KeywordMatches" in df.columns and "Keyword" not in df.columns:
        df["Keyword"] = df["KeywordMatches"]
    elif "Keyword" not in df.columns:
        df["Keyword"] = category

    if "Summary" not in df.columns:
        df["Summary"] = ""
    if "AI Analysis" not in df.columns:
        df["AI Analysis"] = ""
    if "Action Plan" not in df.columns:
        df["Action Plan"] = ""

    df["pipeline_category"] = category
    df["Keyword"] = df["Keyword"].fillna("").astype(str)
    df.loc[df["Keyword"].str.strip() == "", "Keyword"] = category

    return df


def build_mail_input() -> bool:
    inputs = [
        ("REGULATION", BASE_DIR / "4-1.regulation_ai_summary.xlsx"),
        ("NEWS", BASE_DIR / "4-2.news_ai_summary.xlsx"),
    ]

    frames: list[pd.DataFrame] = []
    for category, path in inputs:
        if not path.exists():
            log(f"MAIL INPUT SOURCE MISSING : {path.name}")
            continue
        try:
            df = pd.read_excel(path)
            df = normalize_mail_columns(df, category)
            frames.append(df)
            log(f"MAIL INPUT SOURCE OK : {path.name} / rows={len(df)}")
        except Exception as exc:
            log(f"MAIL INPUT SOURCE FAIL : {path.name} / {exc}")

    if not frames:
        log("MAIL INPUT BUILD FAILED : no source rows")
        return False

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(how="all")
    combined = combined[
        (combined["Headline"].fillna("").astype(str).str.strip() != "")
        & (combined["URL"].fillna("").astype(str).str.strip() != "")
    ]

    if combined.empty:
        log("MAIL INPUT BUILD FAILED : combined file has no valid rows")
        return False

    sort_cols = [c for c in ["pipeline_category", "score", "Date", "CollectedAt"] if c in combined.columns]
    if sort_cols:
        ascending = [True if c == "pipeline_category" else False for c in sort_cols]
        combined = combined.sort_values(sort_cols, ascending=ascending, kind="stable")

    combined.to_excel(MAIL_INPUT_FILE, index=False)
    log(f"MAIL INPUT CREATED : {MAIL_INPUT_FILE} / rows={len(combined)}")
    return True


def run_stage(
    stage_name: str,
    steps: list[Step],
    python_exe: str,
    dry_run: bool,
    keep_going: bool,
    results: list[tuple[str, str, str]],
) -> bool:
    log("")
    log("#" * 80)
    log(f"{stage_name} START")
    log("#" * 80)

    stage_ok = True
    for step in steps:
        status = run_script(step, python_exe, dry_run=dry_run)
        results.append((step.name, step.script, status))

        if step.required and status == "FAILED":
            stage_ok = False
            log(f"PIPELINE REQUIRED STEP FAILED : {step.name}")
            if not keep_going:
                return False

    return stage_ok


def print_result(results: list[tuple[str, str, str]]) -> None:
    log("#" * 80)
    log("GTI PIPELINE RESULT")
    log("#" * 80)

    for step_name, script_file, status in results:
        log(f"{step_name} / {script_file} : {status}")

    counts = {}
    for _, _, status in results:
        counts[status] = counts.get(status, 0) + 1

    log("-" * 80)
    for key in ["OK", "WARNING", "SKIPPED", "DRY_RUN", "FAILED"]:
        log(f"{key:<8}: {counts.get(key, 0)}")
    log("#" * 80)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run GTI law/news split pipeline")
    parser.add_argument("--no-archive", action="store_true", help="Do not archive previous outputs before running")
    parser.add_argument("--skip-mail", action="store_true", help="Run steps 1-4 only")
    parser.add_argument("--keep-going", action="store_true", help="Continue after required step failures")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running scripts")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ensure_dirs()
    apply_pipeline_env_defaults()

    log("#" * 80)
    log("GTI LAW/NEWS SPLIT PIPELINE START")
    log("#" * 80)
    log(f"BASE_DIR : {BASE_DIR}")

    python_exe = get_python_exe()
    log(f"PYTHON : {python_exe}")

    if not args.no_archive:
        archive_outputs()

    results: list[tuple[str, str, str]] = []

    stages = [
        ("STAGE 1 - LAW1 OFFICIAL REGULATION CRAWL", STAGE_1),
        ("STAGE 2 - NEWS COLLECTORS", STAGE_2),
        ("STAGE 3 - NEWS MERGE", STAGE_3),
        ("STAGE 3-1 - LAW1/NEWS ARTICLE SUMMARY", STAGE_3_ARTICLE),
        ("STAGE 4 - LAW1/NEWS AI ANALYSIS", STAGE_4),
    ]

    pipeline_ok = True
    for stage_name, steps in stages:
        stage_ok = run_stage(stage_name, steps, python_exe, args.dry_run, args.keep_going, results)
        pipeline_ok = pipeline_ok and stage_ok
        if not stage_ok and not args.keep_going:
            break

    if pipeline_ok and not args.skip_mail and not args.dry_run:
        # Step5 v24+ reads 4-1/4-2 directly. 4.gti_mail_input.xlsx is built only as an optional audit file.
        build_mail_input()
        stage_ok = run_stage("STAGE 5 - MAIL", STAGE_5, python_exe, args.dry_run, args.keep_going, results)
        pipeline_ok = pipeline_ok and stage_ok
    elif args.skip_mail:
        log("STAGE 5 SKIPPED BY OPTION")
    elif args.dry_run:
        log("MAIL INPUT BUILD SKIPPED BY DRY RUN")

    print_result(results)

    if pipeline_ok and not any(status == "FAILED" for _, _, status in results):
        log("GTI PIPELINE FINISHED")
        return 0

    log("GTI PIPELINE FINISHED WITH ERROR")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())






