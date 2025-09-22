import re
from datetime import datetime
import pandas as pd
from jobspy import scrape_jobs


LOCATIONS = ["United States", "London, UK", "Paris, France"]
SITES = ["indeed", "linkedin", "glassdoor", "google"]
SEARCH_TERM = "finance"
RESULTS_WANTED = 100 
DAYS_OLD = 14
OUTPUT_FILE = f"finance_jobs_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

# Regex filters
INCLUDE_PATTERNS = [
    r"\bfinance\b", r"\bfinancial\b", r"\bfp&a\b", r"\btreasury\b",
    r"\bcontroller\b", r"\baccounting\b", r"\bfinancial analyst\b",
    r"\bfinance manager\b", r"\bfinance director\b", r"\binvestment\b",
    r"\basset management\b", r"\bwealth management\b", r"\brisk\b",
    r"\bcredit\b", r"\bprivate equity\b", r"\bventure capital\b",
    r"\bhedge fund\b", r"\binvestment banking\b", r"\bequity research\b",
]
EXCLUDE_PATTERNS = [
    r"\bmarketing\b", r"\bassistant\b", r"\bvolunteer\b",
    r"\bteacher\b", r"\bnurse\b", r"\bdriver\b",
    r"\bsoftware\b", r"\bdeveloper\b", r"\bengineer\b",
    r"\bdata scientist\b", r"\bml\b", r"\bai\b", r"\bit support\b",
]
MIN_SALARY = None
DEDUPLICATE_ON = ["job_url", "title", "company"]


def compile_patterns(words):
    return [re.compile(w, flags=re.IGNORECASE) for w in words]


def any_match(patterns, text):
    return any(p.search(text) for p in patterns)


def is_finance_job(title, description, include_patterns, exclude_patterns):
    blob = f"{title or ''}\n{description or ''}"
    if not any_match(include_patterns, blob):
        return False
    if any_match(exclude_patterns, blob):
        return False
    return True


def save_df(df: pd.DataFrame, path: str) -> None:
    if path.lower().endswith(".xlsx"):
        df.to_excel(path, index=False)
    else:
        df.to_csv(path, index=False, encoding="utf-8-sig")


def main():
    include_patterns = compile_patterns(INCLUDE_PATTERNS)
    exclude_patterns = compile_patterns(EXCLUDE_PATTERNS)

    all_frames = []
    for loc in LOCATIONS:
        print(f"Scraping {loc} ...")
        df = scrape_jobs(
            site_name=SITES,
            search_term=SEARCH_TERM,
            location=loc,
            results_wanted=RESULTS_WANTED,
            hours_old=DAYS_OLD * 24,
            is_remote=False,
            linkedin_fetch_description=True,
        )
        if df is None or df.empty:
            continue

        for col in ["title", "company", "location", "date_posted", "is_remote",
                    "description", "site", "job_url", "salary_min", "salary_max", "job_type"]:
            if col not in df.columns:
                df[col] = None

        # Finance filtering
        mask_fin = df.apply(
            lambda r: is_finance_job(
                str(r.get("title", "")), str(r.get("description", "")),
                include_patterns, exclude_patterns
            ),
            axis=1
        )
        df = df.loc[mask_fin].copy()

        # Salary filtering
        if MIN_SALARY is not None:
            def meets_min(row):
                vals = []
                for c in ("salary_min", "salary_max"):
                    if c in row and pd.notna(row[c]):
                        vals.append(float(row[c]))
                return bool(vals) and max(vals) >= MIN_SALARY
            df = df[df.apply(meets_min, axis=1)]

        df["search_location"] = loc
        all_frames.append(df)

    if not all_frames:
        print("No results found after filtering.")
        return

    out = pd.concat(all_frames, ignore_index=True)

    # Deduplicate
    if DEDUPLICATE_ON:
        out = out.drop_duplicates(subset=DEDUPLICATE_ON, keep="first")


    if "date_posted" in out.columns:
        out["date_posted"] = pd.to_datetime(out["date_posted"], errors="coerce", utc=True)
        out = out.sort_values("date_posted", ascending=False, na_position="last")

    save_df(out, OUTPUT_FILE)
    print(f"Saved {len(out):,} to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()