def load_keywords() -> List[str]:
    """
    Tight loader for keywords (제시어)
    Priority:
      1) custom_queries.TXT
      2) sites.xlsx (strict sheet/column rules)
      3) fallback ["관세"]

    Excel rule (sites.xlsx):
      - Prefer specific sheets: sites/site/list/source/config (case-insensitive contains)
      - Keyword columns allowed: 제시어, 검색어, 키워드, query, keyword, term
      - Optional enable columns allowed: 사용, 활성, enable, enabled, active, use
      - If enable col exists => only truthy rows are used
      - Split cell values by [comma, semicolon, pipe, newline] into multiple keywords
    """
    import os
    import re
    import pandas as pd

    def _dedup(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    def _clean(s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"\s+", " ", s)
        return s

    def _split_keywords(cell: str) -> List[str]:
        cell = _clean(cell)
        if not cell:
            return []
        # 관세,관세율; tariff | section 301  \n 등 분해
        parts = re.split(r"[,;|\n]+", cell)
        parts = [_clean(p) for p in parts]
        return [p for p in parts if p]

    def _is_truthy(v) -> bool:
        if v is None:
            return False
        if isinstance(v, (int, float)):
            return v == 1
        s = str(v).strip().lower()
        return s in {"1", "y", "yes", "true", "t", "on", "use", "enable", "enabled", "active"}

    # 1) TXT
    txt_path = os.path.join(os.path.dirname(__file__), "custom_queries.TXT")
    if os.path.exists(txt_path):
        kws = []
        with open(txt_path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                kws.extend(_split_keywords(s))
        kws = _dedup([k for k in kws if k])
        if kws:
            return kws

    # 2) XLSX (strict)
    xlsx_path = os.path.join(os.path.dirname(__file__), "sites.xlsx")
    if os.path.exists(xlsx_path):
        try:
            xl = pd.ExcelFile(xlsx_path)

            # ✅ 타이트: 시트 후보를 제한
            sheet_priority_keywords = ["sites", "site", "list", "source", "config", "setting", "master"]
            sheets = xl.sheet_names

            # 우선순위 시트 먼저
            preferred = []
            others = []
            for sh in sheets:
                key = sh.strip().lower()
                if any(k in key for k in sheet_priority_keywords):
                    preferred.append(sh)
                else:
                    others.append(sh)
            # preferred 먼저 보고, 없으면 others도 보되 "제시어 컬럼이 정확히 있는 시트만" 사용
            scan_order = preferred + others

            # ✅ 타이트: 컬럼 후보 제한 (여기 없는 컬럼은 제시어로 절대 안 봄)
            kw_cols = ["제시어", "검색어", "키워드", "query", "keyword", "term"]
            enable_cols = ["사용", "활성", "enable", "enabled", "active", "use"]

            all_kws = []
            for sh in scan_order:
                df = xl.parse(sh)

                # 컬럼명 trim
                df.columns = [str(c).strip() for c in df.columns]

                kw_col = next((c for c in kw_cols if c in df.columns), None)
                if not kw_col:
                    continue  # ✅ 타이트: 제시어 컬럼 없으면 해당 시트 무시

                en_col = next((c for c in enable_cols if c in df.columns), None)

                for _, row in df.iterrows():
                    if en_col is not None:
                        if not _is_truthy(row.get(en_col)):
                            continue
                    cell = row.get(kw_col)
                    if pd.isna(cell):
                        continue
                    all_kws.extend(_split_keywords(str(cell)))

                # ✅ 타이트: 첫 “유효 시트”에서 키워드를 찾으면 그 시트만 사용하고 종료
                # (여러 시트 섞이면 관리가 어려워지는 걸 방지)
                if all_kws:
                    break

            all_kws = _dedup([k for k in all_kws if k])
            if all_kws:
                return all_kws

        except Exception:
            pass

    return ["관세"]
