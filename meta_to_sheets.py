import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

GRAPH_BASE = "https://graph.facebook.com"
JST = ZoneInfo("Asia/Tokyo")

TARGET_ACTION_CV = "offsite_conversion.fb_pixel_purchase"
TARGET_ACTION_SALES = "purchase"

MONTHLY_METRIC_HEADERS = [
    "Month", "campaign_name", "reach",
    "cv_view_1d", "cv_click_7d",
    "sales_view_1d", "sales_click_7d",
    "cpa_click_7d", "roas_click_7d",
]

FILTER_CAMPAIGN_KEYWORDS = [
    "Camper(CORE)",
    "Camper(SP)",
    "Camper(CORE-L)",
    "CV最適化",
]

FILTER_CAMPAIGN_TOTAL_NAMES = {
    "Camper(CORE)": "Camper(CORE)_合計",
    "Camper(SP)": "Camper(SP)_合計",
    "Camper(CORE-L)": "Camper(CORE-L)_合計",
    "CV最適化": "CV最適化_合計",
}

ROW_METRIC_HEADERS = [
    "impressions", "reach", "spend",
    "cv_view_1d", "cv_click_7d",
    "sales_view_1d", "sales_click_7d",
    "cpa_click_7d", "roas_click_7d",
]

AUSE_ROW_METRIC_HEADERS = [
    "impressions", "spend", "cv",
]

AUDE_ROW_EXTRA_METRIC_HEADERS = [
    "link_clicks", "clicks_all", "purchase",
    "add_to_cart", "leads",
    "post_reactions", "post_comments",
    "post_saves", "post_shares",
]

AUDE_ACTION_TYPE_CANDIDATES = {
    "add_to_cart": [
        "offsite_conversion.fb_pixel_add_to_cart",
        "omni_add_to_cart",
        "mobile_app_add_to_cart",
        "add_to_cart",
    ],
    "leads": [
        "lead",
        "offsite_conversion.fb_pixel_lead",
        "onsite_conversion.lead_grouped",
        "leadgen.other",
    ],
    "post_reactions": ["post_reaction"],
    "post_comments": ["comment"],
    "post_saves": ["post_save"],
    "post_shares": ["post_share", "post"],
}


def _act_id_normalize(m_act_id: str) -> str:
    s = str(m_act_id).strip()
    return s[4:] if s.startswith("act_") else s


def this_month_range_to_yesterday_jst() -> Optional[Tuple[str, str]]:
    today = datetime.now(JST).date()
    yesterday = today - timedelta(days=1)
    since = date(today.year, today.month, 1)
    if yesterday < since:
        return None
    return since.isoformat(), yesterday.isoformat()


def month_start_n_months_ago(base_month_start: date, months_ago: int) -> date:
    year = base_month_start.year
    month = base_month_start.month - months_ago
    while month <= 0:
        year -= 1
        month += 12
    return date(year, month, 1)


def compare_monthly_range_to_yesterday_jst() -> Optional[Tuple[str, str]]:
    today = datetime.now(JST).date()
    yesterday = today - timedelta(days=1)
    current_month_start = date(today.year, today.month, 1)
    since = month_start_n_months_ago(current_month_start, 1)
    if yesterday < since:
        return None
    return since.isoformat(), yesterday.isoformat()


def monthly_range_to_yesterday_jst() -> Optional[Tuple[str, str]]:
    today = datetime.now(JST).date()
    yesterday = today - timedelta(days=1)
    current_month_start = date(today.year, today.month, 1)
    since = month_start_n_months_ago(current_month_start, 14)
    if yesterday < since:
        return None
    return since.isoformat(), yesterday.isoformat()


def meta_get_insights(
    api_version: str, m_token: str, m_act_id: str, fields: List[str],
    date_preset: Optional[str] = None, time_range: Optional[Dict[str, str]] = None,
    action_attribution_windows: Optional[List[str]] = None,
    level: str = "campaign", breakdowns: Optional[List[str]] = None,
    time_increment: Optional[str] = None,
    filtering: Optional[List[Dict[str, Any]]] = None,
    limit: int = 500, max_pages: int = 50,
) -> List[Dict[str, Any]]:

    act = _act_id_normalize(m_act_id)
    url = f"{GRAPH_BASE}/{api_version}/act_{act}/insights"

    params: Dict[str, Any] = {
        "access_token": m_token,
        "level": level,
        "fields": ",".join(fields),
        "limit": limit,
    }

    if date_preset:
        params["date_preset"] = date_preset
    else:
        params["time_range"] = json.dumps(time_range, separators=(",", ":"))

    if action_attribution_windows:
        params["action_attribution_windows"] = json.dumps(action_attribution_windows, separators=(",", ":"))

    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)

    if time_increment:
        params["time_increment"] = time_increment

    if filtering:
        params["filtering"] = json.dumps(filtering, separators=(",", ":"))

    out: List[Dict[str, Any]] = []
    pages = 0

    while True:
        r = requests.get(url, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Meta API error: {r.status_code} {r.text}")

        payload = r.json()
        out.extend(payload.get("data", []))

        next_url = payload.get("paging", {}).get("next")
        if not next_url:
            break

        pages += 1
        if pages >= max_pages:
            break

        url = next_url
        params = None
        time.sleep(0.2)

    return out


def get_action_value(actions: Optional[List[Dict[str, Any]]], target_action: str, attr_window: str) -> float:
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == target_action:
            try:
                if attr_window == "value":
                    return float(a.get("value", 0))
                return float(a.get(attr_window, 0))
            except:
                return 0.0
    return 0.0


def get_action_value_multi(actions: Optional[List[Dict[str, Any]]], target_actions: List[str], attr_window: str) -> float:
    if not actions:
        return 0.0
    total = 0.0
    target_set = set(target_actions)
    for a in actions:
        if a.get("action_type") in target_set:
            try:
                if attr_window == "value":
                    total += float(a.get("value", 0))
                else:
                    total += float(a.get(attr_window, 0))
            except:
                continue
    return total


def extract_metrics(row: Dict[str, Any], attr_window_cv: str = "1d_view", attr_window_cv_click: str = "7d_click") -> Dict[str, Any]:
    return {
        "spend": float(row.get("spend") or 0.0),
        "reach": int(row.get("reach") or 0),
        "impressions": int(row.get("impressions") or 0),
        "cv_1d": get_action_value(row.get("actions", []), TARGET_ACTION_CV, attr_window_cv),
        "cv_7d": get_action_value(row.get("actions", []), TARGET_ACTION_CV, attr_window_cv_click),
        "sales_1d": get_action_value(row.get("action_values", []), TARGET_ACTION_SALES, attr_window_cv),
        "sales_7d": get_action_value(row.get("action_values", []), TARGET_ACTION_SALES, attr_window_cv_click),
    }


def extract_aude_metrics(row: Dict[str, Any], attr_window_cv: str = "1d_view", attr_window_cv_click: str = "7d_click") -> Dict[str, Any]:
    metrics = extract_metrics(row, attr_window_cv, attr_window_cv_click)
    actions = row.get("actions", [])
    metrics.update({
        "link_clicks": float(row.get("inline_link_clicks") or 0.0),
        "clicks_all": float(row.get("clicks") or 0.0),
        "purchase": get_action_value(actions, "purchase", attr_window_cv_click),
        "add_to_cart": get_action_value_multi(actions, AUDE_ACTION_TYPE_CANDIDATES["add_to_cart"], attr_window_cv_click),
        "leads": get_action_value_multi(actions, AUDE_ACTION_TYPE_CANDIDATES["leads"], attr_window_cv_click),
        "post_reactions": get_action_value_multi(actions, AUDE_ACTION_TYPE_CANDIDATES["post_reactions"], attr_window_cv_click),
        "post_comments": get_action_value_multi(actions, AUDE_ACTION_TYPE_CANDIDATES["post_comments"], attr_window_cv_click),
        "post_saves": get_action_value_multi(actions, AUDE_ACTION_TYPE_CANDIDATES["post_saves"], attr_window_cv_click),
        "post_shares": get_action_value_multi(actions, AUDE_ACTION_TYPE_CANDIDATES["post_shares"], attr_window_cv_click),
    })
    return metrics


def fmt_value(x: Any) -> Any:
    if x is None:
        return ""
    try:
        return round(float(x), 6)
    except:
        return ""


def compute_monthly_metric_row(metrics: Dict[str, Any]) -> List[Any]:
    spend = metrics.get("spend", 0.0)
    cv7 = metrics.get("cv_7d", 0.0)
    sales7 = metrics.get("sales_7d", 0.0)
    cpa = (spend / cv7) if cv7 > 0 else None
    roas = (sales7 / spend) if spend > 0 else None

    return [
        fmt_value(metrics.get("impressions", 0)),
        fmt_value(metrics.get("reach", 0)),
        fmt_value(spend),
        fmt_value(metrics.get("cv_1d", 0.0)),
        fmt_value(cv7),
        fmt_value(metrics.get("sales_1d", 0.0)),
        fmt_value(sales7),
        fmt_value(cpa),
        fmt_value(roas),
    ]


def compute_monthly_aude_metric_row(metrics: Dict[str, Any]) -> List[Any]:
    spend = metrics.get("spend", 0.0)
    cv7 = metrics.get("cv_7d", 0.0)
    sales7 = metrics.get("sales_7d", 0.0)
    cpa = (spend / cv7) if cv7 > 0 else None
    roas = (sales7 / spend) if spend > 0 else None

    return [
        fmt_value(metrics.get("impressions", 0)),
        fmt_value(metrics.get("reach", 0)),
        fmt_value(spend),
        fmt_value(metrics.get("link_clicks", 0.0)),
        fmt_value(metrics.get("clicks_all", 0.0)),
        fmt_value(metrics.get("purchase", 0.0)),
        fmt_value(metrics.get("add_to_cart", 0.0)),
        fmt_value(metrics.get("leads", 0.0)),
        fmt_value(metrics.get("post_reactions", 0.0)),
        fmt_value(metrics.get("post_comments", 0.0)),
        fmt_value(metrics.get("post_saves", 0.0)),
        fmt_value(metrics.get("post_shares", 0.0)),
        fmt_value(metrics.get("cv_1d", 0.0)),
        fmt_value(cv7),
        fmt_value(metrics.get("sales_1d", 0.0)),
        fmt_value(metrics.get("sales_7d", 0.0)),
        fmt_value(cpa),
        fmt_value(roas),
    ]


def compute_monthly_ause_metric_row(metrics: Dict[str, Any]) -> List[Any]:
    return [
        fmt_value(metrics.get("impressions", 0)),
        fmt_value(metrics.get("spend", 0.0)),
        fmt_value(metrics.get("cv_1d", 0.0)),
    ]


def build_ad_monthly_table(rows: List[Dict]) -> List[List[Any]]:
    header = ["Month", "campaign_name", "adset_name", "ad_id", "ad_name"] + ROW_METRIC_HEADERS
    table = [header]

    sorted_rows = sorted(
        rows,
        key=lambda r: (
            r.get("date_start", ""),
            r.get("campaign_name", ""),
            r.get("adset_name", ""),
            r.get("ad_name", ""),
            r.get("ad_id", ""),
        )
    )

    for r in sorted_rows:
        table.append([
            (r.get("date_start") or "")[:7],
            r.get("campaign_name", ""),
            r.get("adset_name", ""),
            r.get("ad_id", ""),
            r.get("ad_name", ""),
            *compute_monthly_metric_row(extract_metrics(r)),
        ])

    return table


def build_audience_monthly_table(
    adset_rows: List[Dict], camp_rows: List[Dict], gender_rows: List[Dict], age_rows: List[Dict], plat_rows: List[Dict]
) -> List[List[Any]]:
    header = ["Month", "Category", "Campaign Name", "Breakdown"] + ROW_METRIC_HEADERS
    table = [header]

    def add_rows(rows: List[Dict], cat_name: str, camp_fn, detail_fn):
        for r in sorted(rows, key=lambda x: (x.get("date_start", ""), camp_fn(x), detail_fn(x), x.get("campaign_id", ""), x.get("adset_id", ""))):
            table.append([
                (r.get("date_start") or "")[:7],
                cat_name,
                camp_fn(r),
                detail_fn(r),
                *compute_monthly_metric_row(extract_metrics(r)),
            ])

    add_rows(adset_rows, "AdSet", lambda r: r.get("campaign_name", ""), lambda r: r.get("adset_name", ""))
    add_rows(camp_rows, "Campaign Total", lambda r: r.get("campaign_name", ""), lambda r: "Total")
    add_rows(gender_rows, "Gender", lambda r: r.get("campaign_name", ""), lambda r: r.get("gender", ""))
    add_rows(age_rows, "Age", lambda r: r.get("campaign_name", ""), lambda r: r.get("age", ""))
    add_rows(plat_rows, "Platform", lambda r: r.get("campaign_name", ""), lambda r: r.get("publisher_platform", ""))

    return table


def build_audiencedetail_monthly_table(
    adset_plat_rows: List[Dict],
    adset_gen_age_rows: List[Dict],
    plat_pos_dev_rows: List[Dict],
) -> List[List[Any]]:
    header = [
        "Month", "Category", "campaign_name", "Detail1", "Detail2", "Detail3",
        "impressions", "reach", "spend",
        "link_clicks", "clicks_all", "purchase", "add_to_cart", "leads",
        "post_reactions", "post_comments", "post_saves", "post_shares",
        "cv_view_1d", "cv_click_7d", "sales_view_1d", "sales_click_7d", "cpa_click_7d", "roas_click_7d",
    ]
    table = [header]

    def add_rows(rows: List[Dict], cat_name: str, campaign_name_fn, d1_fn, d2_fn, d3_fn):
        for r in sorted(rows, key=lambda x: (x.get("date_start", ""), campaign_name_fn(x), d1_fn(x), d2_fn(x), d3_fn(x), x.get("adset_id", ""), x.get("campaign_id", ""))):
            table.append([
                (r.get("date_start") or "")[:7],
                cat_name,
                campaign_name_fn(r),
                d1_fn(r),
                d2_fn(r),
                d3_fn(r),
                *compute_monthly_aude_metric_row(extract_aude_metrics(r)),
            ])

    add_rows(
        adset_plat_rows,
        "AdSet x Platform",
        lambda r: r.get("campaign_name", ""),
        lambda r: r.get("adset_name", ""),
        lambda r: r.get("publisher_platform", ""),
        lambda r: "",
    )
    add_rows(
        adset_gen_age_rows,
        "AdSet x Gender x Age",
        lambda r: r.get("campaign_name", ""),
        lambda r: r.get("adset_name", ""),
        lambda r: r.get("gender", ""),
        lambda r: r.get("age", ""),
    )
    add_rows(
        plat_pos_dev_rows,
        "Platform x Position x Device Platform",
        lambda r: r.get("campaign_name", ""),
        lambda r: r.get("publisher_platform", ""),
        lambda r: r.get("platform_position", ""),
        lambda r: r.get("device_platform", ""),
    )

    return table


def build_audiencesegment_monthly_table(rows: List[Dict], breakdown_key: str) -> List[List[Any]]:
    header = ["Month", "Category", "Campaign Name", "Audience Segment"] + AUSE_ROW_METRIC_HEADERS
    table = [header]

    seg_totals: Dict[Tuple[str, str], Dict[str, Any]] = {}

    sorted_rows = sorted(
        rows,
        key=lambda r: (
            r.get("date_start", ""),
            r.get("campaign_name", ""),
            str(r.get(breakdown_key) or "__MISSING__"),
            r.get("campaign_id", ""),
        )
    )

    for r in sorted_rows:
        month = (r.get("date_start") or "")[:7]
        persona = r.get(breakdown_key)
        if persona in (None, ""):
            persona = "__MISSING__"
        metrics = extract_metrics(r, "value", "value")

        table.append([
            month,
            "Campaign",
            r.get("campaign_name", ""),
            persona,
            *compute_monthly_ause_metric_row(metrics),
        ])

        total_key = (month, str(persona))
        if total_key not in seg_totals:
            seg_totals[total_key] = {"impressions": 0, "spend": 0.0, "cv_1d": 0.0}
        seg_totals[total_key]["impressions"] += metrics.get("impressions", 0)
        seg_totals[total_key]["spend"] += metrics.get("spend", 0.0)
        seg_totals[total_key]["cv_1d"] += metrics.get("cv_1d", 0.0)

    for month, persona in sorted(seg_totals.keys()):
        table.append([
            month,
            "Total",
            "All Campaigns Sum",
            persona,
            *compute_monthly_ause_metric_row(seg_totals[(month, persona)]),
        ])

    return table


def build_daily_table(last_rows: List[Dict], this_rows: List[Dict]) -> List[List[Any]]:
    header = [
        "Period", "Date", "campaign_id", "campaign_name", "impressions", "reach", "spend",
        "cv_view_1d", "cv_click_7d", "sales_view_1d", "sales_click_7d",
        "cpa_click_7d", "roas_click_7d"
    ]
    table = [header]

    def fmt(x: Any) -> Any:
        if x is None:
            return ""
        try:
            return round(float(x), 6)
        except:
            return ""

    for period_name, rows in [("Last Month", last_rows), ("This Month", this_rows)]:
        for r in rows:
            m = extract_metrics(r)
            imp, reach, spend = m["impressions"], m["reach"], m["spend"]
            cv1, cv7 = m["cv_1d"], m["cv_7d"]
            s1, s7 = m["sales_1d"], m["sales_7d"]
            cpa = (spend / cv7) if cv7 > 0 else None
            roas = (s7 / spend) if spend > 0 else None

            table.append([
                period_name, r.get("date_start", ""), r.get("campaign_id", ""), r.get("campaign_name", ""),
                fmt(imp), fmt(reach), fmt(spend),
                fmt(cv1), fmt(cv7), fmt(s1), fmt(s7), fmt(cpa), fmt(roas)
            ])
    return table


def build_monthly_table(rows: List[Dict[str, Any]]) -> List[List[Any]]:
    header = MONTHLY_METRIC_HEADERS
    table = [header]

    def fmt(x: Any) -> Any:
        if x is None:
            return ""
        try:
            return round(float(x), 6)
        except:
            return ""

    sorted_rows = sorted(
        rows,
        key=lambda r: (
            r.get("date_start", ""),
            r.get("campaign_name", ""),
            r.get("campaign_id", ""),
        )
    )

    for r in sorted_rows:
        m = extract_metrics(r)
        spend = m["spend"]
        cv7 = m["cv_7d"]
        s7 = m["sales_7d"]
        cpa = (spend / cv7) if cv7 > 0 else None
        roas = (s7 / spend) if spend > 0 else None
        month = (r.get("date_start") or "")[:7]

        table.append([
            month,
            r.get("campaign_name", ""),
            fmt(m["reach"]),
            fmt(m["cv_1d"]),
            fmt(m["cv_7d"]),
            fmt(m["sales_1d"]),
            fmt(m["sales_7d"]),
            fmt(cpa),
            fmt(roas),
        ])

    return table


def build_filter_table(detail_rows: List[Dict[str, Any]], total_rows: List[Dict[str, Any]]) -> List[List[Any]]:
    header = MONTHLY_METRIC_HEADERS
    table = [header]

    def fmt(x: Any) -> Any:
        if x is None:
            return ""
        try:
            return round(float(x), 6)
        except:
            return ""

    filtered_detail_rows = [
        r for r in detail_rows
        if any(k in str(r.get("campaign_name", "")) for k in FILTER_CAMPAIGN_KEYWORDS)
    ]

    filtered_detail_rows = sorted(
        filtered_detail_rows,
        key=lambda r: (
            r.get("date_start", ""),
            r.get("campaign_name", ""),
            r.get("campaign_id", ""),
        )
    )

    totals_by_month: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for r in total_rows:
        month = (r.get("date_start") or "")[:7]
        total_name = str(r.get("_filter_total_name", ""))
        totals_by_month.setdefault(month, {})[total_name] = r

    detail_by_month: Dict[str, List[Dict[str, Any]]] = {}
    for r in filtered_detail_rows:
        month = (r.get("date_start") or "")[:7]
        detail_by_month.setdefault(month, []).append(r)

    all_months = sorted(set(detail_by_month.keys()) | set(totals_by_month.keys()))

    ordered_total_names = [
        FILTER_CAMPAIGN_TOTAL_NAMES["Camper(CORE)"],
        FILTER_CAMPAIGN_TOTAL_NAMES["Camper(SP)"],
        FILTER_CAMPAIGN_TOTAL_NAMES["Camper(CORE-L)"],
        FILTER_CAMPAIGN_TOTAL_NAMES["CV最適化"],
    ]

    for month in all_months:
        for r in detail_by_month.get(month, []):
            m = extract_metrics(r)
            spend = m["spend"]
            cv7 = m["cv_7d"]
            s7 = m["sales_7d"]
            cpa = (spend / cv7) if cv7 > 0 else None
            roas = (s7 / spend) if spend > 0 else None

            table.append([
                month,
                r.get("campaign_name", ""),
                fmt(m["reach"]),
                fmt(m["cv_1d"]),
                fmt(m["cv_7d"]),
                fmt(m["sales_1d"]),
                fmt(m["sales_7d"]),
                fmt(cpa),
                fmt(roas),
            ])

        month_totals = totals_by_month.get(month, {})
        for total_name in ordered_total_names:
            r = month_totals.get(total_name)
            if not r:
                continue

            m = extract_metrics(r)
            spend = m["spend"]
            cv7 = m["cv_7d"]
            s7 = m["sales_7d"]
            cpa = (spend / cv7) if cv7 > 0 else None
            roas = (s7 / spend) if spend > 0 else None

            table.append([
                month,
                total_name,
                fmt(m["reach"]),
                fmt(m["cv_1d"]),
                fmt(m["cv_7d"]),
                fmt(m["sales_1d"]),
                fmt(m["sales_7d"]),
                fmt(cpa),
                fmt(roas),
            ])

    return table


def sheets_write(spreadsheet_id: str, worksheet_title: str, values_2d: List[List[Any]], g_creds: Dict[str, Any]) -> None:
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(g_creds, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)

    try:
        ss = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        titles = {s["properties"]["title"] for s in ss.get("sheets", [])}
        if worksheet_title not in titles:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={"requests": [{"addSheet": {"properties": {"title": worksheet_title}}}]}
            ).execute()

        service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=f"{worksheet_title}!A:Z", body={}).execute()
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=f"{worksheet_title}!A1", valueInputOption="USER_ENTERED", body={"values": values_2d}
        ).execute()
    except Exception as e:
        raise RuntimeError(f"sheets_write failed for sheet='{worksheet_title}': {type(e).__name__}") from None


def main():
    raw = os.environ.get("APP_SECRET_JSON")
    if not raw:
        raise RuntimeError("Missing env APP_SECRET_JSON")

    cfg = json.loads(raw)
    m_token = cfg["m_token"]
    m_act_id = cfg["m_act_id"]
    _s_id_raw = cfg["s_id"]
    s_id_list = _s_id_raw if isinstance(_s_id_raw, list) else [_s_id_raw]
    sheets_map = cfg.get("sheets", {})
    g_creds = cfg["g_creds"]

    api_version = cfg.get("m_api_version", "v24.0")

    rng = this_month_range_to_yesterday_jst()
    this_since, this_until = rng if rng else (None, None)

    compare_rng = compare_monthly_range_to_yesterday_jst()
    compare_since, compare_until = compare_rng if compare_rng else (None, None)

    data_cache = {"last": {}, "this": {}, "monthly": {}}

    def get_data(period: str, level: str, fields: List[str], breakdowns: Optional[List[str]] = None, time_increment: Optional[str] = None, attr_windows: Optional[List[str]] = ["1d_view", "7d_click"]) -> List[Dict]:
        cache_key = f"{level}_{','.join(fields)}_{','.join(breakdowns) if breakdowns else 'none'}_{time_increment or 'none'}_{str(attr_windows)}"
        if cache_key not in data_cache[period]:
            if period == "last":
                data_cache[period][cache_key] = meta_get_insights(
                    api_version, m_token, m_act_id, fields, date_preset="last_month",
                    action_attribution_windows=attr_windows, level=level, breakdowns=breakdowns, time_increment=time_increment
                )
            else:
                if not this_since:
                    data_cache[period][cache_key] = []
                else:
                    data_cache[period][cache_key] = meta_get_insights(
                        api_version, m_token, m_act_id, fields, time_range={"since": this_since, "until": this_until},
                        action_attribution_windows=attr_windows, level=level, breakdowns=breakdowns, time_increment=time_increment
                    )
        return data_cache[period][cache_key]

    def get_monthly_data(
        level: str,
        fields: List[str],
        breakdowns: Optional[List[str]] = None,
        attr_windows: Optional[List[str]] = ["1d_view", "7d_click"],
        filtering: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict]:
        filtering_key = json.dumps(filtering, separators=(",", ":"), ensure_ascii=False) if filtering else "none"
        cache_key = f"{level}_{','.join(fields)}_{','.join(breakdowns) if breakdowns else 'none'}_monthly_{str(attr_windows)}_{filtering_key}"

        if cache_key not in data_cache["monthly"]:
            if not compare_since:
                data_cache["monthly"][cache_key] = []
            else:
                data_cache["monthly"][cache_key] = meta_get_insights(
                    api_version, m_token, m_act_id, fields,
                    time_range={"since": compare_since, "until": compare_until},
                    action_attribution_windows=attr_windows,
                    level=level, breakdowns=breakdowns, time_increment="monthly",
                    filtering=filtering,
                )
        return data_cache["monthly"][cache_key]

    for sheet_kind, worksheet_title in sheets_map.items():
        kind = str(sheet_kind).strip().upper()
        print(f"Processing {kind} to sheet '{worksheet_title}'...")

        if kind == "MONTHLY":
            fields = ["campaign_id", "campaign_name", "reach", "spend", "actions", "action_values"]
            monthly_rng = monthly_range_to_yesterday_jst()
            monthly_rows = []
            if monthly_rng:
                monthly_since, monthly_until = monthly_rng
                monthly_rows = meta_get_insights(
                    api_version, m_token, m_act_id, fields,
                    time_range={"since": monthly_since, "until": monthly_until},
                    action_attribution_windows=["1d_view", "7d_click"],
                    level="campaign", time_increment="monthly"
                )

            table = build_monthly_table(monthly_rows)
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote {kind} rows={len(table)-1}")

        elif kind == "FILTER":
            detail_fields = ["campaign_id", "campaign_name", "reach", "spend", "actions", "action_values"]
            total_fields = ["reach", "spend", "actions", "action_values"]

            filter_monthly_rng = monthly_range_to_yesterday_jst()
            filter_detail_rows = []
            filter_total_rows: List[Dict[str, Any]] = []

            if filter_monthly_rng:
                filter_since, filter_until = filter_monthly_rng

                filter_detail_rows = meta_get_insights(
                    api_version, m_token, m_act_id, detail_fields,
                    time_range={"since": filter_since, "until": filter_until},
                    action_attribution_windows=["1d_view", "7d_click"],
                    level="campaign", time_increment="monthly"
                )

                for keyword in FILTER_CAMPAIGN_KEYWORDS:
                    rows = meta_get_insights(
                        api_version, m_token, m_act_id, total_fields,
                        time_range={"since": filter_since, "until": filter_until},
                        action_attribution_windows=["1d_view", "7d_click"],
                        level="account", time_increment="monthly",
                        filtering=[
                            {
                                "field": "campaign.name",
                                "operator": "CONTAIN",
                                "value": keyword,
                            }
                        ],
                    )

                    for r in rows:
                        r2 = dict(r)
                        r2["_filter_total_name"] = FILTER_CAMPAIGN_TOTAL_NAMES[keyword]
                        filter_total_rows.append(r2)

            table = build_filter_table(filter_detail_rows, filter_total_rows)

            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote FILTER rows={len(table)-1}")

        elif kind == "DAILY":
            fields = ["campaign_id", "campaign_name", "spend", "reach", "impressions", "actions", "action_values"]
            last_daily = get_data("last", "campaign", fields, time_increment="1")
            this_daily = get_data("this", "campaign", fields, time_increment="1")

            table = build_daily_table(last_daily, this_daily)
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote DAILY rows={len(table)-1}")

        elif kind == "AD":
            fields = ["campaign_name", "adset_name", "ad_id", "ad_name", "spend", "reach", "impressions", "actions", "action_values"]
            ad_rows = get_monthly_data("ad", fields)

            table = build_ad_monthly_table(ad_rows)
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote AD rows={len(table)-1}")

        elif kind == "AUDIENCE":
            adset_fields = ["campaign_name", "adset_id", "adset_name", "spend", "reach", "impressions", "actions", "action_values"]
            camp_fields = ["campaign_id", "campaign_name", "spend", "reach", "impressions", "actions", "action_values"]

            adset_rows = get_monthly_data("adset", adset_fields)
            camp_rows = get_monthly_data("campaign", camp_fields)
            gender_rows = get_monthly_data("campaign", camp_fields, ["gender"])
            age_rows = get_monthly_data("campaign", camp_fields, ["age"])
            plat_rows = get_monthly_data("campaign", camp_fields, ["publisher_platform"])

            table = build_audience_monthly_table(adset_rows, camp_rows, gender_rows, age_rows, plat_rows)
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote AUDIENCE rows={len(table)-1}")

        elif kind == "AUDIENCEDETAIL":
            adset_fields = [
                "campaign_name", "adset_id", "adset_name", "spend", "reach", "impressions",
                "clicks", "inline_link_clicks", "actions", "action_values"
            ]
            camp_fields = [
                "campaign_id", "campaign_name", "spend", "reach", "impressions",
                "clicks", "inline_link_clicks", "actions", "action_values"
            ]

            def aude_debug(tag: str, rows: List[Dict[str, Any]]) -> None:
                print(f"[AUDE DEBUG] {tag}: rows={len(rows)}")
                if not rows:
                    return

                sample = rows[0]
                action_type_counts: Dict[str, int] = {}
                for r in rows:
                    acts = r.get("actions")
                    if isinstance(acts, list):
                        for a in acts:
                            if not isinstance(a, dict):
                                continue
                            at = str(a.get("action_type") or "__NONE__")
                            action_type_counts[at] = action_type_counts.get(at, 0) + 1

                top_action_types = sorted(action_type_counts.items(), key=lambda x: x[1], reverse=True)[:15]
                print(f"[AUDE DEBUG] {tag}: action_type top15={top_action_types}")
                print(
                    f"[AUDE DEBUG] {tag}: sample direct metrics "
                    f"clicks={sample.get('clicks', 0)} inline_link_clicks={sample.get('inline_link_clicks', 0)}"
                )

                acts = sample.get("actions") if isinstance(sample.get("actions"), list) else []
                sample_action_map: Dict[str, Any] = {}
                for a in acts:
                    if not isinstance(a, dict):
                        continue
                    at = a.get("action_type")
                    if at is not None:
                        sample_action_map[str(at)] = a.get("7d_click", a.get("value", 0))

                for metric_name, candidates in AUDE_ACTION_TYPE_CANDIDATES.items():
                    matched = {c: sample_action_map.get(c, 0) for c in candidates if c in sample_action_map}
                    print(f"[AUDE DEBUG] {tag}: sample_matches {metric_name}={matched or 'NO_MATCH'}")

            adset_plat_rows = get_monthly_data("adset", adset_fields, ["publisher_platform"])
            adset_gen_age_rows = get_monthly_data("adset", adset_fields, ["gender", "age"])
            plat_pos_dev_rows = get_monthly_data("campaign", camp_fields, ["publisher_platform", "platform_position", "device_platform"])

            aude_debug("monthly adset x platform", adset_plat_rows)
            aude_debug("monthly adset x gender x age", adset_gen_age_rows)
            aude_debug("monthly platform x position x device", plat_pos_dev_rows)

            table = build_audiencedetail_monthly_table(
                adset_plat_rows,
                adset_gen_age_rows,
                plat_pos_dev_rows
            )
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote AUDIENCEDETAIL rows={len(table)-1}")

        elif kind == "AUDIENCESEGMENT":
            camp_fields = ["campaign_id", "campaign_name", "spend", "reach", "impressions", "actions", "action_values"]

            seg_attr = ["default"]
            breakdown_candidates = ["user_segment_key", "audience_segment", "user_persona_name"]

            def ause_debug(tag: str, rows: List[Dict[str, Any]], bd: str) -> None:
                print(f"[AUSE DEBUG] {tag}: rows={len(rows)} bd={bd}")
                if not rows:
                    return

                sample = rows[0]
                counts: Dict[str, int] = {}
                missing = 0
                for r in rows:
                    if bd not in r:
                        missing += 1
                        v = "__MISSING__"
                    else:
                        v = r.get(bd)
                    counts[str(v)] = counts.get(str(v), 0) + 1

                top3 = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:3]
                print(f"[AUSE DEBUG] {tag}: bd top3={top3} missing={missing}/{len(rows)}")

                acts = sample.get("actions")
                cv_val = 0
                sales_val = 0
                if isinstance(acts, list):
                    for a in acts:
                        if not isinstance(a, dict):
                            continue
                        if a.get("action_type") == TARGET_ACTION_CV:
                            cv_val = a.get("value", 0)
                        elif a.get("action_type") == TARGET_ACTION_SALES:
                            sales_val = a.get("value", 0)

                print(f"[AUSE DEBUG] {tag}: sample_action_values cv={cv_val} sales={sales_val}")

            def has_real_breakdown(rows: List[Dict[str, Any]], bd: str) -> bool:
                if not rows:
                    return False
                present = 0
                non_empty = 0
                for r in rows:
                    if bd in r:
                        present += 1
                        if r.get(bd) not in (None, ""):
                            non_empty += 1
                return present > 0 and non_empty > 0

            chosen_bd = None
            seg_rows: List[Dict[str, Any]] = []

            for bd in breakdown_candidates:
                try:
                    rows_try = get_monthly_data("campaign", camp_fields, [bd], attr_windows=seg_attr)
                except RuntimeError as e:
                    print(f"[AUSE DEBUG] breakdown '{bd}' API error: {e}")
                    continue

                ause_debug("monthly", rows_try, bd)

                if has_real_breakdown(rows_try, bd):
                    chosen_bd = bd
                    seg_rows = rows_try
                    break

            if not chosen_bd:
                chosen_bd = breakdown_candidates[0]
                seg_rows = get_monthly_data("campaign", camp_fields, [chosen_bd], attr_windows=seg_attr)
                ause_debug("monthly(fallback)", seg_rows, chosen_bd)

            table = build_audiencesegment_monthly_table(seg_rows, chosen_bd)
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote AUDIENCESEGMENT rows={len(table)-1} (bd={chosen_bd})")

        else:
            print(f"SKIP: sheet_kind '{kind}' is not implemented yet")


if __name__ == "__main__":
    main()
