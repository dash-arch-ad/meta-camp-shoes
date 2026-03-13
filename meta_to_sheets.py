import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple, Callable

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
    "Month", "campaign_name",
    "cv_view_1d", "cv_click_7d",
    "sales_view_1d", "sales_click_7d",
    "cpa_click_7d", "roas_click_7d",
]

METRIC_HEADERS = [
    "last_month_impressions", "last_month_reach", "last_month_spend",
    "last_month_cv_view_1d", "last_month_cv_click_7d",
    "last_month_sales_view_1d", "last_month_sales_click_7d",
    "last_month_cpa_click_7d", "last_month_roas_click_7d",
    "this_month_impressions", "this_month_reach", "this_month_spend",
    "this_month_cv_view_1d", "this_month_cv_click_7d",
    "this_month_sales_view_1d", "this_month_sales_click_7d",
    "this_month_cpa_click_7d", "this_month_roas_click_7d",
]

# auseシート専用のヘッダー
AUSE_METRIC_HEADERS = [
    "last_month_impressions", "last_month_spend", "last_month_cv",
    "last_month_link_clicks", "last_month_clicks_all",
    "last_month_add_to_cart", "last_month_leads",
    "last_month_post_reactions", "last_month_post_comments",
    "last_month_post_saves", "last_month_post_shares",
    "this_month_impressions", "this_month_spend", "this_month_cv",
    "this_month_link_clicks", "this_month_clicks_all",
    "this_month_add_to_cart", "this_month_leads",
    "this_month_post_reactions", "this_month_post_comments",
    "this_month_post_saves", "this_month_post_shares",
]

AUSE_ACTION_TYPE_CANDIDATES = {
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

AUSE_SUM_KEYS = [
    "impressions", "spend", "cv_1d", "cv_7d",
    "link_clicks", "clicks_all",
    "add_to_cart", "leads",
    "post_reactions", "post_comments",
    "post_saves", "post_shares",
]


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
    actions = row.get("actions", [])
    return {
        "spend": float(row.get("spend") or 0.0),
        "reach": int(row.get("reach") or 0),
        "impressions": int(row.get("impressions") or 0),
        "cv_1d": get_action_value(actions, TARGET_ACTION_CV, attr_window_cv),
        "cv_7d": get_action_value(actions, TARGET_ACTION_CV, attr_window_cv_click),
        "sales_1d": get_action_value(row.get("action_values", []), TARGET_ACTION_SALES, attr_window_cv),
        "sales_7d": get_action_value(row.get("action_values", []), TARGET_ACTION_SALES, attr_window_cv_click),
        "link_clicks": float(row.get("inline_link_clicks") or 0.0),
        "clicks_all": float(row.get("clicks") or 0.0),
        "add_to_cart": get_action_value_multi(actions, AUSE_ACTION_TYPE_CANDIDATES["add_to_cart"], attr_window_cv_click),
        "leads": get_action_value_multi(actions, AUSE_ACTION_TYPE_CANDIDATES["leads"], attr_window_cv_click),
        "post_reactions": get_action_value_multi(actions, AUSE_ACTION_TYPE_CANDIDATES["post_reactions"], attr_window_cv_click),
        "post_comments": get_action_value_multi(actions, AUSE_ACTION_TYPE_CANDIDATES["post_comments"], attr_window_cv_click),
        "post_saves": get_action_value_multi(actions, AUSE_ACTION_TYPE_CANDIDATES["post_saves"], attr_window_cv_click),
        "post_shares": get_action_value_multi(actions, AUSE_ACTION_TYPE_CANDIDATES["post_shares"], attr_window_cv_click),
    }


def map_by_key(rows: List[Dict], key_func: Callable[[Dict], Any], is_ause: bool = False) -> Dict[str, Dict]:
    res = {}
    for r in rows:
        k = key_func(r)
        if not k:
            continue
        metrics = extract_metrics(r, "value", "value") if is_ause else extract_metrics(r)
        res[k] = {"dim": r, "metrics": metrics}
    return res


def compute_metric_row(ld: Dict[str, Any], td: Dict[str, Any]) -> List[Any]:
    def fmt(x: Any) -> Any:
        if x is None:
            return ""
        try:
            return round(float(x), 6)
        except:
            return ""

    l_imp, l_reach, l_spend = ld.get("impressions", 0), ld.get("reach", 0), ld.get("spend", 0.0)
    l_cv_1d, l_cv_7d = ld.get("cv_1d", 0.0), ld.get("cv_7d", 0.0)
    l_sales_1d, l_sales_7d = ld.get("sales_1d", 0.0), ld.get("sales_7d", 0.0)
    l_cpa = (l_spend / l_cv_7d) if l_cv_7d > 0 else None
    l_roas = (l_sales_7d / l_spend) if l_spend > 0 else None

    t_imp, t_reach, t_spend = td.get("impressions", 0), td.get("reach", 0), td.get("spend", 0.0)
    t_cv_1d, t_cv_7d = td.get("cv_1d", 0.0), td.get("cv_7d", 0.0)
    t_sales_1d, t_sales_7d = td.get("sales_1d", 0.0), td.get("sales_7d", 0.0)
    t_cpa = (t_spend / t_cv_7d) if t_cv_7d > 0 else None
    t_roas = (t_sales_7d / t_spend) if t_spend > 0 else None

    return [
        fmt(l_imp), fmt(l_reach), fmt(l_spend),
        fmt(l_cv_1d), fmt(l_cv_7d), fmt(l_sales_1d), fmt(l_sales_7d),
        fmt(l_cpa), fmt(l_roas),
        fmt(t_imp), fmt(t_reach), fmt(t_spend),
        fmt(t_cv_1d), fmt(t_cv_7d), fmt(t_sales_1d), fmt(t_sales_7d),
        fmt(t_cpa), fmt(t_roas),
    ]


def compute_ause_metric_row(ld: Dict[str, Any], td: Dict[str, Any]) -> List[Any]:
    def fmt(x: Any) -> Any:
        if x is None:
            return ""
        try:
            return round(float(x), 6)
        except:
            return ""

    l_imp, l_spend = ld.get("impressions", 0), ld.get("spend", 0.0)
    l_cv_1d = ld.get("cv_1d", 0.0)
    l_link_clicks = ld.get("link_clicks", 0.0)
    l_clicks_all = ld.get("clicks_all", 0.0)
    l_add_to_cart = ld.get("add_to_cart", 0.0)
    l_leads = ld.get("leads", 0.0)
    l_post_reactions = ld.get("post_reactions", 0.0)
    l_post_comments = ld.get("post_comments", 0.0)
    l_post_saves = ld.get("post_saves", 0.0)
    l_post_shares = ld.get("post_shares", 0.0)

    t_imp, t_spend = td.get("impressions", 0), td.get("spend", 0.0)
    t_cv_1d = td.get("cv_1d", 0.0)
    t_link_clicks = td.get("link_clicks", 0.0)
    t_clicks_all = td.get("clicks_all", 0.0)
    t_add_to_cart = td.get("add_to_cart", 0.0)
    t_leads = td.get("leads", 0.0)
    t_post_reactions = td.get("post_reactions", 0.0)
    t_post_comments = td.get("post_comments", 0.0)
    t_post_saves = td.get("post_saves", 0.0)
    t_post_shares = td.get("post_shares", 0.0)

    return [
        fmt(l_imp), fmt(l_spend), fmt(l_cv_1d),
        fmt(l_link_clicks), fmt(l_clicks_all),
        fmt(l_add_to_cart), fmt(l_leads),
        fmt(l_post_reactions), fmt(l_post_comments),
        fmt(l_post_saves), fmt(l_post_shares),
        fmt(t_imp), fmt(t_spend), fmt(t_cv_1d),
        fmt(t_link_clicks), fmt(t_clicks_all),
        fmt(t_add_to_cart), fmt(t_leads),
        fmt(t_post_reactions), fmt(t_post_comments),
        fmt(t_post_saves), fmt(t_post_shares),
    ]


def build_campaign_table(last_map: Dict, this_map: Dict) -> List[List[Any]]:
    header = ["campaign_id", "campaign_name"] + METRIC_HEADERS
    table = [header]
    for k in sorted(set(last_map.keys()) | set(this_map.keys())):
        ld, td = last_map.get(k, {}), this_map.get(k, {})
        dim = td.get("dim") or ld.get("dim") or {}
        row = [k, dim.get("campaign_name", "")]
        row.extend(compute_metric_row(ld.get("metrics", {}), td.get("metrics", {})))
        table.append(row)
    return table


def build_ad_table(last_map: Dict, this_map: Dict) -> List[List[Any]]:
    header = ["campaign_name", "adset_name", "ad_id", "ad_name"] + METRIC_HEADERS
    table = [header]
    for k in sorted(set(last_map.keys()) | set(this_map.keys())):
        ld, td = last_map.get(k, {}), this_map.get(k, {})
        dim = td.get("dim") or ld.get("dim") or {}
        row = [dim.get("campaign_name", ""), dim.get("adset_name", ""), k, dim.get("ad_name", "")]
        row.extend(compute_metric_row(ld.get("metrics", {}), td.get("metrics", {})))
        table.append(row)
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
            fmt(m["cv_1d"]),
            fmt(m["cv_7d"]),
            fmt(m["sales_1d"]),
            fmt(m["sales_7d"]),
            fmt(cpa),
            fmt(roas),
        ])

    return table


def build_audience_table(
    l_adset, t_adset, l_camp, t_camp, l_gender, t_gender, l_age, t_age, l_plat, t_plat
) -> List[List[Any]]:
    header = ["Category", "Campaign Name", "Breakdown"] + METRIC_HEADERS
    table = [header]

    def add_rows(last_m, this_m, cat_name, camp_fn, detail_fn):
        for k in sorted(set(last_m.keys()) | set(this_m.keys())):
            ld, td = last_m.get(k, {}), this_m.get(k, {})
            dim = td.get("dim") or ld.get("dim") or {}
            row = [cat_name, camp_fn(k, dim), detail_fn(k, dim)]
            row.extend(compute_metric_row(ld.get("metrics", {}), td.get("metrics", {})))
            table.append(row)

    add_rows(l_adset, t_adset, "AdSet", lambda k, d: d.get("campaign_name", ""), lambda k, d: d.get("adset_name", ""))
    add_rows(l_camp, t_camp, "Campaign Total", lambda k, d: d.get("campaign_name", ""), lambda k, d: "Total")
    add_rows(l_gender, t_gender, "Gender", lambda k, d: d.get("campaign_name", ""), lambda k, d: d.get("gender", ""))
    add_rows(l_age, t_age, "Age", lambda k, d: d.get("campaign_name", ""), lambda k, d: d.get("age", ""))
    add_rows(l_plat, t_plat, "Platform", lambda k, d: d.get("campaign_name", ""), lambda k, d: d.get("publisher_platform", ""))

    return table


def build_audiencedetail_table(
    l_adset_plat, t_adset_plat,
    l_adset_gen_age, t_adset_gen_age,
    l_plat_pos_dev, t_plat_pos_dev
) -> List[List[Any]]:
    header = ["Category", "Detail1", "Detail2", "Detail3"] + METRIC_HEADERS
    table = [header]

    def add_rows(last_m, this_m, cat_name, d1_fn, d2_fn, d3_fn):
        for k in sorted(set(last_m.keys()) | set(this_m.keys())):
            ld, td = last_m.get(k, {}), this_m.get(k, {})
            dim = td.get("dim") or ld.get("dim") or {}
            row = [cat_name, d1_fn(k, dim), d2_fn(k, dim), d3_fn(k, dim)]
            row.extend(compute_metric_row(ld.get("metrics", {}), td.get("metrics", {})))
            table.append(row)

    add_rows(l_adset_plat, t_adset_plat, "AdSet x Platform",
             lambda k, d: d.get("adset_name", ""),
             lambda k, d: d.get("publisher_platform", ""),
             lambda k, d: "")

    add_rows(l_adset_gen_age, t_adset_gen_age, "AdSet x Gender x Age",
             lambda k, d: d.get("adset_name", ""),
             lambda k, d: d.get("gender", ""),
             lambda k, d: d.get("age", ""))

    add_rows(l_plat_pos_dev, t_plat_pos_dev, "Platform x Position x Device",
             lambda k, d: d.get("publisher_platform", ""),
             lambda k, d: d.get("platform_position", ""),
             lambda k, d: d.get("impression_device", ""))

    return table


def build_audiencesegment_table(l_camp_seg, t_camp_seg, breakdown_key) -> List[List[Any]]:
    header = ["Category", "Campaign Name", "Audience Segment"] + AUSE_METRIC_HEADERS
    table = [header]

    seg_totals = {"last": {}, "this": {}}

    def add_to_totals(period, persona, metrics):
        if persona not in seg_totals[period]:
            seg_totals[period][persona] = {k: 0.0 for k in AUSE_SUM_KEYS}
        t = seg_totals[period][persona]
        for key in AUSE_SUM_KEYS:
            t[key] += metrics.get(key, 0.0)

    for k in sorted(set(l_camp_seg.keys()) | set(t_camp_seg.keys())):
        ld, td = l_camp_seg.get(k, {}), t_camp_seg.get(k, {})
        dim = td.get("dim") or ld.get("dim") or {}
        persona = dim.get(breakdown_key, "__MISSING__")

        if ld:
            add_to_totals("last", persona, ld.get("metrics", {}))
        if td:
            add_to_totals("this", persona, td.get("metrics", {}))

        row = ["Campaign", dim.get("campaign_name", ""), persona]
        row.extend(compute_ause_metric_row(ld.get("metrics", {}), td.get("metrics", {})))
        table.append(row)

    all_personas = sorted(set(seg_totals["last"].keys()) | set(seg_totals["this"].keys()))
    for p in all_personas:
        ld_metrics = seg_totals["last"].get(p, {})
        td_metrics = seg_totals["this"].get(p, {})
        row = ["Total", "All Campaigns Sum", p]
        row.extend(compute_ause_metric_row(ld_metrics, td_metrics))
        table.append(row)

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

    data_cache = {"last": {}, "this": {}}

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

    for sheet_kind, worksheet_title in sheets_map.items():
        kind = str(sheet_kind).strip().upper()
        print(f"Processing {kind} to sheet '{worksheet_title}'...")

        if kind == "MONTHLY":
            fields = ["campaign_id", "campaign_name", "spend", "actions", "action_values"]
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
            last_map = map_by_key(get_data("last", "ad", fields), lambda r: r.get("ad_id"))
            this_map = map_by_key(get_data("this", "ad", fields), lambda r: r.get("ad_id"))

            table = build_ad_table(last_map, this_map)
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote AD rows={len(table)-1}")

        elif kind == "AUDIENCE":
            adset_fields = ["campaign_name", "adset_id", "adset_name", "spend", "reach", "impressions", "actions", "action_values"]
            camp_fields = ["campaign_id", "campaign_name", "spend", "reach", "impressions", "actions", "action_values"]

            l_adset = map_by_key(get_data("last", "adset", adset_fields), lambda r: r.get("adset_id"))
            t_adset = map_by_key(get_data("this", "adset", adset_fields), lambda r: r.get("adset_id"))

            l_camp = map_by_key(get_data("last", "campaign", camp_fields), lambda r: r.get("campaign_id"))
            t_camp = map_by_key(get_data("this", "campaign", camp_fields), lambda r: r.get("campaign_id"))

            l_gender = map_by_key(get_data("last", "campaign", camp_fields, ["gender"]), lambda r: f"{r.get('campaign_id')}_{r.get('gender')}")
            t_gender = map_by_key(get_data("this", "campaign", camp_fields, ["gender"]), lambda r: f"{r.get('campaign_id')}_{r.get('gender')}")

            l_age = map_by_key(get_data("last", "campaign", camp_fields, ["age"]), lambda r: f"{r.get('campaign_id')}_{r.get('age')}")
            t_age = map_by_key(get_data("this", "campaign", camp_fields, ["age"]), lambda r: f"{r.get('campaign_id')}_{r.get('age')}")

            l_plat = map_by_key(get_data("last", "campaign", camp_fields, ["publisher_platform"]), lambda r: f"{r.get('campaign_id')}_{r.get('publisher_platform')}")
            t_plat = map_by_key(get_data("this", "campaign", camp_fields, ["publisher_platform"]), lambda r: f"{r.get('campaign_id')}_{r.get('publisher_platform')}")

            table = build_audience_table(l_adset, t_adset, l_camp, t_camp, l_gender, t_gender, l_age, t_age, l_plat, t_plat)
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote AUDIENCE rows={len(table)-1}")

        elif kind == "AUDIENCEDETAIL":
            adset_fields = ["campaign_name", "adset_id", "adset_name", "spend", "reach", "impressions", "actions", "action_values"]
            acc_fields = ["spend", "reach", "impressions", "actions", "action_values"]

            l_adset_plat = map_by_key(get_data("last", "adset", adset_fields, ["publisher_platform"]), lambda r: f"{r.get('adset_id')}_{r.get('publisher_platform')}")
            t_adset_plat = map_by_key(get_data("this", "adset", adset_fields, ["publisher_platform"]), lambda r: f"{r.get('adset_id')}_{r.get('publisher_platform')}")

            l_adset_gen_age = map_by_key(get_data("last", "adset", adset_fields, ["gender", "age"]), lambda r: f"{r.get('adset_id')}_{r.get('gender')}_{r.get('age')}")
            t_adset_gen_age = map_by_key(get_data("this", "adset", adset_fields, ["gender", "age"]), lambda r: f"{r.get('adset_id')}_{r.get('gender')}_{r.get('age')}")

            l_plat_pos_dev = map_by_key(get_data("last", "account", acc_fields, ["publisher_platform", "platform_position", "impression_device"]), lambda r: f"{r.get('publisher_platform')}_{r.get('platform_position')}_{r.get('impression_device')}")
            t_plat_pos_dev = map_by_key(get_data("this", "account", acc_fields, ["publisher_platform", "platform_position", "impression_device"]), lambda r: f"{r.get('publisher_platform')}_{r.get('platform_position')}_{r.get('impression_device')}")

            table = build_audiencedetail_table(
                l_adset_plat, t_adset_plat,
                l_adset_gen_age, t_adset_gen_age,
                l_plat_pos_dev, t_plat_pos_dev
            )
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote AUDIENCEDETAIL rows={len(table)-1}")

        # --- 修正: auseシート取得時のみ breakdown候補を検証し、actionsログで原因特定できるようにする ---
        elif kind == "AUDIENCESEGMENT":
            camp_fields = [
                "campaign_id", "campaign_name", "spend", "reach", "impressions",
                "clicks", "inline_link_clicks", "actions", "action_values"
            ]

            # ログの許可リストに基づき "default" を指定（維持）
            seg_attr = ["default"]

            # 公式/互換の候補（user_segment_key を最優先）
            breakdown_candidates = ["user_segment_key", "audience_segment", "user_persona_name"]

            def ause_debug(tag: str, rows: List[Dict[str, Any]], bd: str) -> None:
                print(f"[AUSE DEBUG] {tag}: rows={len(rows)} bd={bd}")
                if not rows:
                    return

                sample = rows[0]
                counts: Dict[str, int] = {}
                missing = 0
                action_type_counts: Dict[str, int] = {}
                for r in rows:
                    if bd not in r:
                        missing += 1
                        v = "__MISSING__"
                    else:
                        v = r.get(bd)
                    counts[str(v)] = counts.get(str(v), 0) + 1

                    acts = r.get("actions")
                    if isinstance(acts, list):
                        for a in acts:
                            if not isinstance(a, dict):
                                continue
                            at = str(a.get("action_type") or "__NONE__")
                            action_type_counts[at] = action_type_counts.get(at, 0) + 1

                top3 = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:3]
                print(f"[AUSE DEBUG] {tag}: bd top3={top3} missing={missing}/{len(rows)}")

                top_action_types = sorted(action_type_counts.items(), key=lambda x: x[1], reverse=True)[:15]
                print(f"[AUSE DEBUG] {tag}: action_type top15={top_action_types}")
                print(
                    f"[AUSE DEBUG] {tag}: sample direct metrics "
                    f"clicks={sample.get('clicks', 0)} inline_link_clicks={sample.get('inline_link_clicks', 0)}"
                )

                acts = sample.get("actions") if isinstance(sample.get("actions"), list) else []
                sample_action_map: Dict[str, Any] = {}
                for a in acts:
                    if not isinstance(a, dict):
                        continue
                    at = a.get("action_type")
                    if at is not None:
                        sample_action_map[str(at)] = a.get("value", 0)

                print(
                    f"[AUSE DEBUG] {tag}: sample_action_values "
                    f"cv={sample_action_map.get(TARGET_ACTION_CV, 0)} sales={sample_action_map.get(TARGET_ACTION_SALES, 0)}"
                )

                for metric_name, candidates in AUSE_ACTION_TYPE_CANDIDATES.items():
                    matched = {c: sample_action_map.get(c, 0) for c in candidates if c in sample_action_map}
                    print(f"[AUSE DEBUG] {tag}: sample_matches {metric_name}={matched or 'NO_MATCH'}")

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
            l_rows: List[Dict[str, Any]] = []
            t_rows: List[Dict[str, Any]] = []

            for bd in breakdown_candidates:
                try:
                    l_try = get_data("last", "campaign", camp_fields, [bd], attr_windows=seg_attr)
                    t_try = get_data("this", "campaign", camp_fields, [bd], attr_windows=seg_attr)
                except RuntimeError as e:
                    print(f"[AUSE DEBUG] breakdown '{bd}' API error: {e}")
                    continue

                ause_debug("last", l_try, bd)
                ause_debug("this", t_try, bd)

                if has_real_breakdown(l_try, bd) or has_real_breakdown(t_try, bd):
                    chosen_bd = bd
                    l_rows, t_rows = l_try, t_try
                    break

            if not chosen_bd:
                chosen_bd = breakdown_candidates[0]
                l_rows = get_data("last", "campaign", camp_fields, [chosen_bd], attr_windows=seg_attr)
                t_rows = get_data("this", "campaign", camp_fields, [chosen_bd], attr_windows=seg_attr)
                ause_debug("last(fallback)", l_rows, chosen_bd)
                ause_debug("this(fallback)", t_rows, chosen_bd)

            def ause_map_key(r: Dict[str, Any]) -> str:
                seg = r.get(chosen_bd)
                if seg in (None, ""):
                    seg = "__MISSING__"
                cid = r.get("campaign_id") or "__NO_CAMP__"
                return f"{cid}_{seg}"

            l_camp_seg = map_by_key(l_rows, ause_map_key, is_ause=True)
            t_camp_seg = map_by_key(t_rows, ause_map_key, is_ause=True)

            table = build_audiencesegment_table(l_camp_seg, t_camp_seg, chosen_bd)
            for s_id in s_id_list:
                sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote AUDIENCESEGMENT rows={len(table)-1} (bd={chosen_bd})")

        else:
            print(f"SKIP: sheet_kind '{kind}' is not implemented yet")


if __name__ == "__main__":
    main()
