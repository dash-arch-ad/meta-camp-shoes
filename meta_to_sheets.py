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

# ---------------------------------------------------------
# 取得したいアクションタイプ（管理画面の列名に合わせます）
# ---------------------------------------------------------
# 画像の「Website purchases」列に合わせる
TARGET_ACTION_CV = "offsite_conversion.fb_pixel_purchase" 

# 画像の「Purchases conversion value」列に合わせる
TARGET_ACTION_SALES = "purchase"
# ---------------------------------------------------------

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

def meta_get_insights(
    api_version: str,
    m_token: str,
    m_act_id: str,
    fields: List[str],
    date_preset: Optional[str] = None,
    time_range: Optional[Dict[str, str]] = None,
    action_attribution_windows: Optional[List[str]] = None,
    level: str = "campaign",
    limit: int = 500,
    max_pages: int = 50,
) -> List[Dict[str, Any]]:
    
    if (date_preset is None) == (time_range is None):
        raise ValueError("Specify exactly one of date_preset or time_range")

    act = _act_id_normalize(m_act_id)
    url = f"{GRAPH_BASE}/{api_version}/act_{act}/insights"

    params: Dict[str, Any] = {
        "access_token": m_token,
        "level": level,
        "fields": ",".join(fields),
        "limit": limit,
    }

    if date_preset is not None:
        params["date_preset"] = date_preset
    else:
        params["time_range"] = json.dumps(time_range, separators=(",", ":"))

    if action_attribution_windows:
        params["action_attribution_windows"] = json.dumps(action_attribution_windows, separators=(",", ":"))

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
    """指定したアクションタイプとアトリビューションウィンドウの数値をピンポイントで取得"""
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == target_action:
            try:
                return float(a.get(attr_window, 0))
            except (TypeError, ValueError):
                return 0.0
    return 0.0

def rows_to_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        cid = row.get("campaign_id")
        if not cid: continue

        name = row.get("campaign_name", "")
        spend = float(row.get("spend") or 0.0)
        reach = int(row.get("reach") or 0)

        actions = row.get("actions", [])
        action_values = row.get("action_values", [])

        # 1回のループで1d_viewと7d_clickの両方を抽出
        cv_1d = get_action_value(actions, TARGET_ACTION_CV, "1d_view")
        cv_7d = get_action_value(actions, TARGET_ACTION_CV, "7d_click")

        sales_1d = get_action_value(action_values, TARGET_ACTION_SALES, "1d_view")
        sales_7d = get_action_value(action_values, TARGET_ACTION_SALES, "7d_click")

        out[cid] = {
            "campaign_name": name,
            "spend": spend,
            "reach": reach,
            "cv_view_1d": cv_1d,
            "cv_click_7d": cv_7d,
            "sales_view_1d": sales_1d,
            "sales_click_7d": sales_7d,
        }
    return out

def build_monthly_table(last_data: Dict[str, Dict[str, Any]], this_data: Dict[str, Dict[str, Any]]) -> List[List[Any]]:
    all_ids = sorted(set(last_data.keys()) | set(this_data.keys()))

    header = [
        "campaign_id", "campaign_name",
        "last_month_cv_view_1d", "last_month_cv_click_7d",
        "last_month_sales_view_1d", "last_month_sales_click_7d",
        "last_month_spend", "last_month_reach",
        "last_month_cpa_click_7d", "last_month_roas_click_7d",
        "this_month_cv_view_1d", "this_month_cv_click_7d",
        "this_month_sales_view_1d", "this_month_sales_click_7d",
        "this_month_spend", "this_month_reach",
        "this_month_cpa_click_7d", "this_month_roas_click_7d",
    ]

    table = [header]

    def fmt(x: Any) -> Any:
        if x is None: return ""
        try: return round(float(x), 6)
        except: return ""

    for cid in all_ids:
        ld = last_data.get(cid, {})
        td = this_data.get(cid, {})

        name = td.get("campaign_name") or ld.get("campaign_name") or ""

        l_spend = ld.get("spend", 0.0)
        l_reach = ld.get("reach", 0)
        l_cv_1d = ld.get("cv_view_1d", 0.0)
        l_cv_7d = ld.get("cv_click_7d", 0.0)
        l_sales_1d = ld.get("sales_view_1d", 0.0)
        l_sales_7d = ld.get("sales_click_7d", 0.0)
        l_cpa = (l_spend / l_cv_7d) if l_cv_7d > 0 else None
        l_roas = (l_sales_7d / l_spend) if l_spend > 0 else None

        t_spend = td.get("spend", 0.0)
        t_reach = td.get("reach", 0)
        t_cv_1d = td.get("cv_view_1d", 0.0)
        t_cv_7d = td.get("cv_click_7d", 0.0)
        t_sales_1d = td.get("sales_view_1d", 0.0)
        t_sales_7d = td.get("sales_click_7d", 0.0)
        t_cpa = (t_spend / t_cv_7d) if t_cv_7d > 0 else None
        t_roas = (t_sales_7d / t_spend) if t_spend > 0 else None

        table.append([
            cid, name,
            fmt(l_cv_1d), fmt(l_cv_7d), fmt(l_sales_1d), fmt(l_sales_7d),
            fmt(l_spend) if l_spend or l_spend == 0.0 else "", 
            fmt(l_reach) if l_reach or l_reach == 0 else "",
            fmt(l_cpa), fmt(l_roas),
            fmt(t_cv_1d), fmt(t_cv_7d), fmt(t_sales_1d), fmt(t_sales_7d),
            fmt(t_spend) if t_spend or t_spend == 0.0 else "", 
            fmt(t_reach) if t_reach or t_reach == 0 else "",
            fmt(t_cpa), fmt(t_roas),
        ])
    return table

def sheets_write(spreadsheet_id: str, worksheet_title: str, values_2d: List[List[Any]], g_creds: Dict[str, Any]) -> None:
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(g_creds, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)

    ss = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = {s["properties"]["title"] for s in ss.get("sheets", [])}
    if worksheet_title not in titles:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": worksheet_title}}}]},
        ).execute()

    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{worksheet_title}!A:Z",
        body={},
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{worksheet_title}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": values_2d},
    ).execute()

def main():
    raw = os.environ.get("APP_SECRET_JSON")
    if not raw:
        raise RuntimeError("Missing env APP_SECRET_JSON")

    cfg = json.loads(raw)

    m_token = cfg["m_token"]
    m_act_id = cfg["m_act_id"]
    s_id = cfg["s_id"]
    sheets_map = cfg.get("sheets", {})
    g_creds = cfg["g_creds"]

    api_version = cfg.get("m_api_version", "v20.0")

    fields = [
        "campaign_id",
        "campaign_name",
        "spend",
        "reach",
        "actions",
        "action_values",
    ]

    for sheet_kind, worksheet_title in sheets_map.items():
        kind = str(sheet_kind).strip().upper()

        if kind == "MONTHLY":
            # --- 先月分（1回のリクエストで1d_viewと7d_clickを取得） ---
            last_rows = meta_get_insights(
                api_version=api_version,
                m_token=m_token,
                m_act_id=m_act_id,
                fields=fields,
                date_preset="last_month",
                action_attribution_windows=["1d_view", "7d_click"],
            )
            last_data = rows_to_metrics(last_rows)

            # --- 今月分（1回のリクエストで1d_viewと7d_clickを取得） ---
            rng = this_month_range_to_yesterday_jst()
            if rng is None:
                this_data = {}
            else:
                since, until = rng
                this_rows = meta_get_insights(
                    api_version=api_version,
                    m_token=m_token,
                    m_act_id=m_act_id,
                    fields=fields,
                    time_range={"since": since, "until": until},
                    action_attribution_windows=["1d_view", "7d_click"],
                )
                this_data = rows_to_metrics(this_rows)

            table = build_monthly_table(last_data, this_data)
            sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote MONTHLY to sheet '{worksheet_title}' rows={len(table)-1}")

        else:
            print(f"SKIP: sheet_kind '{kind}' is not implemented yet")

if __name__ == "__main__":
    main()
