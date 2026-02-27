import os
import json
import time
from typing import Any, Dict, List, Optional

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

GRAPH_BASE = "https://graph.facebook.com"

# ---- Meta: CV/ROAS のデフォルト定義（必要なら調整）----
DEFAULT_CV_ACTION_TYPES = [
    "lead",
    "omni_lead",
    "offsite_conversion.fb_pixel_lead",
    "offsite_conversion.lead",
    "onsite_conversion.lead_grouped",
]

DEFAULT_VALUE_ACTION_TYPES_FOR_ROAS = [
    "purchase",
    "omni_purchase",
    "offsite_conversion.purchase",
    "web_in_store_purchase",
]


def _act_id_normalize(m_act_id: str) -> str:
    s = str(m_act_id).strip()
    return s[4:] if s.startswith("act_") else s


def meta_get_insights(
    api_version: str,
    m_token: str,
    m_act_id: str,
    date_preset: str,
    fields: List[str],
    level: str = "campaign",
    limit: int = 500,
    max_pages: int = 50,
) -> List[Dict[str, Any]]:
    act = _act_id_normalize(m_act_id)
    url = f"{GRAPH_BASE}/{api_version}/act_{act}/insights"

    params = {
        "access_token": m_token,
        "level": level,
        "date_preset": date_preset,
        "fields": ",".join(fields),
        "limit": limit,
    }

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


def sum_action_list(actions: Optional[List[Dict[str, str]]], wanted: List[str]) -> float:
    if not actions:
        return 0.0
    wanted_set = set(wanted)
    total = 0.0
    for a in actions:
        if a.get("action_type") in wanted_set:
            try:
                total += float(a.get("value", 0))
            except (TypeError, ValueError):
                pass
    return total


def pick_purchase_roas(purchase_roas: Optional[List[Dict[str, str]]]) -> Optional[float]:
    if not purchase_roas:
        return None
    total = 0.0
    found = False
    for x in purchase_roas:
        try:
            total += float(x.get("value", 0))
            found = True
        except (TypeError, ValueError):
            pass
    return total if found else None


def build_monthly_table(
    last_rows: List[Dict[str, Any]],
    this_rows: List[Dict[str, Any]],
    cv_action_types: List[str],
    roas_value_action_types: List[str],
) -> List[List[Any]]:
    def to_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            cid = row.get("campaign_id")
            if not cid:
                continue

            name = row.get("campaign_name", "")
            spend = float(row.get("spend") or 0.0)

            actions = row.get("actions")
            action_values = row.get("action_values")
            purchase_roas = row.get("purchase_roas")

            cv = sum_action_list(actions, cv_action_types)
            cpa = (spend / cv) if cv > 0 else None

            roas = pick_purchase_roas(purchase_roas)
            if roas is None:
                value = sum_action_list(action_values, roas_value_action_types)
                roas = (value / spend) if spend > 0 else None

            out[cid] = {
                "campaign_id": cid,
                "campaign_name": name,
                "spend": spend,
                "cv": cv,
                "cpa": cpa,
                "roas": roas,
            }
        return out

    last = to_metrics(last_rows)
    this_ = to_metrics(this_rows)

    all_ids = sorted(set(last.keys()) | set(this_.keys()))

    header = [
        "campaign_id",
        "campaign_name",
        "last_month_cv",
        "last_month_cpa",
        "last_month_roas",
        "last_month_spend",
        "this_month_cv",
        "this_month_cpa",
        "this_month_roas",
        "this_month_spend",
    ]
    table: List[List[Any]] = [header]

    def fmt_num(x: Any) -> Any:
        if x is None:
            return ""
        try:
            return round(float(x), 6)
        except (TypeError, ValueError):
            return ""

    for cid in all_ids:
        lm = last.get(cid, {})
        tm = this_.get(cid, {})
        name = (tm.get("campaign_name") or lm.get("campaign_name") or "")

        table.append([
            cid,
            name,
            fmt_num(lm.get("cv", 0.0)),
            fmt_num(lm.get("cpa")),
            fmt_num(lm.get("roas")),
            fmt_num(lm.get("spend", 0.0)),
            fmt_num(tm.get("cv", 0.0)),
            fmt_num(tm.get("cpa")),
            fmt_num(tm.get("roas")),
            fmt_num(tm.get("spend", 0.0)),
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

    cv_action_types = cfg.get("cv_action_types", DEFAULT_CV_ACTION_TYPES)
    roas_value_action_types = cfg.get("roas_value_action_types", DEFAULT_VALUE_ACTION_TYPES_FOR_ROAS)

    api_version = cfg.get("m_api_version", "v25.0")

    fields = [
        "campaign_id",
        "campaign_name",
        "spend",
        "actions",
        "action_values",
        "purchase_roas",
    ]

    for sheet_kind, worksheet_title in sheets_map.items():
        kind = str(sheet_kind).strip().upper()

        if kind == "MONTHLY":
            last_rows = meta_get_insights(api_version, m_token, m_act_id, "last_month", fields)
            this_rows = meta_get_insights(api_version, m_token, m_act_id, "this_month", fields)
            table = build_monthly_table(last_rows, this_rows, cv_action_types, roas_value_action_types)
            sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote MONTHLY to sheet '{worksheet_title}' rows={len(table)-1}")

        else:
            print(f"SKIP: sheet_kind '{kind}' is not implemented yet (worksheet='{worksheet_title}')")


if __name__ == "__main__":
    main()
