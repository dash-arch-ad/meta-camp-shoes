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


# Ads Managerの「Purchase」がアカウント/設定によって複数action_typeに分かれることがあるため、
# 実務でズレにくいように代表例を広めに含めます。
# もし「purchaseだけ」に絞りたいなら、["purchase"] のみにしてください。
DEFAULT_PURCHASE_ACTION_TYPES = [
    "purchase",
    "omni_purchase",
    "offsite_conversion.purchase",
    "web_in_store_purchase",
]


def _act_id_normalize(m_act_id: str) -> str:
    s = str(m_act_id).strip()
    return s[4:] if s.startswith("act_") else s


def this_month_range_to_yesterday_jst() -> Optional[Tuple[str, str]]:
    """
    JST基準で:
      since = 当月1日
      until = 前日
    todayが1日の場合は範囲が作れないのでNone
    """
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
    """
    Either:
      - date_preset="last_month" etc
    Or:
      - time_range={"since":"YYYY-MM-DD","until":"YYYY-MM-DD"}

    For attribution-specific actions/action_values:
      - action_attribution_windows=["1d_view"] or ["7d_click"] etc
    """
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
        # time_range はJSON文字列で渡すのが安定
        params["time_range"] = json.dumps(time_range, separators=(",", ":"))

    if action_attribution_windows:
        # attribution windows もJSON配列で渡すのが安定
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
        params = None  # next URL already includes query params
        time.sleep(0.2)

    return out


def sum_action_list(actions: Optional[List[Dict[str, Any]]], wanted: List[str], attr_window: str = "value") -> float:
    if not actions:
        return 0.0
    wanted_set = set(wanted)
    total = 0.0
    for a in actions:
        if a.get("action_type") in wanted_set:
            try:
                # デフォルトの 'value' ではなく、指定されたアトリビューションキーの値を取得する
                total += float(a.get(attr_window, 0))
            except (TypeError, ValueError):
                pass
    return total


def rows_to_purchase_metrics(
    rows: List[Dict[str, Any]],
    purchase_action_types: List[str],
    attr_window: str = "value"
) -> Dict[str, Dict[str, Any]]:
    """
    Returns dict keyed by campaign_id:
      {
        cid: {
          campaign_id, campaign_name,
          spend, reach,
          purchase_cv,   # Purchase count
          purchase_value # Purchase value (revenue)
        }
      }
    """
    out: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        cid = row.get("campaign_id")
        if not cid:
            continue

        name = row.get("campaign_name", "")
        spend = float(row.get("spend") or 0.0)
        
        # 修正: reachを取得 (APIからは文字列で返ってくるためfloat/int変換用に取得)
        reach = int(row.get("reach") or 0)

        actions = row.get("actions")          # counts
        action_values = row.get("action_values")  # values

        # どのアトリビューションウィンドウの値を取得するか指定
        purchase_cv = sum_action_list(actions, purchase_action_types, attr_window)
        purchase_value = sum_action_list(action_values, purchase_action_types, attr_window)

        out[cid] = {
            "campaign_id": cid,
            "campaign_name": name,
            "spend": spend,
            "reach": reach, # 修正: 辞書に保存
            "purchase_cv": purchase_cv,
            "purchase_value": purchase_value,
        }

    return out


def build_monthly_table_purchase_attr(
    last_view: Dict[str, Dict[str, Any]],
    last_click: Dict[str, Dict[str, Any]],
    this_view: Dict[str, Dict[str, Any]],
    this_click: Dict[str, Dict[str, Any]],
) -> List[List[Any]]:
    """
    出力列:
      - 先月（フル月）: CV(view1d), CV(click7d), 売上(view1d), 売上(click7d), spend, reach, CPA/ROAS(click7d)
      - 今月（当月1日〜前日）: 同上
    """
    all_ids = sorted(set(last_view.keys()) | set(last_click.keys()) | set(this_view.keys()) | set(this_click.keys()))

    # 修正: ヘッダーに reach を追加
    header = [
        "campaign_id",
        "campaign_name",

        "last_month_cv_view_1d",
        "last_month_cv_click_7d",
        "last_month_sales_view_1d",
        "last_month_sales_click_7d",
        "last_month_spend",
        "last_month_reach",
        "last_month_cpa_click_7d",
        "last_month_roas_click_7d",

        "this_month_cv_view_1d",
        "this_month_cv_click_7d",
        "this_month_sales_view_1d",
        "this_month_sales_click_7d",
        "this_month_spend",
        "this_month_reach",
        "this_month_cpa_click_7d",
        "this_month_roas_click_7d",
    ]

    def fmt(x: Any) -> Any:
        if x is None:
            return ""
        try:
            return round(float(x), 6)
        except (TypeError, ValueError):
            return ""

    table: List[List[Any]] = [header]

    for cid in all_ids:
        # campaign_name はどれかにあればそれを採用
        name = (
            (this_click.get(cid) or {}).get("campaign_name")
            or (this_view.get(cid) or {}).get("campaign_name")
            or (last_click.get(cid) or {}).get("campaign_name")
            or (last_view.get(cid) or {}).get("campaign_name")
            or ""
        )

        # spend, reach は attribution window で変わらないはずなので click側を優先、なければview側
        lm_spend = (last_click.get(cid) or last_view.get(cid) or {}).get("spend")
        lm_reach = (last_click.get(cid) or last_view.get(cid) or {}).get("reach")
        tm_spend = (this_click.get(cid) or this_view.get(cid) or {}).get("spend")
        tm_reach = (this_click.get(cid) or this_view.get(cid) or {}).get("reach")

        lm_cv_view = (last_view.get(cid) or {}).get("purchase_cv")
        lm_cv_click = (last_click.get(cid) or {}).get("purchase_cv")
        lm_sales_view = (last_view.get(cid) or {}).get("purchase_value")
        lm_sales_click = (last_click.get(cid) or {}).get("purchase_value")

        tm_cv_view = (this_view.get(cid) or {}).get("purchase_cv")
        tm_cv_click = (this_click.get(cid) or {}).get("purchase_cv")
        tm_sales_view = (this_view.get(cid) or {}).get("purchase_value")
        tm_sales_click = (this_click.get(cid) or {}).get("purchase_value")

        # CPA/ROAS は click7d を基準に固定（要件の「Click:7days」列と整合）
        lm_cpa_click = (float(lm_spend) / float(lm_cv_click)) if (lm_spend is not None and lm_cv_click and lm_cv_click > 0) else None
        lm_roas_click = (float(lm_sales_click) / float(lm_spend)) if (lm_spend and lm_spend > 0 and lm_sales_click is not None) else None

        tm_cpa_click = (float(tm_spend) / float(tm_cv_click)) if (tm_spend is not None and tm_cv_click and tm_cv_click > 0) else None
        tm_roas_click = (float(tm_sales_click) / float(tm_spend)) if (tm_spend and tm_spend > 0 and tm_sales_click is not None) else None

        # 修正: 配列出力に reach を追加
        table.append([
            cid,
            name,

            fmt(lm_cv_view),
            fmt(lm_cv_click),
            fmt(lm_sales_view),
            fmt(lm_sales_click),
            fmt(lm_spend),
            fmt(lm_reach),
            fmt(lm_cpa_click),
            fmt(lm_roas_click),

            fmt(tm_cv_view),
            fmt(tm_cv_click),
            fmt(tm_sales_view),
            fmt(tm_sales_click),
            fmt(tm_spend),
            fmt(tm_reach),
            fmt(tm_cpa_click),
            fmt(tm_roas_click),
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

    # purchase action types（secretsで上書き可能にしておく）
    purchase_action_types = cfg.get("purchase_action_types", DEFAULT_PURCHASE_ACTION_TYPES)

    # 修正: campaign-level fields に "reach" を追加
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
            # --- 先月（フル月） ---
            last_rows_view = meta_get_insights(
                api_version=api_version,
                m_token=m_token,
                m_act_id=m_act_id,
                fields=fields,
                date_preset="last_month",
                action_attribution_windows=["1d_view"],
            )
            last_rows_click = meta_get_insights(
                api_version=api_version,
                m_token=m_token,
                m_act_id=m_act_id,
                fields=fields,
                date_preset="last_month",
                action_attribution_windows=["7d_click"],
            )

            last_view = rows_to_purchase_metrics(last_rows_view, purchase_action_types, "1d_view")
            last_click = rows_to_purchase_metrics(last_rows_click, purchase_action_types, "7d_click")

            # --- 今月（当月1日〜前日、JST計算） ---
            rng = this_month_range_to_yesterday_jst()
            if rng is None:
                this_view = {}
                this_click = {}
            else:
                since, until = rng
                this_rows_view = meta_get_insights(
                    api_version=api_version,
                    m_token=m_token,
                    m_act_id=m_act_id,
                    fields=fields,
                    time_range={"since": since, "until": until},
                    action_attribution_windows=["1d_view"],
                )
                this_rows_click = meta_get_insights(
                    api_version=api_version,
                    m_token=m_token,
                    m_act_id=m_act_id,
                    fields=fields,
                    time_range={"since": since, "until": until},
                    action_attribution_windows=["7d_click"],
                )

                this_view = rows_to_purchase_metrics(this_rows_view, purchase_action_types, "1d_view")
                this_click = rows_to_purchase_metrics(this_rows_click, purchase_action_types, "7d_click")

            table = build_monthly_table_purchase_attr(last_view, last_click, this_view, this_click)
            sheets_write(s_id, worksheet_title, table, g_creds)
            print(f"OK: wrote MONTHLY(purchase attr) to sheet '{worksheet_title}' rows={len(table)-1}")

        else:
            print(f"SKIP: sheet_kind '{kind}' is not implemented yet (worksheet='{worksheet_title}')")


if __name__ == "__main__":
    main()
