import os
import json
import time
import requests
import gspread
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from oauth2client.service_account import ServiceAccountCredentials

META_API_VERSION = "v25.0"
JST = ZoneInfo("Asia/Tokyo")

SHEET_NAME = "gitreport1"
MONTH_COUNT = 2

PURCHASE_ACTION_TYPES = [
    "offsite_conversion.fb_pixel_purchase",
    "omni_purchase",
    "purchase",
]

MAX_RETRIES = 5
REQUEST_TIMEOUT = 120


def main():
    print("=== Start gitreport1 export ===")

    config = load_secret()
    resolved = resolve_config(config)
    validate_config(resolved)

    act_id = normalize_meta_act_id(resolved["meta"]["account_id"])
    token = resolved["meta"]["token"]

    month_ranges = get_month_ranges(MONTH_COUNT)
    print("Target ranges:", format_ranges(month_ranges))

    rows = build_gitreport1_rows(
        act_id=act_id,
        token=token,
        month_ranges=month_ranges,
    )

    print(f"Built rows: {len(rows)}")

    spreadsheets = connect_spreadsheets(
        resolved["sheet"]["spreadsheet_ids"],
        resolved["sheet"]["google_service_account"],
    )

    for spreadsheet in spreadsheets:
        write_gitreport1(spreadsheet, rows)

    print("=== Completed ===")


# =========================================================
# Secret / config
# =========================================================

def load_secret():
    raw = os.environ.get("APP_SECRET_JSON")
    if not raw:
        raise RuntimeError("APP_SECRET_JSON is not set")

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"APP_SECRET_JSON is invalid JSON: {e}") from e


def resolve_config(config):
    meta = config.get("meta", {})
    sheets = config.get("sheets", {})
    gcp = config.get("gcp_service_account")

    spreadsheet_ids = sheets.get("spreadsheet_id")

    if isinstance(spreadsheet_ids, str):
        spreadsheet_ids = [spreadsheet_ids]
    elif not isinstance(spreadsheet_ids, list):
        spreadsheet_ids = []

    spreadsheet_ids = [
        str(x).strip()
        for x in spreadsheet_ids
        if str(x).strip()
    ]

    return {
        "meta": {
            "token": meta.get("token"),
            "account_id": meta.get("account_id"),
        },
        "sheet": {
            "spreadsheet_ids": spreadsheet_ids,
            "google_service_account": normalize_google_service_account(gcp),
        },
    }


def validate_config(resolved):
    required = {
        "meta.token": resolved["meta"]["token"],
        "meta.account_id": resolved["meta"]["account_id"],
        "sheets.spreadsheet_id": resolved["sheet"]["spreadsheet_ids"],
        "gcp_service_account": resolved["sheet"]["google_service_account"],
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing required config keys: {', '.join(missing)}"
        )


def normalize_google_service_account(creds):
    if not creds:
        return None

    fixed = dict(creds)
    private_key = fixed.get("private_key", "")

    if private_key:
        fixed["private_key"] = private_key.replace("\\n", "\n")

    return fixed


def normalize_meta_act_id(raw_act_id):
    cleaned = (
        str(raw_act_id)
        .replace("act=", "")
        .replace("act_", "")
        .replace("act", "")
        .strip()
    )
    return f"act_{cleaned}"


# =========================================================
# Date
# =========================================================

def add_months(base_date, months):
    month = base_date.month - 1 + months
    year = base_date.year + month // 12
    month = month % 12 + 1
    return date(year, month, 1)


def get_month_ranges(month_count):
    today_jst = datetime.now(JST).date()
    yesterday = today_jst - timedelta(days=1)

    this_month_start = date(
        today_jst.year,
        today_jst.month,
        1,
    )

    # 1日実行時は当月に対象日がないため前月まで
    if yesterday < this_month_start:
        end_month_start = add_months(this_month_start, -1)
    else:
        end_month_start = this_month_start

    start_month = add_months(
        end_month_start,
        -(month_count - 1),
    )

    ranges = []
    current = start_month

    while current <= end_month_start:
        next_month = add_months(current, 1)
        month_end = next_month - timedelta(days=1)
        until = min(month_end, yesterday)

        if current <= until:
            ranges.append({
                "label": current.strftime("%Y-%m"),
                "since": current,
                "until": until,
            })

        current = next_month

    return ranges


def iter_dates(since, until):
    current = since
    while current <= until:
        yield current
        current += timedelta(days=1)


def format_ranges(ranges):
    return ", ".join(
        f"{r['label']}({r['since']} to {r['until']})"
        for r in ranges
    )


# =========================================================
# Meta API
# =========================================================

def meta_get(url, params=None):
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            try:
                payload = response.json()
            except Exception:
                payload = None

            if (
                response.ok
                and not (
                    isinstance(payload, dict)
                    and payload.get("error")
                )
            ):
                return response

            body = truncate_text(
                json.dumps(payload, ensure_ascii=False)
                if payload is not None
                else response.text
            )

            last_error = RuntimeError(
                f"Meta API failed. "
                f"status={response.status_code}, body={body}"
            )

            retryable = response.status_code >= 500

            if isinstance(payload, dict) and payload.get("error"):
                code = payload["error"].get("code")
                if code in (1, 2, 4, 17, 32, 613):
                    retryable = True

            if not retryable or attempt == MAX_RETRIES:
                raise last_error

        except requests.RequestException as e:
            last_error = e

            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Meta request error: {repr(e)}"
                ) from e

        wait_sec = 2 ** (attempt - 1)
        print(
            f"Meta retry {attempt}/{MAX_RETRIES}, "
            f"sleep={wait_sec}s"
        )
        time.sleep(wait_sec)

    raise RuntimeError(
        f"Meta API request failed: {last_error}"
    )


def fetch_meta_insights(
    act_id,
    token,
    since,
    until,
    level,
    fields,
    time_increment=None,
    breakdowns=None,
    action_attribution_windows=None,
):
    url = (
        f"https://graph.facebook.com/"
        f"{META_API_VERSION}/{act_id}/insights"
    )

    params = {
        "access_token": token,
        "level": level,
        "time_range": json.dumps({
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }),
        "fields": ",".join(fields),
        "limit": 5000,
    }

    if time_increment is not None:
        params["time_increment"] = str(time_increment)

    if breakdowns:
        params["breakdowns"] = ",".join(breakdowns)

    if action_attribution_windows:
        params["action_attribution_windows"] = json.dumps(
            action_attribution_windows
        )

    context = {
        "level": level,
        "since": since.strftime("%Y-%m-%d"),
        "until": until.strftime("%Y-%m-%d"),
        "time_increment": time_increment,
        "breakdowns": breakdowns or [],
        "action_attribution_windows":
            action_attribution_windows or [],
        "fields": fields,
    }

    print(
        "[META REQUEST]",
        json.dumps(context, ensure_ascii=False),
    )

    rows = []

    while True:
        try:
            response = meta_get(
                url,
                params=params,
            )
        except RuntimeError as e:
            raise RuntimeError(
                "Meta Insights request failed. "
                f"context="
                f"{json.dumps(context, ensure_ascii=False)}; "
                f"error={e}"
            ) from e

        payload = response.json()
        rows.extend(payload.get("data", []))

        next_url = payload.get(
            "paging",
            {},
        ).get("next")

        if not next_url:
            break

        url = next_url
        params = None

    return rows


# =========================================================
# gitreport1
# =========================================================

def build_gitreport1_rows(
    act_id,
    token,
    month_ranges,
):
    rows = []

    # APIリクエストは4window指定。
    # 1d_clickは出力には使わない。
    attr_windows = [
        "1d_click",
        "7d_click",
        "1d_view",
        "incrementality",
    ]

    for month_range in month_ranges:

        # ---------------------------------------------
        # ad_day
        # 月 × 日 × 広告
        # ---------------------------------------------
        print(
            f"Fetching ad_day: {month_range['label']}"
        )

        # Meta error_subcode 1504044 is a Sync report failure.
        # Avoid one large "ad x day x month" synchronous query:
        # fetch each date independently.
        for target_day in iter_dates(
            month_range["since"],
            month_range["until"],
        ):
            print(
                f"Fetching ad_day: "
                f"{target_day.strftime('%Y-%m-%d')}"
            )

            batch = fetch_meta_insights(
                act_id=act_id,
                token=token,
                since=target_day,
                until=target_day,
                level="ad",
                fields=[
                    "campaign_name",
                    "adset_name",
                    "ad_name",
                    "actions",
                    "action_values",
                ],
                # 1日だけのtime_rangeなのでtime_incrementは不要
                time_increment=None,
                action_attribution_windows=attr_windows,
            )

            for item in batch:
                rows.append(
                    make_gitreport1_row(
                        scope="ad_day",
                        month=month_range["label"],
                        day=target_day.strftime("%Y-%m-%d"),
                        item=item,
                    )
                )

        # ---------------------------------------------
        # adset_gen_age
        # 月 × 広告セット × 性別 × 年齢
        # ---------------------------------------------
        print(
            f"Fetching adset_gen_age: "
            f"{month_range['label']}"
        )

        batch = fetch_meta_insights(
            act_id=act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="adset",
            fields=[
                "campaign_name",
                "adset_name",
                "actions",
                "action_values",
            ],
            time_increment=None,
            breakdowns=[
                "gender",
                "age",
            ],
            action_attribution_windows=attr_windows,
        )

        for item in batch:
            rows.append(
                make_gitreport1_row(
                    scope="adset_gen_age",
                    month=month_range["label"],
                    day="",
                    item=item,
                    gender=item.get("gender", ""),
                    age=item.get("age", ""),
                )
            )

        # ---------------------------------------------
        # adset_pm
        # 月 × 広告セット × 配置
        # ---------------------------------------------
        print(
            f"Fetching adset_pm: {month_range['label']}"
        )

        batch = fetch_meta_insights(
            act_id=act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="adset",
            fields=[
                "campaign_name",
                "adset_name",
                "actions",
                "action_values",
            ],
            time_increment=None,
            breakdowns=[
                "publisher_platform",
                "platform_position",
                "device_platform",
            ],
            action_attribution_windows=attr_windows,
        )

        for item in batch:
            rows.append(
                make_gitreport1_row(
                    scope="adset_pm",
                    month=month_range["label"],
                    day="",
                    item=item,
                    platform=item.get(
                        "publisher_platform",
                        "",
                    ),
                    placement=item.get(
                        "platform_position",
                        "",
                    ),
                    device_platform=item.get(
                        "device_platform",
                        "",
                    ),
                )
            )

    return sort_rows(rows)


def make_gitreport1_row(
    scope,
    month,
    day,
    item,
    gender="",
    age="",
    platform="",
    placement="",
    device_platform="",
):
    actions = item.get("actions", [])
    action_values = item.get("action_values", [])

    cv_view_1d = extract_purchase_metric(
        actions,
        "1d_view",
    )

    cv_click_7d = extract_purchase_metric(
        actions,
        "7d_click",
    )

    cv_incr = extract_purchase_metric(
        actions,
        "incrementality",
    )

    sale_view_1d = extract_purchase_metric(
        action_values,
        "1d_view",
    )

    sales_click_7d = extract_purchase_metric(
        action_values,
        "7d_click",
    )

    sales_incr = extract_purchase_metric(
        action_values,
        "incrementality",
    )

    return [
        "meta",
        scope,
        month,
        day,
        item.get("campaign_name", ""),
        item.get("adset_name", ""),
        item.get("ad_name", ""),
        gender,
        age,
        platform,
        placement,
        device_platform,
        cv_view_1d,
        cv_click_7d,
        cv_view_1d + cv_click_7d,
        cv_incr,
        sale_view_1d,
        sales_click_7d,
        sale_view_1d + sales_click_7d,
        sales_incr,
    ]


def extract_purchase_metric(
    action_list,
    attr_window,
):
    if not isinstance(action_list, list):
        return 0.0

    action_map = {}

    for action in action_list:
        if not isinstance(action, dict):
            continue

        action_type = action.get("action_type")

        if action_type:
            action_map[action_type] = action

    # 同一Purchaseを複数action_typeで二重計上しない
    for action_type in PURCHASE_ACTION_TYPES:
        action = action_map.get(action_type)

        if action is not None:
            return to_float(
                action.get(attr_window)
            )

    return 0.0


def sort_rows(rows):
    scope_order = {
        "ad_day": 0,
        "adset_gen_age": 1,
        "adset_pm": 2,
    }

    return sorted(
        rows,
        key=lambda r: (
            r[2],
            scope_order.get(r[1], 99),
            r[3],
            r[4],
            r[5],
            r[6],
            r[7],
            r[8],
            r[9],
            r[10],
            r[11],
        ),
    )


# =========================================================
# Google Sheets
# =========================================================

def connect_spreadsheets(
    sheet_ids,
    google_creds_dict,
):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = (
        ServiceAccountCredentials
        .from_json_keyfile_dict(
            google_creds_dict,
            scope,
        )
    )

    client = gspread.authorize(creds)

    spreadsheets = []

    for sheet_id in sheet_ids:
        spreadsheet = client.open_by_key(sheet_id)
        spreadsheets.append(spreadsheet)

        print(
            f"Google Sheets connected: {sheet_id}"
        )

    return spreadsheets


def write_gitreport1(
    spreadsheet,
    rows,
):
    header = [
        "media",
        "scope",
        "month",
        "day",
        "campaign_name",
        "adset_name",
        "ad_name",
        "gender",
        "age",
        "platform",
        "placement",
        "device_platform",
        "cv_view_1d",
        "cv_click_7d",
        "cv_view1d_click7d",
        "cv_incr",
        "sale_view_1d",
        "sales_click_7d",
        "sales_view1d_click7d",
        "sales_incr",
    ]

    try:
        worksheet = spreadsheet.worksheet(
            SHEET_NAME
        )
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=SHEET_NAME,
            rows=1000,
            cols=len(header),
        )

    worksheet.clear()

    output = [header] + rows

    worksheet.update(
        "A1",
        output,
    )

    print(
        f"Write success: {SHEET_NAME}, "
        f"rows={len(rows)}"
    )


# =========================================================
# Utils
# =========================================================

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def truncate_text(
    value,
    limit=1200,
):
    value = str(value)

    if len(value) <= limit:
        return value

    return value[:limit] + "...(truncated)"


if __name__ == "__main__":
    main()
