import os
import json
import time
import requests
import gspread
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# =========================================================
# Settings
# =========================================================

META_API_VERSION = "v25.0"
JST = ZoneInfo("Asia/Tokyo")

SHEET_GITREPORT1 = "gitreport1"
SHEET_GITREPORT2 = "gitreport2"
SHEET_GITREPORT3 = "gitreport3"

GITREPORT1_MONTHS = 2
GITREPORT2_MONTHS = 13
GITREPORT3_MONTHS = 2

MAX_RETRIES = 5
REQUEST_TIMEOUT = 120

# gitreport1:
# APIリクエストでは4windowを指定。
# 1d_clickは出力には使わない。
ATTRIBUTION_WINDOWS = [
    "1d_click",
    "7d_click",
    "1d_view",
    "incrementality",
]

# 同一Purchaseを複数action_typeで二重計上しないため、
# 優先順に最初に存在するものだけ採用。
PURCHASE_ACTION_TYPES = [
    "offsite_conversion.fb_pixel_purchase",
    "omni_purchase",
    "purchase",
]

FILTER_CAMPAIGN_TOTAL_NAMES = {
    "Camper(CORE)": "Camper(CORE)_合計",
    "Camper(SP)": "Camper(SP)_合計",
    "Camper(CORE-L)": "Camper(CORE-L)_合計",
    "CV最適化": "CV最適化_合計",
}

AUDIENCE_SEGMENT_BREAKDOWN_CANDIDATES = [
    "user_segment_key",
    "audience_segment",
    "user_persona_name",
]


# =========================================================
# Main
# =========================================================

def main():
    print("=== Start Meta 3-sheet export ===")

    config = load_secret()
    mask_sensitive_values(config)

    resolved = resolve_config(config)
    validate_config(resolved)

    act_id = normalize_meta_act_id(resolved["meta"]["account_id"])
    token = resolved["meta"]["token"]

    ranges_2m = get_month_ranges(GITREPORT1_MONTHS)
    ranges_13m = get_month_ranges(GITREPORT2_MONTHS)

    print("gitreport1 ranges:", format_ranges(ranges_2m))
    print("gitreport2 ranges:", format_ranges(ranges_13m))
    print("gitreport3 ranges:", format_ranges(ranges_2m))

    spreadsheets = connect_spreadsheets(
        sheet_ids=resolved["sheet"]["spreadsheet_ids"],
        google_creds_dict=resolved["sheet"]["google_service_account"],
    )

    failures = []

    # -----------------------------------------------------
    # gitreport1
    # -----------------------------------------------------
    try:
        print("\n=== Start gitreport1 ===")
        rows1 = build_gitreport1_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_2m,
        )
        print(f"gitreport1 rows built: {len(rows1)}")

        for spreadsheet in spreadsheets:
            write_gitreport1(spreadsheet, rows1)

        print("=== Completed gitreport1 ===")

    except Exception as e:
        failures.append(("gitreport1", e))
        print(f"=== FAILED gitreport1: {repr(e)} ===")

    # -----------------------------------------------------
    # gitreport2
    # -----------------------------------------------------
    try:
        print("\n=== Start gitreport2 ===")
        rows2 = build_gitreport2_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_13m,
        )
        print(f"gitreport2 rows built: {len(rows2)}")

        for spreadsheet in spreadsheets:
            write_gitreport2(spreadsheet, rows2)

        print("=== Completed gitreport2 ===")

    except Exception as e:
        failures.append(("gitreport2", e))
        print(f"=== FAILED gitreport2: {repr(e)} ===")

    # -----------------------------------------------------
    # gitreport3
    # -----------------------------------------------------
    try:
        print("\n=== Start gitreport3 ===")
        rows3 = build_gitreport3_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_2m,
        )
        print(f"gitreport3 rows built: {len(rows3)}")

        for spreadsheet in spreadsheets:
            write_gitreport3(spreadsheet, rows3)

        print("=== Completed gitreport3 ===")

    except Exception as e:
        failures.append(("gitreport3", e))
        print(f"=== FAILED gitreport3: {repr(e)} ===")

    # 他シートを最後まで実行したうえで、
    # 失敗があればActions自体はfailedにする。
    if failures:
        summary = " | ".join(
            f"{name}: {repr(err)}"
            for name, err in failures
        )
        raise RuntimeError(
            f"One or more sheet exports failed: {summary}"
        )

    print("\n=== All exports completed ===")


# =========================================================
# Secret / Config
# =========================================================

def load_secret():
    raw = os.environ.get("APP_SECRET_JSON")
    if not raw:
        raise RuntimeError("APP_SECRET_JSON is not set")

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"APP_SECRET_JSON is invalid JSON: {e}"
        ) from e


def mask_sensitive_values(config):
    meta = config.get("meta", {})

    for value in [
        meta.get("token"),
        meta.get("account_id"),
    ]:
        if value:
            value = str(value).strip()

            if value and "\n" not in value:
                print(f"::add-mask::{value}")


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
            "google_service_account":
                normalize_google_service_account(gcp),
        },
    }


def validate_config(resolved):
    required = {
        "meta.token": resolved["meta"]["token"],
        "meta.account_id": resolved["meta"]["account_id"],
        "sheets.spreadsheet_id":
            resolved["sheet"]["spreadsheet_ids"],
        "gcp_service_account":
            resolved["sheet"]["google_service_account"],
    }

    missing = [
        k
        for k, v in required.items()
        if not v
    ]

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
        fixed["private_key"] = private_key.replace(
            "\\n",
            "\n",
        )

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
# Date helpers
# =========================================================

def add_months(base_date, months):
    month = base_date.month - 1 + months
    year = base_date.year + month // 12
    month = month % 12 + 1

    return date(
        year,
        month,
        1,
    )


def get_month_ranges(month_count):
    """
    JST基準。

    例: 2026-08-13実行 / 2ヶ月
      2026-07-01 ～ 2026-07-31
      2026-08-01 ～ 2026-08-12

    1日実行時は当月に対象日がないため、
    前月を最新月として指定月数分取得。
    """
    today_jst = datetime.now(JST).date()
    yesterday = today_jst - timedelta(days=1)

    this_month_start = date(
        today_jst.year,
        today_jst.month,
        1,
    )

    if yesterday < this_month_start:
        end_month_start = add_months(
            this_month_start,
            -1,
        )
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
        natural_month_end = (
            next_month - timedelta(days=1)
        )
        until = min(
            natural_month_end,
            yesterday,
        )

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
# Meta generic requests
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
                json.dumps(
                    payload,
                    ensure_ascii=False,
                )
                if payload is not None
                else response.text
            )

            last_error = RuntimeError(
                "Meta API failed. "
                f"status={response.status_code}, "
                f"body={body}"
            )

            retryable = (
                response.status_code >= 500
            )

            if (
                isinstance(payload, dict)
                and payload.get("error")
            ):
                code = payload["error"].get("code")

                if code in (
                    1,
                    2,
                    4,
                    17,
                    32,
                    613,
                ):
                    retryable = True

            if (
                not retryable
                or attempt == MAX_RETRIES
            ):
                raise last_error

        except requests.RequestException as e:
            last_error = e

            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Meta request error: {repr(e)}"
                ) from e

        wait_sec = 2 ** (attempt - 1)

        print(
            f"Meta retry "
            f"{attempt}/{MAX_RETRIES}, "
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
    filtering=None,
):
    url = (
        f"https://graph.facebook.com/"
        f"{META_API_VERSION}/"
        f"{act_id}/insights"
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
        params["time_increment"] = str(
            time_increment
        )

    if breakdowns:
        params["breakdowns"] = ",".join(
            breakdowns
        )

    if action_attribution_windows:
        params[
            "action_attribution_windows"
        ] = json.dumps(
            action_attribution_windows
        )

    if filtering:
        params["filtering"] = json.dumps(
            filtering,
            ensure_ascii=False,
        )

    context = {
        "level": level,
        "since": since.strftime("%Y-%m-%d"),
        "until": until.strftime("%Y-%m-%d"),
        "time_increment": time_increment,
        "breakdowns": breakdowns or [],
        "action_attribution_windows":
            action_attribution_windows or [],
        "filtering": filtering or [],
        "fields": fields,
    }

    print(
        "[META REQUEST]",
        json.dumps(
            context,
            ensure_ascii=False,
        ),
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

        rows.extend(
            payload.get("data", [])
        )

        next_url = (
            payload
            .get("paging", {})
            .get("next")
        )

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
    """
    scope:
      campaign_day
        月 × 日 × キャンペーン
        → API安定性を優先して1日ずつ取得

      ad
        月 × 広告

      adset_gen_age
        月 × 広告セット × 性別 × 年齢

      adset_pm
        月 × 広告セット ×
        platform × placement × device_platform
    """
    rows = []

    for month_range in month_ranges:

        # -------------------------------------------------
        # campaign_day
        # 1日ずつ取得
        # -------------------------------------------------
        for target_day in iter_dates(
            month_range["since"],
            month_range["until"],
        ):
            print(
                f"[gitreport1] "
                f"Fetching campaign_day: "
                f"{target_day:%Y-%m-%d}"
            )

            batch = fetch_meta_insights(
                act_id=act_id,
                token=token,
                since=target_day,
                until=target_day,
                level="campaign",
                fields=[
                    "campaign_name",
                    "actions",
                    "action_values",
                ],
                action_attribution_windows=
                    ATTRIBUTION_WINDOWS,
            )

            for item in batch:
                rows.append(
                    make_gitreport1_row(
                        scope="campaign_day",
                        month=month_range["label"],
                        day=target_day.strftime(
                            "%Y-%m-%d"
                        ),
                        item=item,
                    )
                )

        # -------------------------------------------------
        # ad
        # 月 × 広告
        # -------------------------------------------------
        print(
            f"[gitreport1] "
            f"Fetching ad: "
            f"{month_range['label']}"
        )

        batch = fetch_meta_insights(
            act_id=act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="ad",
            fields=[
                "campaign_name",
                "adset_name",
                "ad_name",
                "actions",
                "action_values",
            ],
            action_attribution_windows=
                ATTRIBUTION_WINDOWS,
        )

        for item in batch:
            rows.append(
                make_gitreport1_row(
                    scope="ad",
                    month=month_range["label"],
                    day="",
                    item=item,
                )
            )

        # -------------------------------------------------
        # adset_gen_age
        # -------------------------------------------------
        print(
            f"[gitreport1] "
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
            breakdowns=[
                "gender",
                "age",
            ],
            action_attribution_windows=
                ATTRIBUTION_WINDOWS,
        )

        for item in batch:
            rows.append(
                make_gitreport1_row(
                    scope="adset_gen_age",
                    month=month_range["label"],
                    day="",
                    item=item,
                    gender=item.get(
                        "gender",
                        "",
                    ),
                    age=item.get(
                        "age",
                        "",
                    ),
                )
            )

        # -------------------------------------------------
        # adset_pm
        # -------------------------------------------------
        print(
            f"[gitreport1] "
            f"Fetching adset_pm: "
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
            breakdowns=[
                "publisher_platform",
                "platform_position",
                "device_platform",
            ],
            action_attribution_windows=
                ATTRIBUTION_WINDOWS,
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

    return sort_gitreport1(rows)


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
    actions = item.get(
        "actions",
        [],
    )
    action_values = item.get(
        "action_values",
        [],
    )

    cv_view_1d = extract_purchase_window(
        actions,
        "1d_view",
    )

    cv_click_7d = extract_purchase_window(
        actions,
        "7d_click",
    )

    cv_incr = extract_purchase_window(
        actions,
        "incrementality",
    )

    sale_view_1d = extract_purchase_window(
        action_values,
        "1d_view",
    )

    sales_click_7d = extract_purchase_window(
        action_values,
        "7d_click",
    )

    sales_incr = extract_purchase_window(
        action_values,
        "incrementality",
    )

    return [
        "meta",                           # A media
        scope,                            # B scope
        month,                            # C month
        day,                              # D day
        item.get("campaign_name", ""),    # E campaign_name
        item.get("adset_name", ""),       # F adset_name
        item.get("ad_name", ""),          # G ad_name
        gender,                           # H gender
        age,                              # I age
        platform,                         # J platform
        placement,                        # K placement
        device_platform,                  # L device_platform
        cv_view_1d,                       # M cv_view_1d
        cv_click_7d,                      # N cv_click_7d
        cv_view_1d + cv_click_7d,         # O cv_view1d_click7d
        cv_incr,                          # P cv_incr
        sale_view_1d,                     # Q sale_view_1d
        sales_click_7d,                   # R sales_click_7d
        sale_view_1d + sales_click_7d,    # S sales_view1d_click7d
        sales_incr,                       # T sales_incr
    ]


def extract_purchase_window(
    action_list,
    window_key,
):
    if not isinstance(
        action_list,
        list,
    ):
        return 0.0

    action_map = {}

    for action in action_list:
        if not isinstance(
            action,
            dict,
        ):
            continue

        action_type = action.get(
            "action_type"
        )

        if action_type:
            action_map[
                action_type
            ] = action

    for action_type in PURCHASE_ACTION_TYPES:
        action = action_map.get(
            action_type
        )

        if action is not None:
            return to_float(
                action.get(window_key)
            )

    return 0.0


def sort_gitreport1(rows):
    scope_order = {
        "campaign_day": 0,
        "ad": 1,
        "adset_gen_age": 2,
        "adset_pm": 3,
    }

    return sorted(
        rows,
        key=lambda r: (
            r[2],
            scope_order.get(
                r[1],
                99,
            ),
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
# gitreport2
# =========================================================

def build_gitreport2_rows(
    act_id,
    token,
    month_ranges,
):
    """
    A month
    B campaign_name
    C reach

    通常キャンペーン別reachに加え、
    FILTER_CAMPAIGN_TOTAL_NAMESの4グループを
    account level + campaign.name CONTAINで直接取得。

    キャンペーン別reachを足し算しないため、
    グループ内のユニークリーチ重複を避けられる。
    """
    rows = []

    for month_range in month_ranges:
        print(
            f"[gitreport2] "
            f"Fetching campaign reach: "
            f"{month_range['label']}"
        )

        # ---------------------------------------------
        # campaign別
        # ---------------------------------------------
        campaign_rows = fetch_meta_insights(
            act_id=act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="campaign",
            fields=[
                "campaign_name",
                "reach",
            ],
        )

        for item in campaign_rows:
            rows.append([
                month_range["label"],
                item.get(
                    "campaign_name",
                    "",
                ),
                to_int(
                    item.get("reach")
                ),
            ])

        # ---------------------------------------------
        # campaign group別 deduplicated reach
        # ---------------------------------------------
        for keyword, total_name in (
            FILTER_CAMPAIGN_TOTAL_NAMES.items()
        ):
            print(
                f"[gitreport2] "
                f"Fetching total: "
                f"{month_range['label']} / "
                f"{keyword}"
            )

            total_rows = fetch_meta_insights(
                act_id=act_id,
                token=token,
                since=month_range["since"],
                until=month_range["until"],
                level="account",
                fields=[
                    "reach",
                ],
                filtering=[
                    {
                        "field": "campaign.name",
                        "operator": "CONTAIN",
                        "value": keyword,
                    }
                ],
            )

            # 1ヶ月 time_range + account levelなので
            # 通常は1行。安全のため複数行なら加算。
            total_reach = sum(
                to_int(
                    item.get("reach")
                )
                for item in total_rows
            )

            rows.append([
                month_range["label"],
                total_name,
                total_reach,
            ])

    return sorted(
        rows,
        key=lambda r: (
            r[0],
            r[1],
        ),
    )


# =========================================================
# gitreport3
# =========================================================

def build_gitreport3_rows(
    act_id,
    token,
    month_ranges,
):
    """
    Audience Segmentは既存auseと同じ取得方式。

    breakdown候補:
      user_segment_key
      audience_segment
      user_persona_name

    実際に値が返る最初のbreakdownを採用。

    想定値:
      existing
      prospecting
      unknown

    A month
    B campaign_name
    C audience_segment
    D impressions
    E spend × 1.2
    F cv

    cv:
      offsite_conversion.fb_pixel_purchase の value

    attribution:
      ["default"]
    """
    rows = []

    for month_range in month_ranges:
        print(
            f"[gitreport3] "
            f"Fetching audience segment: "
            f"{month_range['label']}"
        )

        chosen_breakdown = None
        segment_rows = []

        fields = [
            "campaign_id",
            "campaign_name",
            "impressions",
            "spend",
            "actions",
        ]

        for breakdown in (
            AUDIENCE_SEGMENT_BREAKDOWN_CANDIDATES
        ):
            try:
                rows_try = fetch_meta_insights(
                    act_id=act_id,
                    token=token,
                    since=month_range["since"],
                    until=month_range["until"],
                    level="campaign",
                    fields=fields,
                    breakdowns=[
                        breakdown
                    ],
                    action_attribution_windows=[
                        "default"
                    ],
                )

            except RuntimeError as e:
                print(
                    f"[gitreport3] "
                    f"breakdown='{breakdown}' "
                    f"API error: {e}"
                )
                continue

            if has_real_breakdown(
                rows_try,
                breakdown,
            ):
                chosen_breakdown = breakdown
                segment_rows = rows_try
                break

            print(
                f"[gitreport3] "
                f"breakdown='{breakdown}' "
                f"returned no usable values"
            )

        if not chosen_breakdown:
            raise RuntimeError(
                "gitreport3 failed: none of the "
                "Audience Segment breakdown candidates "
                "returned usable data. "
                f"month={month_range['label']}; "
                f"candidates="
                f"{AUDIENCE_SEGMENT_BREAKDOWN_CANDIDATES}"
            )

        print(
            f"[gitreport3] "
            f"{month_range['label']} "
            f"using breakdown="
            f"'{chosen_breakdown}', "
            f"rows={len(segment_rows)}"
        )

        for item in segment_rows:
            cv = extract_action_value(
                item.get(
                    "actions",
                    [],
                ),
                target_action=
                    "offsite_conversion.fb_pixel_purchase",
            )

            rows.append([
                month_range["label"],
                item.get(
                    "campaign_name",
                    "",
                ),
                item.get(
                    chosen_breakdown,
                    "",
                ),
                to_int(
                    item.get(
                        "impressions"
                    )
                ),
                round(
                    to_float(
                        item.get("spend")
                    ) * 1.2,
                    2,
                ),
                cv,
            ])

    return sorted(
        rows,
        key=lambda r: (
            r[0],
            r[1],
            r[2],
        ),
    )


def has_real_breakdown(
    rows,
    breakdown_key,
):
    if not rows:
        return False

    for row in rows:
        if (
            breakdown_key in row
            and row.get(
                breakdown_key
            ) not in (
                None,
                "",
            )
        ):
            return True

    return False


def extract_action_value(
    action_list,
    target_action,
):
    """
    action_attribution_windows=["default"]時の
    actions内 value を取得。
    """
    if not isinstance(
        action_list,
        list,
    ):
        return 0.0

    for action in action_list:
        if not isinstance(
            action,
            dict,
        ):
            continue

        if (
            action.get("action_type")
            == target_action
        ):
            return to_float(
                action.get("value")
            )

    return 0.0


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

    client = gspread.authorize(
        creds
    )

    spreadsheets = []

    for sheet_id in sheet_ids:
        try:
            spreadsheet = client.open_by_key(
                sheet_id
            )

            spreadsheets.append(
                spreadsheet
            )

            print(
                f"Google Sheets connected: "
                f"{sheet_id}"
            )

        except Exception as e:
            raise RuntimeError(
                "Google Sheets connection error: "
                f"sheet_id={sheet_id}, "
                f"{repr(e)}"
            ) from e

    return spreadsheets


def get_or_create_worksheet(
    spreadsheet,
    sheet_name,
    cols,
):
    try:
        return spreadsheet.worksheet(
            sheet_name
        )

    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=sheet_name,
            rows=1000,
            cols=cols,
        )


def write_sheet(
    spreadsheet,
    sheet_name,
    header,
    rows,
):
    worksheet = get_or_create_worksheet(
        spreadsheet,
        sheet_name,
        cols=len(header),
    )

    worksheet.clear()

    output = [
        header
    ] + rows

    worksheet.update(
        "A1",
        output,
    )

    print(
        f"Write success: "
        f"spreadsheet={spreadsheet.id}, "
        f"sheet={sheet_name}, "
        f"rows={len(rows)}"
    )


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

    write_sheet(
        spreadsheet,
        SHEET_GITREPORT1,
        header,
        rows,
    )


def write_gitreport2(
    spreadsheet,
    rows,
):
    header = [
        "month",
        "campaign_name",
        "reach",
    ]

    write_sheet(
        spreadsheet,
        SHEET_GITREPORT2,
        header,
        rows,
    )


def write_gitreport3(
    spreadsheet,
    rows,
):
    header = [
        "month",
        "campaign_name",
        "audience_segment",
        "impressions",
        "spend",
        "cv",
    ]

    write_sheet(
        spreadsheet,
        SHEET_GITREPORT3,
        header,
        rows,
    )


# =========================================================
# Utils
# =========================================================

def to_float(value):
    try:
        return float(value)
    except (
        TypeError,
        ValueError,
    ):
        return 0.0


def to_int(value):
    try:
        return int(
            float(value)
        )
    except (
        TypeError,
        ValueError,
    ):
        return 0


def truncate_text(
    value,
    limit=1200,
):
    value = str(value)

    if len(value) <= limit:
        return value

    return (
        value[:limit]
        + "...(truncated)"
    )


if __name__ == "__main__":
    main()
