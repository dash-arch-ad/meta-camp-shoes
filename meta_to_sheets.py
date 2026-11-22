import os
import json
import time
import logging
import math
import re
import sys
import uuid
from contextlib import ExitStack, contextmanager
from urllib.parse import parse_qs, urlsplit
import requests
import gspread
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from google.auth.exceptions import RefreshError
from gspread.exceptions import APIError, SpreadsheetNotFound
from oauth2client.client import AccessTokenRefreshError
from oauth2client.service_account import ServiceAccountCredentials

# =========================================================
# Settings
# =========================================================

META_API_VERSION = "v27.0"
JST = ZoneInfo("Asia/Tokyo")

SHEET_MON_DAY = "mon_day"
SHEET_AD = "ad"
SHEET_AUDIENCE = "audience"
SHEET_AUDIENCE_DETAIL = "audience_detail"
SHEET_SEGMENTS = "segments"

GITREPORT1_MONTHS = 14
GITREPORT2_MONTHS = 3
GITREPORT3_MONTHS = 3
GITREPORT4_MONTHS = 3

MAX_RETRIES = 5
REQUEST_TIMEOUT = 120

# Meta公式のIncrementality取得用window。
# 1d_clickは出力には使わないが、APIリクエストには含める。
ATTRIBUTION_WINDOWS = [
    "1d_click",
    "7d_click",
    "1d_view",
    "incrementality",
]

FILTER_CAMPAIGN_TOTAL_NAMES = {
    "Camper(CORE)": "Camper(CORE)_合計",
    "Camper(SP)": "Camper(SP)_合計",
    "Camper(CORE-L)": "Camper(CORE-L)_合計",
    "ASC_CV最適化": "CV最適化_合計",
}

AUDIENCE_SEGMENT_BREAKDOWN_CANDIDATES = [
    "user_segment_key",
    "audience_segment",
    "user_persona_name",
]

# 同じ購入が複数action_typeで返るケースがあるため、
# 合算せず、上から最初に存在するものだけ採用する。
PURCHASE_COUNT_CANDIDATES = [
    "offsite_conversion.fb_pixel_purchase",
]

PURCHASE_VALUE_CANDIDATES = [
    "offsite_conversion.fb_pixel_purchase",
]

ADD_TO_CART_CANDIDATES = [
    "omni_add_to_cart",
    "offsite_conversion.fb_pixel_add_to_cart",
    "add_to_cart",
]

META_LEAD_CANDIDATES = [
    "onsite_conversion.lead_grouped",
]

POST_REACTION_CANDIDATES = [
    "post_reaction",
]

POST_COMMENT_CANDIDATES = [
    "comment",
]

POST_SAVE_CANDIDATES = [
    "onsite_conversion.post_save",
    "post_save",
]

POST_SHARE_CANDIDATES = [
    "post_share",
    "post",
]


# =========================================================
# Main
# =========================================================

def main():
    with report_step("Configuration"):
        resolved = resolve_config(load_secret())
        validate_config(resolved)
        act_id = normalize_meta_act_id(resolved["meta"]["account_id"])
        token = resolved["meta"]["token"]
        targets = list(dict.fromkeys(resolved["sheet"]["spreadsheet_ids"]))

    print(f"Opening Google Sheets destinations: {len(targets)}.", flush=True)
    books = connect_spreadsheets(targets, resolved["sheet"]["google_service_account"])
    with ExitStack() as locks:
        owners = {}
        # Acquire shared locks in a consistent order, regardless of the configured output order.
        for index in sorted(range(len(books)), key=targets.__getitem__):
            with report_step(f"Google Sheets lock, destination {index + 1}"):
                owners[index] = locks.enter_context(export_lock(books[index]))
        print(f"Reading Meta Ads Insights ({META_API_VERSION}).", flush=True)
        months = get_month_ranges(GITREPORT1_MONTHS)
        recent = get_month_ranges(GITREPORT2_MONTHS)
        reports = [
            (SHEET_MON_DAY, MON_DAY_HEADER, build_mon_day_rows(act_id, token, months, recent)),
            (SHEET_AD, AD_HEADER, build_ad_rows(act_id, token, recent)),
            (SHEET_AUDIENCE, AUDIENCE_HEADER, build_audience_rows(act_id, token, recent)),
            (SHEET_AUDIENCE_DETAIL, AUDIENCE_DETAIL_HEADER, build_audience_detail_rows(act_id, token, recent)),
            (SHEET_SEGMENTS, SEGMENTS_HEADER, build_segments_rows(act_id, token, recent)),
        ]
        plans = []
        for i, book in enumerate(books):
            with report_step(f"Prepare Sheets update, destination {i + 1}"):
                plans.append(publication_requests(book, owners[i], reports))
        for i, (book, plan) in enumerate(zip(books, plans)):
            try:
                publish(book, owners[i], plan)
            except ExportError as error:
                print(f"Destination {i + 1}: {error}", file=sys.stderr)
                print(f"Remaining destinations not attempted: {len(books) - i - 1}", file=sys.stderr)
                raise
            print(f"Destination {i + 1}: published all five tabs.")
    print("All exports completed.")


def load_secret():
    raw = os.environ.get(
        "APP_SECRET_JSON"
    )

    if not raw:
        raise ExportError("APP_SECRET_JSON is not set.")
    try:
        config = json.loads(raw)
    except json.JSONDecodeError:
        raise ExportError("APP_SECRET_JSON is invalid JSON.") from None
    if not isinstance(config, dict):
        raise ExportError("APP_SECRET_JSON must contain a JSON object.")
    return config


def resolve_config(config):
    meta = config.get(
        "meta",
        {},
    )
    sheets = config.get(
        "sheets",
        {},
    )
    gcp = config.get(
        "gcp_service_account"
    )

    spreadsheet_ids = sheets.get(
        "spreadsheet_id"
    )

    if isinstance(
        spreadsheet_ids,
        str,
    ):
        spreadsheet_ids = [
            spreadsheet_ids
        ]

    elif not isinstance(
        spreadsheet_ids,
        list,
    ):
        spreadsheet_ids = []

    spreadsheet_ids = [
        str(x).strip()
        for x in spreadsheet_ids
        if str(x).strip()
    ]

    return {
        "meta": {
            "token": meta.get(
                "token"
            ),
            "account_id": meta.get(
                "account_id"
            ),
        },
        "sheet": {
            "spreadsheet_ids":
                spreadsheet_ids,
            "google_service_account":
                normalize_google_service_account(
                    gcp
                ),
        },
    }


def validate_config(resolved):
    required = {
        "meta.token":
            resolved["meta"]["token"],
        "meta.account_id":
            resolved["meta"]["account_id"],
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
        raise ExportError("Missing required config keys: " + ", ".join(missing))
    if not isinstance(resolved["meta"]["token"], str) or not resolved["meta"]["token"].strip():
        raise ExportError("meta.token must be a nonempty string.")


def normalize_google_service_account(
    creds,
):
    if not creds:
        return None

    fixed = dict(
        creds
    )

    private_key = fixed.get(
        "private_key",
        "",
    )

    if private_key:
        fixed[
            "private_key"
        ] = private_key.replace(
            "\\n",
            "\n",
        )

    return fixed


def normalize_meta_act_id(
    raw_act_id,
):
    cleaned = (
        str(raw_act_id)
        .replace("act=", "")
        .replace("act_", "")
        .replace("act", "")
        .strip()
    )

    if not re.fullmatch(r"[0-9]+", cleaned):
        raise ExportError("Invalid Meta account ID.")
    return f"act_{cleaned}"


# =========================================================
# Date helpers
# =========================================================

def add_months(
    base_date,
    months,
):
    month = (
        base_date.month
        - 1
        + months
    )

    year = (
        base_date.year
        + month // 12
    )

    month = (
        month % 12
        + 1
    )

    return date(
        year,
        month,
        1,
    )


def get_month_ranges(
    month_count,
):
    """
    JST基準。

    例: 2026-08-14実行 / 2ヶ月
      2026-07-01 ～ 2026-07-31
      2026-08-01 ～ 2026-08-13

    当月は前日まで。
    1日実行時は前月を最新月とする。
    """
    today_jst = datetime.now(
        JST
    ).date()

    yesterday = (
        today_jst
        - timedelta(days=1)
    )

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
        end_month_start = (
            this_month_start
        )

    start_month = add_months(
        end_month_start,
        -(month_count - 1),
    )

    ranges = []
    current = start_month

    while current <= end_month_start:
        next_month = add_months(
            current,
            1,
        )

        natural_month_end = (
            next_month
            - timedelta(days=1)
        )

        until = min(
            natural_month_end,
            yesterday,
        )

        if current <= until:
            ranges.append({
                "label":
                    current.strftime(
                        "%Y-%m"
                    ),
                "since":
                    current,
                "until":
                    until,
            })

        current = next_month

    return ranges


def iter_dates(
    since,
    until,
):
    current = since

    while current <= until:
        yield current
        current += timedelta(
            days=1
        )


def to_sheets_date_serial(
    value,
):
    """
    Google Sheetsへ日付型として扱える数値を出力。
    表示形式はスプレッドシート側で設定する。
    """
    if not value:
        return ""

    if isinstance(
        value,
        date,
    ):
        d = value
    else:
        d = datetime.strptime(
            str(value),
            "%Y-%m-%d",
        ).date()

    base = date(
        1899,
        12,
        30,
    )

    return (
        d - base
    ).days


def format_ranges(
    ranges,
):
    return ", ".join(
        f"{r['label']}"
        f"({r['since']} to {r['until']})"
        for r in ranges
    )


# =========================================================
# Meta API
# =========================================================

class ExportError(Exception):
    """Only fixed labels and validated numeric diagnostics may be displayed."""


class UnsupportedBreakdown(ExportError):
    pass


def sheets_error_detail(error):
    if isinstance(error, (RefreshError, AccessTokenRefreshError)):
        return "Google service account authentication failed"
    if isinstance(error, SpreadsheetNotFound):
        return "spreadsheet not found or inaccessible"
    if isinstance(error, PermissionError):
        return "spreadsheet access denied (HTTP 403)"
    if isinstance(error, APIError):
        status = getattr(error.response, "status_code", None)
        if type(status) is int and 100 <= status <= 599:
            return f"Google Sheets API rejected the request (HTTP {status})"
    if isinstance(error, requests.Timeout):
        return "request timed out"
    if isinstance(error, requests.ConnectionError):
        return "connection failed"
    return "operation failed"


@contextmanager
def report_step(label):
    try:
        yield
    except ExportError as error:
        raise ExportError(f"{label}: {error}") from None
    except Exception as error:
        raise ExportError(f"{label}: {sheets_error_detail(error)}.") from None


def meta_get(url, params, token, allow_fallback):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, headers={"Authorization": f"Bearer {token}"},
                                    timeout=REQUEST_TIMEOUT, allow_redirects=False)
        except requests.RequestException:
            problem = "Meta API connection failed."
            retryable = True
        else:
            status = response.status_code
            try:
                payload = response.json()
            except ValueError:
                payload = None
            if 200 <= status < 300 and isinstance(payload, dict) and "error" not in payload:
                return payload
            error = payload.get("error") if isinstance(payload, dict) else None
            error = error if isinstance(error, dict) else {}
            code = error.get("code") if type(error.get("code")) is int else None
            details = [f"HTTP {status}"]
            for key in ("code", "error_subcode"):
                if type(error.get(key)) is int:
                    details.append(f"{key}={error[key]}")
            problem = "Meta API failed (" + ", ".join(details) + ")."
            if allow_fallback and status == 400 and code == 100:
                # Match only parameter diagnostics, never emit the remote message.
                message = error.get("message")
                if isinstance(message, str) and "breakdown" in message.lower():
                    raise UnsupportedBreakdown(problem)
            retryable = (status >= 500 or status == 429 or code in (1, 2, 4, 17, 32, 613))
            if 300 <= status < 400:
                retryable = False
        if not retryable or attempt == MAX_RETRIES:
            raise ExportError(problem) from None
        print(f"Meta retry {attempt}/{MAX_RETRIES}.")
        time.sleep(2 ** (attempt - 1))


def require_number(value):
    if type(value) not in (str, float, int):
        raise ExportError("Meta API returned an invalid numeric metric.")
    try:
        valid = math.isfinite(float(value))
    except (ValueError, OverflowError):
        valid = False
    if not valid:
        raise ExportError("Meta API returned an invalid numeric metric.")


def validate_row(row, fields, breakdowns, daily, since, until):
    if not isinstance(row, dict):
        raise ExportError("Meta API returned an invalid row.")
    for field in fields:
        if field in ("actions", "action_values"):
            actions = row.get(field, [])
            if not isinstance(actions, list):
                raise ExportError("Meta API returned invalid action metrics.")
            for action in actions:
                if not isinstance(action, dict) or not isinstance(action.get("action_type"), str) or not action["action_type"]:
                    raise ExportError("Meta API returned an invalid action.")
                for key in ("value", *ATTRIBUTION_WINDOWS):
                    if key in action:
                        require_number(action[key])
        elif field.endswith(("_id", "_name")):
            if not isinstance(row.get(field), str) or not row[field]:
                raise ExportError("Meta API returned an incomplete identity.")
        elif field != "inline_link_clicks" or field in row:
            require_number(row.get(field))
    for name in breakdowns or []:
        if not isinstance(row.get(name), str) or not row[name]:
            raise ExportError("Meta API returned an incomplete breakdown.")
    if daily:
        text = row.get("date_start")
        if not isinstance(text, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", text):
            raise ExportError("Meta API returned an invalid reporting date.")
        if not since <= date.fromisoformat(text) <= until:
            raise ExportError("Meta API returned an out-of-range date.")


def fetch_meta_insights(
    act_id,
    token,
    since,
    until,
    level,
    fields,
    breakdowns=None,
    action_attribution_windows=None,
    filtering=None,
    time_increment=None,
):
    url = (
        "https://graph.facebook.com/"
        f"{META_API_VERSION}/"
        f"{act_id}/insights"
    )

    params = {
        "level":
            level,
        "time_range":
            json.dumps({
                "since":
                    since.strftime(
                        "%Y-%m-%d"
                    ),
                "until":
                    until.strftime(
                        "%Y-%m-%d"
                    ),
            }),
        "fields":
            ",".join(fields),
        "limit":
            5000,
    }

    if time_increment is not None:
        params["time_increment"] = time_increment

    if breakdowns:
        params[
            "breakdowns"
        ] = ",".join(
            breakdowns
        )

    if action_attribution_windows:
        params[
            "action_attribution_windows"
        ] = json.dumps(
            action_attribution_windows
        )

    if filtering:
        params[
            "filtering"
        ] = json.dumps(
            filtering,
            ensure_ascii=False,
        )

    rows = []
    seen = set()
    while True:
        fallback = not seen and breakdowns and len(breakdowns) == 1 and breakdowns[0] in AUDIENCE_SEGMENT_BREAKDOWN_CANDIDATES
        payload = meta_get(url, params, token, fallback)
        if not isinstance(payload.get("data"), list):
            raise ExportError("Meta API returned invalid data.")
        for row in payload["data"]:
            validate_row(row, fields, breakdowns, time_increment == 1, since, until)
        rows.extend(payload["data"])
        paging = payload.get("paging", {})
        if not isinstance(paging, dict):
            raise ExportError("Meta API returned invalid paging.")
        if "next" not in paging:
            return rows
        cursor = next_cursor(url, paging, params)
        if cursor in seen or token in cursor:
            raise ExportError("Meta API returned an invalid or repeated cursor.")
        seen.add(cursor)
        params["after"] = cursor


def next_cursor(endpoint, paging, params):
    if not isinstance(paging["next"], str):
        raise ExportError("Meta API returned invalid paging.")
    target = urlsplit(paging["next"])
    expected = urlsplit(endpoint)
    if (target.scheme, target.netloc, target.path) != (expected.scheme, expected.netloc, expected.path):
        raise ExportError("Meta API returned an unexpected paging destination.")
    query = parse_qs(target.query, keep_blank_values=True)
    if target.fragment or set(query) - (set(params) | {"after", "access_token"}):
        raise ExportError("Meta API returned unsupported paging.")
    cursors = paging.get("cursors", {})
    if not isinstance(cursors, dict):
        raise ExportError("Meta API returned invalid cursors.")
    candidates = query.get("after", [])
    if len(candidates) > 1:
        raise ExportError("Meta API returned ambiguous paging.")
    cursor = cursors.get("after", candidates[0] if candidates else None)
    if not isinstance(cursor, str) or not cursor or (candidates and candidates != [cursor]):
        raise ExportError("Meta API returned unsupported paging.")
    return cursor


def action_map(
    action_list,
):
    result = {}

    if not isinstance(
        action_list,
        list,
    ):
        return result

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
            result[
                str(action_type)
            ] = action

    return result


def extract_candidate_window(
    action_list,
    candidates,
    window_key,
):
    amap = action_map(
        action_list
    )

    for candidate in candidates:
        action = amap.get(
            candidate
        )

        if action is not None:
            return to_float(
                action.get(
                    window_key
                )
            )

    return 0.0


def extract_candidate_value(
    action_list,
    candidates,
):
    amap = action_map(
        action_list
    )

    for candidate in candidates:
        action = amap.get(
            candidate
        )

        if action is not None:
            return to_float(
                action.get(
                    "value"
                )
            )

    return 0.0


def extract_attribution_metrics(
    item,
):
    actions = item.get(
        "actions",
        [],
    )

    action_values = item.get(
        "action_values",
        [],
    )

    cv_view_1d = extract_candidate_window(
        actions,
        PURCHASE_COUNT_CANDIDATES,
        "1d_view",
    )

    cv_click_7d = extract_candidate_window(
        actions,
        PURCHASE_COUNT_CANDIDATES,
        "7d_click",
    )

    cv_incr = extract_candidate_window(
        actions,
        PURCHASE_COUNT_CANDIDATES,
        "incrementality",
    )

    sale_view_1d = extract_candidate_window(
        action_values,
        PURCHASE_VALUE_CANDIDATES,
        "1d_view",
    )

    sales_click_7d = extract_candidate_window(
        action_values,
        PURCHASE_VALUE_CANDIDATES,
        "7d_click",
    )

    sales_incr = extract_candidate_window(
        action_values,
        PURCHASE_VALUE_CANDIDATES,
        "incrementality",
    )

    return {
        "cv_view_1d":
            cv_view_1d,
        "cv_click_7d":
            cv_click_7d,
        "cv_view1d_click7d":
            cv_view_1d
            + cv_click_7d,
        "cv_incr":
            cv_incr,
        "sale_view_1d":
            sale_view_1d,
        "sales_click_7d":
            sales_click_7d,
        "sales_view1d_click7d":
            sale_view_1d
            + sales_click_7d,
        "sales_incr":
            sales_incr,
    }


def attribution_metric_values(
    metrics,
):
    return [
        metrics.get(
            "cv_view_1d",
            0.0,
        ),
        metrics.get(
            "cv_click_7d",
            0.0,
        ),
        metrics.get(
            "cv_view1d_click7d",
            0.0,
        ),
        metrics.get(
            "cv_incr",
            0.0,
        ),
        metrics.get(
            "sale_view_1d",
            0.0,
        ),
        metrics.get(
            "sales_click_7d",
            0.0,
        ),
        metrics.get(
            "sales_view1d_click7d",
            0.0,
        ),
        metrics.get(
            "sales_incr",
            0.0,
        ),
    ]


def extract_default_metrics(
    item,
):
    actions = item.get(
        "actions",
        [],
    )

    action_values = item.get(
        "action_values",
        [],
    )

    return {
        "impressions":
            to_int(
                item.get(
                    "impressions"
                )
            ),
        "link_clicks":
            to_float(
                item.get(
                    "inline_link_clicks"
                )
            ),
        "spend":
            round(
                to_float(
                    item.get(
                        "spend"
                    )
                ) * 1.2,
                6,
            ),
        "content_view":
            extract_candidate_value(
                actions,
                ["offsite_conversion.fb_pixel_view_content"],
            ),
        "purchase":
            extract_candidate_value(
                actions,
                PURCHASE_COUNT_CANDIDATES,
            ),
        "sales":
            extract_candidate_value(
                action_values,
                PURCHASE_VALUE_CANDIDATES,
            ),
        "lead":
            extract_candidate_value(
                actions,
                META_LEAD_CANDIDATES,
            ),
        "add_to_cart":
            extract_candidate_value(
                actions,
                ADD_TO_CART_CANDIDATES,
            ),
        "clicks_all":
            to_float(
                item.get(
                    "clicks"
                )
            ),
        "post_reactions":
            extract_candidate_value(
                actions,
                POST_REACTION_CANDIDATES,
            ),
        "post_comments":
            extract_candidate_value(
                actions,
                POST_COMMENT_CANDIDATES,
            ),
        "post_saves":
            extract_candidate_value(
                actions,
                POST_SAVE_CANDIDATES,
            ),
        "post_shares":
            extract_candidate_value(
                actions,
                POST_SHARE_CANDIDATES,
            ),
    }


# =========================================================
# gitreport1
# campaign / campaign group
# past 13 months
# reach + attribution CV / sales
# =========================================================

def purchase_sales(item):
    """Same default-attribution purchase/sales as audience_detail."""
    metrics = extract_default_metrics(item)
    return [metrics["purchase"], metrics["sales"]]


def fetch_purchase_sales_lookup(act_id, token, since, until, level,
                                breakdowns=None, filtering=None):
    """Separate default-window query; never replace attribution-window data."""
    id_fields = {"campaign": ["campaign_id"],
                 "adset": ["campaign_id", "adset_id"],
                 "ad": ["campaign_id", "adset_id", "ad_id"],
                 "account": []}[level]
    batch = fetch_meta_insights(
        act_id=act_id, token=token, since=since, until=until,
        level=level, fields=id_fields + ["actions", "action_values"],
        breakdowns=breakdowns, filtering=filtering,
        action_attribution_windows=["default"],
    )
    return {
        tuple(str(item.get(field, "")) for field in id_fields + (breakdowns or [])):
            purchase_sales(item)
        for item in batch
    }


def lookup_purchase_sales(lookup, item, level, breakdowns=None):
    fields = {"campaign": ["campaign_id"],
              "adset": ["campaign_id", "adset_id"],
              "ad": ["campaign_id", "adset_id", "ad_id"],
              "account": []}[level] + (breakdowns or [])
    return lookup.get(tuple(str(item.get(field, "")) for field in fields), [0.0, 0.0])


def build_mon_day_rows(
    act_id,
    token,
    monthly_ranges,
    daily_ranges,
):
    rows = []
    leadads_months = {}  # Reuse existing campaign_month API results; no extra discovery calls.

    # campaign_month: 14ヶ月。reachあり。
    for month_range in monthly_ranges:
        date_serial = to_sheets_date_serial(month_range["since"])

        print(
            "[mon_day] Fetching campaign_month: "
            f"{month_range['label']}"
        )

        campaign_rows = fetch_meta_insights(
            act_id=act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="campaign",
            fields=[
                "campaign_id",
                "campaign_name",
                "reach",
                "actions",
                "action_values",
            ],
            action_attribution_windows=ATTRIBUTION_WINDOWS,
        )

        purchase_lookup = fetch_purchase_sales_lookup(
            act_id, token, month_range["since"], month_range["until"], "campaign"
        )
        for item in campaign_rows:
            # Retain every qualifying campaign ID and its months with actual delivery.
            # Monthly reach > 0 means at least one impression was delivered.
            name = str(item.get("campaign_name", ""))
            cid = str(item.get("campaign_id", ""))
            if (cid and "Camper(CORE-L)" in name and "Leadads" in name
                    and to_int(item.get("reach")) > 0):
                info = leadads_months.setdefault(cid, {"name": name, "months": []})
                info["name"] = name
                info["months"].append(month_range)
            attr = extract_attribution_metrics(item)
            rows.append([
                "campaign_month",
                date_serial,
                item.get("campaign_name", ""),
                to_int(item.get("reach")),
                *lookup_purchase_sales(purchase_lookup, item, "campaign"),
                *attribution_metric_values(attr),
            ])

        # キャンペーングループ（月別のみ）
        for keyword, total_name in FILTER_CAMPAIGN_TOTAL_NAMES.items():
            print(
                "[mon_day] Fetching campaign group: "
                f"{month_range['label']} / {keyword}"
            )

            total_rows = fetch_meta_insights(
                act_id=act_id,
                token=token,
                since=month_range["since"],
                until=month_range["until"],
                level="account",
                fields=["reach", "actions", "action_values"],
                filtering=[{
                    "field": "campaign.name",
                    "operator": "CONTAIN",
                    "value": keyword,
                }],
                action_attribution_windows=ATTRIBUTION_WINDOWS,
            )

            group_purchase_lookup = fetch_purchase_sales_lookup(
                act_id, token, month_range["since"], month_range["until"],
                "account", filtering=[{
                    "field": "campaign.name", "operator": "CONTAIN", "value": keyword
                }],
            )
            reach = 0
            summed = empty_attr_metrics()
            for item in total_rows:
                reach += to_int(item.get("reach"))
                add_attr_metrics(
                    summed,
                    extract_attribution_metrics(item),
                )

            rows.append([
                "campaign_month",
                date_serial,
                total_name,
                reach,
                *group_purchase_lookup.get((), [0.0, 0.0]),
                *attribution_metric_values(summed),
            ])

    # campaign_day: 過去3ヶ月。reach不要なので空欄。1日ずつ取得。
    for month_range in daily_ranges:
        for target_day in iter_dates(
            month_range["since"],
            month_range["until"],
        ):
            print(
                "[mon_day] Fetching campaign_day: "
                f"{target_day:%Y-%m-%d}"
            )

            batch = fetch_meta_insights(
                act_id=act_id,
                token=token,
                since=target_day,
                until=target_day,
                level="campaign",
                fields=[
                    "campaign_id",
                    "campaign_name",
                    "actions",
                    "action_values",
                ],
                action_attribution_windows=ATTRIBUTION_WINDOWS,
            )

            purchase_lookup = fetch_purchase_sales_lookup(
                act_id, token, target_day, target_day, "campaign"
            )
            for item in batch:
                attr = extract_attribution_metrics(item)
                rows.append([
                    "campaign_day",
                    to_sheets_date_serial(target_day),
                    item.get("campaign_name", ""),
                    "",
                    *lookup_purchase_sales(purchase_lookup, item, "campaign"),
                    *attribution_metric_values(attr),
                ])

    # Latest Leadads delivery month only; reuse the 14-month campaign_month results.
    rows.extend(build_campaign_period_rows(act_id, token, leadads_months))

    scope_order = {"campaign_month": 0, "campaign_day": 1, "campaign_period": 2}
    return sorted(
        rows,
        key=lambda r: (
            r[1],
            scope_order.get(r[0], 99),
            r[2],
        ),
    )

def build_campaign_period_rows(act_id, token, leadads_months):
    """Export the latest delivery month's CORE-L Leadads campaigns.

    Monthly insights are already fetched by campaign_month. Find the latest
    delivery month, retain *all* campaigns active in that month, and fetch
    daily insights only for those IDs across their known delivery months.
    Dates are bounded by the existing 14-month campaign_month coverage.
    """
    if not leadads_months:
        print("[mon_day] No CORE-L Leadads campaigns in monthly results")
        return []

    latest_month = max(
        month_range["since"]
        for info in leadads_months.values()
        for month_range in info["months"]
    )
    selected = {
        cid: info for cid, info in leadads_months.items()
        if any(r["since"] == latest_month for r in info["months"])
    }
    print(
        f"[mon_day] Latest Leadads month: {latest_month}; "
        f"selected campaigns: {len(selected)}"
    )

    # Only request days in the span of the selected campaigns' active months.
    # This also handles campaigns that cross month boundaries.
    selected_ranges = [r for info in selected.values() for r in info["months"]]
    search_since = min(r["since"] for r in selected_ranges)
    search_until = max(r["until"] for r in selected_ranges)
    ids = sorted(selected)
    campaign_filter = [{
        "field": "campaign.id", "operator": "IN", "value": ids
    }]
    print(
        f"[mon_day] Detecting selected Leadads delivery days: "
        f"{search_since} to {search_until} / {len(ids)} campaigns"
    )
    daily_rows = fetch_meta_insights(
        act_id=act_id, token=token,
        since=search_since, until=search_until,
        level="campaign",
        fields=["campaign_id", "campaign_name", "impressions", "spend"],
        filtering=campaign_filter,
        time_increment=1,
    )
    active = {}
    for item in daily_rows:
        cid = str(item.get("campaign_id", ""))
        if cid not in selected:
            continue
        if (to_int(item.get("impressions")) <= 0
                and to_float(item.get("spend")) <= 0):
            continue
        day_text = item.get("date_start")
        if not day_text:
            continue
        day = date.fromisoformat(day_text)
        entry = active.setdefault(cid, {"first": day, "last": day})
        entry["first"] = min(entry["first"], day)
        entry["last"] = max(entry["last"], day)

    result = []
    for cid, dates in sorted(active.items(), key=lambda pair: (pair[1]["first"], pair[0])):
        since, until = dates["first"], dates["last"]
        name = selected[cid]["name"]
        one_campaign_filter = [{
            "field": "campaign.id", "operator": "IN", "value": [cid]
        }]
        print(f"[mon_day] Fetching campaign_period: {since} to {until}")
        # Fetch the whole period in one request: reach is deduplicated by Meta,
        # not added up from daily or monthly reach values.
        period_rows = fetch_meta_insights(
            act_id=act_id, token=token, since=since, until=until,
            level="campaign",
            fields=["campaign_id", "campaign_name", "reach", "actions", "action_values"],
            filtering=one_campaign_filter,
            action_attribution_windows=ATTRIBUTION_WINDOWS,
        )
        purchase_lookup = fetch_purchase_sales_lookup(
            act_id, token, since, until, "campaign", filtering=one_campaign_filter
        )
        for item in period_rows:
            if str(item.get("campaign_id", "")) != cid:
                continue
            result.append([
                "campaign_period", to_sheets_date_serial(since),
                item.get("campaign_name", name),
                to_int(item.get("reach")),
                *lookup_purchase_sales(purchase_lookup, item, "campaign"),
                *attribution_metric_values(extract_attribution_metrics(item)),
            ])
    return result


def empty_attr_metrics():
    return {
        "cv_view_1d": 0.0,
        "cv_click_7d": 0.0,
        "cv_view1d_click7d": 0.0,
        "cv_incr": 0.0,
        "sale_view_1d": 0.0,
        "sales_click_7d": 0.0,
        "sales_view1d_click7d": 0.0,
        "sales_incr": 0.0,
    }


def add_attr_metrics(
    target,
    source,
):
    for key in target.keys():
        target[key] += to_float(
            source.get(
                key,
                0.0,
            )
        )


# =========================================================
# gitreport2
# campaign_day / ad / campaign_gen_age / campaign_pf
# past 2 months
# attribution CV / sales
# =========================================================

def build_ad_rows(act_id, token, month_ranges):
    rows = []
    scopes = [
        ("campaign", "campaign", ["campaign_name"]),
        ("adset", "adset", ["campaign_name", "adset_name"]),
        ("ad", "ad", ["campaign_name", "adset_name", "ad_name"]),
    ]
    for month_range in month_ranges:
        month_serial = to_sheets_date_serial(month_range["since"])
        for scope, level, dimensions in scopes:
            print(f"[ad] Fetching {scope}: {month_range['label']}")
            batch = fetch_meta_insights(
                act_id=act_id, token=token,
                since=month_range["since"], until=month_range["until"],
                level=level,
                fields=(["campaign_id"] +
                        (["adset_id"] if level in ("adset", "ad") else []) +
                        (["ad_id"] if level == "ad" else []) +
                        dimensions + ["actions", "action_values"]),
                action_attribution_windows=ATTRIBUTION_WINDOWS,
            )
            purchase_lookup = fetch_purchase_sales_lookup(
                act_id, token, month_range["since"], month_range["until"], level
            )
            for item in batch:
                rows.append([
                    scope, month_serial,
                    item.get("campaign_name", ""),
                    item.get("adset_name", "") if scope != "campaign" else "",
                    item.get("ad_name", "") if scope == "ad" else "",
                    *lookup_purchase_sales(purchase_lookup, item, level),
                    *attribution_metric_values(extract_attribution_metrics(item)),
                ])
    order = {"campaign": 0, "adset": 1, "ad": 2}
    return sorted(rows, key=lambda r: (r[1], order[r[0]], r[2], r[3], r[4]))


# =========================================================
# audience: campaign breakdowns / attribution metrics
# =========================================================

GENDER_NAMES = {"male": "男性", "female": "女性", "unknown": "不明"}
PLATFORM_NAMES = {
    "instagram": "Instagram", "facebook": "Facebook",
    "audience_network": "Audience Network", "threads": "Threads",
    "messenger": "Messenger", "uncategorized": "Uncategorized",
}

def normalize_gender(value):
    """Normalize Meta gender breakdown values for both audience sheets."""
    if value is None:
        return ""
    normalized = str(value).strip().lower()
    return GENDER_NAMES.get(normalized, str(value).strip())


def normalize_platform(value):
    return PLATFORM_NAMES.get(str(value).lower(), value or "")


def build_audience_rows(act_id, token, month_ranges):
    rows = []
    scopes = [
        ("campaign_gen", "gender"),
        ("campaign_age", "age"),
        ("campaign_pf", "publisher_platform"),
    ]
    for month_range in month_ranges:
        serial = to_sheets_date_serial(month_range["since"])
        for scope, breakdown in scopes:
            print(f"[audience] Fetching {scope}: {month_range['label']}")
            batch = fetch_meta_insights(
                act_id=act_id, token=token,
                since=month_range["since"], until=month_range["until"],
                level="campaign", fields=["campaign_id", "campaign_name", "actions", "action_values"],
                breakdowns=[breakdown],
                action_attribution_windows=ATTRIBUTION_WINDOWS,
            )
            purchase_lookup = fetch_purchase_sales_lookup(
                act_id, token, month_range["since"], month_range["until"],
                "campaign", breakdowns=[breakdown]
            )
            for item in batch:
                audience = item.get(breakdown, "")
                if breakdown == "gender":
                    audience = normalize_gender(audience)
                elif breakdown == "publisher_platform":
                    audience = normalize_platform(audience)
                rows.append([
                    scope, serial, item.get("campaign_name", ""), audience,
                    *lookup_purchase_sales(purchase_lookup, item, "campaign", [breakdown]),
                    *attribution_metric_values(extract_attribution_metrics(item)),
                ])
    order = {name: i for i, (name, _) in enumerate(scopes)}
    return sorted(rows, key=lambda r: (r[1], order[r[0]], r[2], str(r[3])))


def build_audience_detail_rows(act_id, token, month_ranges):
    rows = []
    scopes = [
        ("adset_pf", "adset", ["publisher_platform"]),
        ("adset_gen_age", "adset", ["gender", "age"]),
        ("campaign_pm", "campaign", [
            "publisher_platform", "platform_position", "device_platform"
        ]),
    ]
    for month_range in month_ranges:
        serial = to_sheets_date_serial(month_range["since"])
        for scope, level, breakdowns in scopes:
            rows.extend(build_audience_detail_scope(
                act_id, token, month_range, serial, scope, level, breakdowns
            ))
    order = {name: i for i, (name, _, _) in enumerate(scopes)}
    return sorted(rows, key=lambda r: (r[1], order[r[0]], *map(str, r[2:9])))


def build_audience_detail_scope(act_id, token, month_range, serial,
                                scope, level, breakdowns):
    # Fetch separately: default metrics and custom attribution windows.
    # Merge by stable IDs and raw breakdown values, not translated labels.
    identity_fields = ["campaign_id", "campaign_name"]
    if level == "adset":
        identity_fields += ["adset_id", "adset_name"]
    shared = dict(
        act_id=act_id, token=token,
        since=month_range["since"], until=month_range["until"],
        level=level, breakdowns=breakdowns,
    )
    print(f"[audience(detail)] Fetching default: {scope} / {month_range['label']}")
    default_rows = fetch_meta_insights(
        **shared,
        fields=identity_fields + [
            "impressions", "inline_link_clicks", "spend", "clicks",
            "actions", "action_values"
        ],
        action_attribution_windows=["default"],
    )
    print(f"[audience(detail)] Fetching attribution: {scope} / {month_range['label']}")
    attr_rows = fetch_meta_insights(
        **shared,
        fields=identity_fields + ["actions", "action_values"],
        action_attribution_windows=ATTRIBUTION_WINDOWS,
    )

    def key_for(item):
        return (
            str(item.get("campaign_id", "")),
            str(item.get("adset_id", "")) if level == "adset" else "",
            *(str(item.get(field, "")) for field in breakdowns),
        )

    merged = {}
    for item in default_rows:
        merged[key_for(item)] = {
            "item": item,
            "default": extract_default_metrics(item),
            "attr": empty_attr_metrics(),
        }
    for item in attr_rows:
        key = key_for(item)
        if key not in merged:
            merged[key] = {
                "item": item,
                "default": empty_default_metrics(),
                "attr": empty_attr_metrics(),
            }
        merged[key]["attr"] = extract_attribution_metrics(item)

    result = []
    for entry in merged.values():
        item, default, attr = entry["item"], entry["default"], entry["attr"]
        result.append([
            scope, serial,
            item.get("campaign_name", ""),
            item.get("adset_name", "") if level == "adset" else "",
            normalize_gender(item.get("gender", "")),
            item.get("age", ""),
            normalize_platform(item.get("publisher_platform", "")),
            item.get("platform_position", ""),
            item.get("device_platform", ""),
            *[default[name] for name in DEFAULT_METRIC_NAMES],
            *attribution_metric_values(attr),
        ])
    return result


DEFAULT_METRIC_NAMES = [
    "impressions", "link_clicks", "spend", "content_view", "purchase", "sales", "lead",
    "add_to_cart", "clicks_all", "post_reactions", "post_comments",
    "post_saves", "post_shares",
]


def empty_default_metrics():
    return {name: 0 for name in DEFAULT_METRIC_NAMES}


# =========================================================
# segments
# campaign x audience_segment
# past 3 months
# impressions / spend x1.2 / cv
# =========================================================

def build_segments_rows(
    act_id,
    token,
    month_ranges,
):
    rows = []

    for month_range in month_ranges:
        month_serial = (
            to_sheets_date_serial(
                month_range["since"]
            )
        )

        print(
            "[segments] "
            "Fetching audience segment: "
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

            except UnsupportedBreakdown:
                continue
            # A validated empty page is a successful zero-row report.
            chosen_breakdown = breakdown
            segment_rows = rows_try
            break
        if chosen_breakdown is None:
            raise ExportError("Meta API rejected all audience segment breakdowns.")


        for item in segment_rows:
            # 既存auseと同じ:
            # offsite_conversion.fb_pixel_purchase の value
            cv = extract_candidate_value(
                item.get(
                    "actions",
                    [],
                ),
                [
                    "offsite_conversion.fb_pixel_purchase"
                ],
            )

            rows.append([
                month_serial,
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
                        item.get(
                            "spend"
                        )
                    ) * 1.2,
                    6,
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


def connect_spreadsheets(
    sheet_ids,
    google_creds_dict,
):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_dict, scope)
        client = gspread.authorize(creds)
    except Exception:
        raise ExportError("Google service account credentials could not be loaded (gcp_service_account).") from None

    books = []
    for index, sheet_id in enumerate(sheet_ids, start=1):
        with report_step(f"Open Google Sheets destination {index}"):
            books.append(client.open_by_key(sheet_id))
    return books


ATTRIBUTION_HEADERS = [
    "cv_view_1d", "cv_click_7d", "cv_view1d_click7d", "cv_incr",
    "sale_view_1d", "sales_click_7d", "sales_view1d_click7d", "sales_incr",
]
MON_DAY_HEADER = ["scope", "date", "campaign_name", "reach", "purchase", "sales"] + ATTRIBUTION_HEADERS
AD_HEADER = ["scope", "month", "campaign_name", "adset_name", "ad_name", "purchase", "sales"] + ATTRIBUTION_HEADERS
AUDIENCE_HEADER = ["scope", "month", "campaign_name", "audience", "purchase", "sales"] + ATTRIBUTION_HEADERS
AUDIENCE_DETAIL_HEADER = [
    "scope", "month", "campaign_name", "adset_name", "gender", "age", "platform",
    "placement", "device_platform",
] + DEFAULT_METRIC_NAMES + ATTRIBUTION_HEADERS
SEGMENTS_HEADER = ["month", "campaign_name", "audience_segment", "impressions", "spend", "cv"]

LOCK_NAME = "camp_shoes_export_lock_v1"
LOCK_PREFIX = "_camp_shoes_export_"
LEASE_SECONDS = 6 * 60 * 60


def active_lock(book):
    metadata = book.fetch_sheet_metadata(params={
        "fields": "sheets(properties(sheetId,title,hidden)),namedRanges(namedRangeId,name,range(sheetId))"
    })
    for item in metadata.get("namedRanges", []):
        if item["name"].casefold() != LOCK_NAME:
            continue
        guard_id = item["namedRangeId"]
        sheet_id = item["range"]["sheetId"]
        for sheet in metadata["sheets"]:
            props = sheet["properties"]
            if props["sheetId"] != sheet_id:
                continue
            title = props["title"]
            match = re.fullmatch(LOCK_PREFIX + r"([0-9]+)_([a-f0-9]{32})", title)
            if match and props.get("hidden") and guard_id == "camp_shoes_" + match[2]:
                return (guard_id, sheet_id, title, int(match[1]))
        raise ExportError("Sheets lock metadata is invalid.")
    return None


def remove_lock(owner):
    # Both operations are atomic. A retired owner cannot delete a successor's tab.
    return [
        {"deleteNamedRange": {"namedRangeId": owner[0]}},
        {"deleteSheet": {"sheetId": owner[1]}},
    ]


@contextmanager
def export_lock(book):
    previous = active_lock(book)
    now = int(time.time())
    if previous and previous[3] > now:
        raise ExportError("Another export holds the Sheets lock.")
    nonce = uuid.uuid4().hex
    sheet_id = 1 + uuid.uuid4().int % 2147483646
    expires = now + LEASE_SECONDS
    title = f"{LOCK_PREFIX}{expires}_{nonce}"
    guard_id = "camp_shoes_" + nonce
    owner = (guard_id, sheet_id, title, expires)
    commands = remove_lock(previous) if previous else []
    commands.extend([
        {"addSheet": {"properties": {
            "sheetId": sheet_id, "title": title, "hidden": True,
            "gridProperties": {"rowCount": 1, "columnCount": 1},
        }}},
        {"addNamedRange": {"namedRange": {
            "namedRangeId": guard_id, "name": LOCK_NAME,
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": 1},
        }}},
    ])
    try:
        book.batch_update({"requests": commands})
    except Exception as error:
        try:
            confirmed = active_lock(book) == owner
        except Exception:
            confirmed = False
        if not confirmed:
            raise ExportError(f"Lock acquisition unconfirmed: {sheets_error_detail(error)}.") from None
    try:
        yield owner
    finally:
        try:
            book.batch_update({"requests": remove_lock(owner)})
        except Exception:
            try:
                released = active_lock(book) != owner
            except Exception:
                released = False
            if not released:
                print("Sheets lock release unconfirmed. The lease expires automatically.", file=sys.stderr)


def raw_cell(value):
    if value == "":
        return {}
    if isinstance(value, str):
        return {"userEnteredValue": {"stringValue": value}}
    return {"userEnteredValue": {"numberValue": value}}


def publication_requests(book, owner, reports):
    metadata = book.fetch_sheet_metadata(params={"fields": "sheets(properties(sheetId,title,gridProperties))"})
    sheets = {item["properties"]["title"]: item["properties"] for item in metadata["sheets"]}
    ids = {props["sheetId"] for props in sheets.values()}
    # Validate the unique lease in the very same atomic batch as all five writes.
    commands = [{"updateNamedRange": {"namedRange": {"namedRangeId": owner[0], "name": LOCK_NAME},
                                      "fields": "name"}}]
    for title, header, rows in reports:
        width, height = len(header), 1 + len(rows)
        props = sheets.get(title)
        if props is None:
            sheet_id = 1 + uuid.uuid4().int % 2147483646
            while sheet_id in ids:
                sheet_id = 1 + uuid.uuid4().int % 2147483646
            ids.add(sheet_id)
            commands.append({"addSheet": {"properties": {
                "sheetId": sheet_id, "title": title,
                "gridProperties": {"columnCount": width, "rowCount": max(1000, height)},
            }}})
        else:
            sheet_id = props["sheetId"]
            grid = props["gridProperties"]
            for dimension, need, current in (("ROWS", height, grid["rowCount"]),
                                              ("COLUMNS", width, grid["columnCount"])):
                if need > current:
                    commands.append({"appendDimension": {"sheetId": sheet_id, "dimension": dimension,
                                                         "length": need - current}})
        # An open-ended row range clears previous values below the new output.
        # Updating userEnteredValue alone preserves formatting and the other columns.
        commands.append({"updateCells": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "startColumnIndex": 0,
                      "endColumnIndex": width},
            "fields": "userEnteredValue",
            "rows": [{"values": [raw_cell(value) for value in row]} for row in [header, *rows]],
        }})
    commands.append({"updateCells": {
        "start": {"sheetId": owner[1], "rowIndex": 0, "columnIndex": 0},
        "fields": "userEnteredValue", "rows": [{"values": [raw_cell("PUBLISHED")]}],
    }})
    return commands


def publish(book, owner, commands):
    try:
        book.batch_update({"requests": commands})
    except Exception as error:
        detail = sheets_error_detail(error)
        try:
            result = book.values_get(f"'{owner[2]}'!A1")
        except Exception:
            raise ExportError(f"Publication outcome unknown: {detail}.") from None
        if result.get("values") != [["PUBLISHED"]]:
            raise ExportError(f"Publication not confirmed: {detail}.") from None


def to_float(
    value,
):
    try:
        return float(
            value
        )
    except (
        TypeError,
        ValueError,
    ):
        return 0.0


def to_int(
    value,
):
    try:
        return int(
            float(
                value
            )
        )
    except (
        TypeError,
        ValueError,
    ):
        return 0


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        main()
    except ExportError as error:
        sys.exit(str(error))
    except Exception:
        sys.exit("Export failed after the last logged step.")
