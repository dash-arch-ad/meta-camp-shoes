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
    "CV最適化": "CV最適化_合計",
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
    print("=== Start Meta 4-sheet export ===")

    config = load_secret()
    mask_sensitive_values(config)

    resolved = resolve_config(config)
    validate_config(resolved)

    act_id = normalize_meta_act_id(
        resolved["meta"]["account_id"]
    )
    token = resolved["meta"]["token"]

    ranges_13m = get_month_ranges(GITREPORT1_MONTHS)
    ranges_3m = get_month_ranges(GITREPORT2_MONTHS)

    print("mon_day monthly ranges:", format_ranges(ranges_13m))
    print("mon_day daily ranges:", format_ranges(ranges_3m))
    print("ad ranges:", format_ranges(ranges_3m))
    print("audience ranges:", format_ranges(ranges_3m))
    print("audience(detail) ranges:", format_ranges(ranges_3m))
    print("segments ranges:", format_ranges(ranges_3m))

    spreadsheets = connect_spreadsheets(
        sheet_ids=resolved["sheet"]["spreadsheet_ids"],
        google_creds_dict=resolved["sheet"]["google_service_account"],
    )

    failures = []

    run_export(
        "mon_day",
        lambda: build_mon_day_rows(
            act_id=act_id,
            token=token,
            monthly_ranges=ranges_13m,
            daily_ranges=ranges_3m,
        ),
        lambda rows: write_all(spreadsheets, write_mon_day, rows),
        failures,
    )

    run_export(
        "ad",
        lambda: build_ad_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_3m,
        ),
        lambda rows: write_all(spreadsheets, write_ad, rows),
        failures,
    )

    run_export(
        "audience",
        lambda: build_audience_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_3m,
        ),
        lambda rows: write_all(spreadsheets, write_audience, rows),
        failures,
    )

    run_export(
        "audience(detail)",
        lambda: build_audience_detail_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_3m,
        ),
        lambda rows: write_all(spreadsheets, write_audience_detail, rows),
        failures,
    )

    run_export(
        "segments",
        lambda: build_segments_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_3m,
        ),
        lambda rows: write_all(spreadsheets, write_segments, rows),
        failures,
    )

    if failures:
        summary = " | ".join(
            f"{name}: {repr(err)}"
            for name, err in failures
        )
        raise RuntimeError(
            f"One or more sheet exports failed: {summary}"
        )

    print("=== All exports completed ===")

def run_export(
    name,
    build_func,
    write_func,
    failures,
):
    print(f"\n=== Start {name} ===")

    try:
        rows = build_func()
        print(
            f"{name} rows built: {len(rows)}"
        )

        write_func(rows)

        print(
            f"=== Completed {name} ==="
        )

    except Exception as e:
        failures.append(
            (name, e)
        )
        print(
            f"=== FAILED {name}: "
            f"{repr(e)} ==="
        )


def write_all(
    spreadsheets,
    writer,
    rows,
):
    for spreadsheet in spreadsheets:
        writer(
            spreadsheet,
            rows,
        )


# =========================================================
# Secret / Config
# =========================================================

def load_secret():
    raw = os.environ.get(
        "APP_SECRET_JSON"
    )

    if not raw:
        raise RuntimeError(
            "APP_SECRET_JSON is not set"
        )

    try:
        return json.loads(raw)

    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"APP_SECRET_JSON is invalid JSON: {e}"
        ) from e


def mask_sensitive_values(config):
    meta = config.get(
        "meta",
        {},
    )

    for value in [
        meta.get("token"),
        meta.get("account_id"),
    ]:
        if not value:
            continue

        value = str(
            value
        ).strip()

        if value and "\n" not in value:
            print(
                f"::add-mask::{value}"
            )


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
        raise RuntimeError(
            "Missing required config keys: "
            + ", ".join(missing)
        )


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

def meta_get(
    url,
    params=None,
):
    last_error = None

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):
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
                    isinstance(
                        payload,
                        dict,
                    )
                    and payload.get(
                        "error"
                    )
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
                response.status_code
                >= 500
            )

            if (
                isinstance(
                    payload,
                    dict,
                )
                and payload.get(
                    "error"
                )
            ):
                code = (
                    payload["error"]
                    .get("code")
                )

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
                or attempt
                == MAX_RETRIES
            ):
                raise last_error

        except requests.RequestException as e:
            last_error = e

            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    "Meta request error: "
                    f"{repr(e)}"
                ) from e

        wait_sec = (
            2 ** (attempt - 1)
        )

        print(
            "Meta retry "
            f"{attempt}/{MAX_RETRIES}, "
            f"sleep={wait_sec}s"
        )

        time.sleep(
            wait_sec
        )

    raise RuntimeError(
        "Meta API request failed: "
        f"{last_error}"
    )


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
        "access_token":
            token,
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

    context = {
        "level":
            level,
        "since":
            since.strftime(
                "%Y-%m-%d"
            ),
        "until":
            until.strftime(
                "%Y-%m-%d"
            ),
        "breakdowns":
            breakdowns or [],
        "action_attribution_windows":
            action_attribution_windows or [],
        "filtering":
            filtering or [],
        "time_increment": time_increment,
        "fields":
            fields,
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
                "context="
                f"{json.dumps(context, ensure_ascii=False)}; "
                f"error={e}"
            ) from e

        payload = response.json()

        rows.extend(
            payload.get(
                "data",
                [],
            )
        )

        next_url = (
            payload
            .get(
                "paging",
                {},
            )
            .get("next")
        )

        if not next_url:
            break

        url = next_url
        params = None

    return rows


# =========================================================
# Common action extractors
# =========================================================

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

    # campaign_month: 過去13ヶ月。reachあり。
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

    # Leadads: one row per campaign for its active period (within 14 months).
    rows.extend(build_campaign_period_rows(act_id, token, daily_ranges))

    scope_order = {"campaign_month": 0, "campaign_day": 1, "campaign_period": 2}
    return sorted(
        rows,
        key=lambda r: (
            r[1],
            scope_order.get(r[0], 99),
            r[2],
        ),
    )

def build_campaign_period_rows(act_id, token, period_ranges):
    """Aggregate each active CORE-L Leadads campaign across month boundaries.

    Activity is determined from daily impressions or spend, within the same
    3-month window as campaign_day. A campaign gets exactly one row.
    """
    active_campaigns = {}
    for month_range in period_ranges:
        print(f"[mon_day] Detecting Leadads activity: {month_range['label']}")
        daily_rows = fetch_meta_insights(
            act_id=act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="campaign",
            fields=["campaign_id", "campaign_name", "impressions", "spend"],
            filtering=[{
                "field": "campaign.name", "operator": "CONTAIN", "value": "Leadads"
            }],
            time_increment=1,
        )
        for item in daily_rows:
            name = str(item.get("campaign_name", ""))
            if "Camper(CORE-L)" not in name or "Leadads" not in name:
                continue
            if to_int(item.get("impressions")) <= 0 and to_float(item.get("spend")) <= 0:
                continue
            campaign_id = str(item.get("campaign_id", ""))
            day_text = item.get("date_start")
            if not campaign_id or not day_text:
                continue
            day = date.fromisoformat(day_text)
            if campaign_id not in active_campaigns:
                active_campaigns[campaign_id] = {
                    "first": day, "last": day, "name": name
                }
            else:
                entry = active_campaigns[campaign_id]
                entry["first"] = min(entry["first"], day)
                entry["last"] = max(entry["last"], day)
                entry["name"] = name

    result = []
    for campaign_id, entry in sorted(active_campaigns.items(), key=lambda x: (x[1]["first"], x[0])):
        since, until = entry["first"], entry["last"]
        print(f"[mon_day] Fetching campaign_period: {entry['name']} / {since} to {until}")
        campaign_filter = [{
            "field": "campaign.id", "operator": "IN", "value": [campaign_id]
        }]
        period_rows = fetch_meta_insights(
            act_id=act_id, token=token, since=since, until=until,
            level="campaign",
            fields=["campaign_id", "campaign_name", "reach", "actions", "action_values"],
            filtering=campaign_filter,
            action_attribution_windows=ATTRIBUTION_WINDOWS,
        )
        purchase_lookup = fetch_purchase_sales_lookup(
            act_id, token, since, until, "campaign", filtering=campaign_filter
        )
        for item in period_rows:
            if str(item.get("campaign_id", "")) != campaign_id:
                continue
            result.append([
                "campaign_period", to_sheets_date_serial(since),
                item.get("campaign_name", entry["name"]),
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

            except RuntimeError as e:
                print(
                    "[segments] "
                    f"breakdown='{breakdown}' "
                    f"API error: {e}"
                )
                continue

            if has_real_breakdown(
                rows_try,
                breakdown,
            ):
                chosen_breakdown = (
                    breakdown
                )
                segment_rows = (
                    rows_try
                )
                break

            print(
                "[segments] "
                f"breakdown='{breakdown}' "
                "returned no usable values"
            )

        if not chosen_breakdown:
            raise RuntimeError(
                "segments failed: none of "
                "the Audience Segment breakdown "
                "candidates returned usable data. "
                f"month={month_range['label']}; "
                "candidates="
                f"{AUDIENCE_SEGMENT_BREAKDOWN_CANDIDATES}"
            )

        print(
            "[segments] "
            f"{month_range['label']} "
            f"using breakdown="
            f"'{chosen_breakdown}', "
            f"rows={len(segment_rows)}"
        )

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
            spreadsheet = (
                client.open_by_key(
                    sheet_id
                )
            )

            spreadsheets.append(
                spreadsheet
            )

            print(
                "Google Sheets connected: "
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
        worksheet = (
            spreadsheet.worksheet(
                sheet_name
            )
        )

    except gspread.WorksheetNotFound:
        worksheet = (
            spreadsheet.add_worksheet(
                title=sheet_name,
                rows=1000,
                cols=cols,
            )
        )

    # 既存シートの列数が足りない場合は拡張。
    if worksheet.col_count < cols:
        worksheet.resize(
            cols=cols
        )

    return worksheet


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

    # 日付はシリアル値として数値で出力。
    # 表示形式はスプレッドシート側の既存設定に任せる。
    worksheet.update(
        range_name="A1",
        values=output,
        raw=True,
    )

    print(
        "Write success: "
        f"spreadsheet={spreadsheet.id}, "
        f"sheet={sheet_name}, "
        f"rows={len(rows)}"
    )


def write_mon_day(
    spreadsheet,
    rows,
):
    header = [
        "scope",
        "date",
        "campaign_name",
        "reach",
        "purchase",
        "sales",
        "cv_view_1d",
        "cv_click_7d",
        "cv_view1d_click7d",
        "cv_incr",
        "sale_view_1d",
        "sales_click_7d",
        "sales_view1d_click7d",
        "sales_incr",
    ]
    write_sheet(spreadsheet, SHEET_MON_DAY, header, rows)


def write_ad(
    spreadsheet,
    rows,
):
    header = [
        "scope",
        "month",
        "campaign_name",
        "adset_name",
        "ad_name",
        "purchase",
        "sales",
        "cv_view_1d",
        "cv_click_7d",
        "cv_view1d_click7d",
        "cv_incr",
        "sale_view_1d",
        "sales_click_7d",
        "sales_view1d_click7d",
        "sales_incr",
    ]
    write_sheet(spreadsheet, SHEET_AD, header, rows)


ATTRIBUTION_HEADERS = [
    "cv_view_1d", "cv_click_7d", "cv_view1d_click7d", "cv_incr",
    "sale_view_1d", "sales_click_7d", "sales_view1d_click7d", "sales_incr",
]


def write_audience(spreadsheet, rows):
    header = ["scope", "month", "campaign_name", "audience", "purchase", "sales"] + ATTRIBUTION_HEADERS
    write_sheet(spreadsheet, SHEET_AUDIENCE, header, rows)


def write_audience_detail(spreadsheet, rows):
    header = [
        "scope", "month", "campaign_name", "adset_name", "gender", "age",
        "platform", "placement", "device_platform",
    ] + DEFAULT_METRIC_NAMES + ATTRIBUTION_HEADERS
    write_sheet(spreadsheet, SHEET_AUDIENCE_DETAIL, header, rows)


def write_segments(
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
    write_sheet(spreadsheet, SHEET_SEGMENTS, header, rows)


# =========================================================
# Utils
# =========================================================

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


def truncate_text(
    value,
    limit=1400,
):
    value = str(
        value
    )

    if len(value) <= limit:
        return value

    return (
        value[:limit]
        + "...(truncated)"
    )


if __name__ == "__main__":
    main()
