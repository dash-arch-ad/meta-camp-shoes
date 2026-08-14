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
SHEET_GITREPORT4 = "gitreport4"

GITREPORT1_MONTHS = 13
GITREPORT2_MONTHS = 2
GITREPORT3_MONTHS = 2
GITREPORT4_MONTHS = 2

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
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
    "purchase",
]

PURCHASE_VALUE_CANDIDATES = [
    "omni_purchase",
    "purchase",
    "offsite_conversion.fb_pixel_purchase",
]

ADD_TO_CART_CANDIDATES = [
    "omni_add_to_cart",
    "offsite_conversion.fb_pixel_add_to_cart",
    "add_to_cart",
]

POST_REACTION_CANDIDATES = [
    "post_reaction",
]

POST_COMMENT_CANDIDATES = [
    "comment",
]

POST_SAVE_CANDIDATES = [
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

    ranges_13m = get_month_ranges(
        GITREPORT1_MONTHS
    )
    ranges_2m = get_month_ranges(
        GITREPORT2_MONTHS
    )

    print(
        "gitreport1 ranges:",
        format_ranges(ranges_13m),
    )
    print(
        "gitreport2 ranges:",
        format_ranges(ranges_2m),
    )
    print(
        "gitreport3 ranges:",
        format_ranges(ranges_2m),
    )
    print(
        "gitreport4 ranges:",
        format_ranges(ranges_2m),
    )

    spreadsheets = connect_spreadsheets(
        sheet_ids=resolved["sheet"]["spreadsheet_ids"],
        google_creds_dict=resolved["sheet"]["google_service_account"],
    )

    failures = []

    run_export(
        "gitreport1",
        lambda: build_gitreport1_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_13m,
        ),
        lambda rows: write_all(
            spreadsheets,
            write_gitreport1,
            rows,
        ),
        failures,
    )

    run_export(
        "gitreport2",
        lambda: build_gitreport2_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_2m,
        ),
        lambda rows: write_all(
            spreadsheets,
            write_gitreport2,
            rows,
        ),
        failures,
    )

    run_export(
        "gitreport3",
        lambda: build_gitreport3_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_2m,
        ),
        lambda rows: write_all(
            spreadsheets,
            write_gitreport3,
            rows,
        ),
        failures,
    )

    run_export(
        "gitreport4",
        lambda: build_gitreport4_rows(
            act_id=act_id,
            token=token,
            month_ranges=ranges_2m,
        ),
        lambda rows: write_all(
            spreadsheets,
            write_gitreport4,
            rows,
        ),
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

def build_gitreport1_rows(
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
            "[gitreport1] "
            "Fetching campaign: "
            f"{month_range['label']}"
        )

        campaign_rows = fetch_meta_insights(
            act_id=act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="campaign",
            fields=[
                "campaign_name",
                "reach",
                "actions",
                "action_values",
            ],
            action_attribution_windows=
                ATTRIBUTION_WINDOWS,
        )

        for item in campaign_rows:
            attr = (
                extract_attribution_metrics(
                    item
                )
            )

            rows.append([
                month_serial,
                item.get(
                    "campaign_name",
                    "",
                ),
                to_int(
                    item.get(
                        "reach"
                    )
                ),
                *attribution_metric_values(
                    attr
                ),
            ])

        for keyword, total_name in (
            FILTER_CAMPAIGN_TOTAL_NAMES.items()
        ):
            print(
                "[gitreport1] "
                "Fetching campaign group: "
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
                    "actions",
                    "action_values",
                ],
                filtering=[
                    {
                        "field":
                            "campaign.name",
                        "operator":
                            "CONTAIN",
                        "value":
                            keyword,
                    }
                ],
                action_attribution_windows=
                    ATTRIBUTION_WINDOWS,
            )

            if total_rows:
                # account level + 1ヶ月rangeでは通常1行。
                # 複数行なら各指標を安全に加算。
                reach = 0
                summed = empty_attr_metrics()

                for item in total_rows:
                    reach += to_int(
                        item.get(
                            "reach"
                        )
                    )

                    add_attr_metrics(
                        summed,
                        extract_attribution_metrics(
                            item
                        ),
                    )

                rows.append([
                    month_serial,
                    total_name,
                    reach,
                    *attribution_metric_values(
                        summed
                    ),
                ])

            else:
                rows.append([
                    month_serial,
                    total_name,
                    0,
                    *attribution_metric_values(
                        empty_attr_metrics()
                    ),
                ])

    return sorted(
        rows,
        key=lambda r: (
            r[0],
            r[1],
        ),
    )


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

def build_gitreport2_rows(
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

        # ---------------------------------------------
        # campaign_day
        # API安定性優先: 1日ずつ取得
        # ---------------------------------------------
        for target_day in iter_dates(
            month_range["since"],
            month_range["until"],
        ):
            print(
                "[gitreport2] "
                "Fetching campaign_day: "
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
                attr = (
                    extract_attribution_metrics(
                        item
                    )
                )

                rows.append(
                    make_gitreport2_row(
                        scope="campaign_day",
                        month=month_serial,
                        day=to_sheets_date_serial(
                            target_day
                        ),
                        item=item,
                        attr=attr,
                    )
                )

        # ---------------------------------------------
        # ad
        # ---------------------------------------------
        print(
            "[gitreport2] "
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
                make_gitreport2_row(
                    scope="ad",
                    month=month_serial,
                    day="",
                    item=item,
                    attr=extract_attribution_metrics(
                        item
                    ),
                )
            )

        # ---------------------------------------------
        # campaign_gen_age
        # ---------------------------------------------
        print(
            "[gitreport2] "
            "Fetching campaign_gen_age: "
            f"{month_range['label']}"
        )

        batch = fetch_meta_insights(
            act_id=act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="campaign",
            fields=[
                "campaign_name",
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
                make_gitreport2_row(
                    scope="campaign_gen_age",
                    month=month_serial,
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
                    attr=extract_attribution_metrics(
                        item
                    ),
                )
            )

        # ---------------------------------------------
        # campaign_pf
        # ---------------------------------------------
        print(
            "[gitreport2] "
            "Fetching campaign_pf: "
            f"{month_range['label']}"
        )

        batch = fetch_meta_insights(
            act_id=act_id,
            token=token,
            since=month_range["since"],
            until=month_range["until"],
            level="campaign",
            fields=[
                "campaign_name",
                "actions",
                "action_values",
            ],
            breakdowns=[
                "publisher_platform",
            ],
            action_attribution_windows=
                ATTRIBUTION_WINDOWS,
        )

        for item in batch:
            rows.append(
                make_gitreport2_row(
                    scope="campaign_pf",
                    month=month_serial,
                    day="",
                    item=item,
                    platform=item.get(
                        "publisher_platform",
                        "",
                    ),
                    attr=extract_attribution_metrics(
                        item
                    ),
                )
            )

    return sort_gitreport2(
        rows
    )


def make_gitreport2_row(
    scope,
    month,
    day,
    item,
    attr,
    gender="",
    age="",
    platform="",
):
    return [
        "meta",
        scope,
        month,
        day,
        item.get(
            "campaign_name",
            "",
        ),
        item.get(
            "adset_name",
            "",
        ),
        item.get(
            "ad_name",
            "",
        ),
        gender,
        age,
        platform,
        *attribution_metric_values(
            attr
        ),
    ]


def sort_gitreport2(
    rows,
):
    scope_order = {
        "campaign_day": 0,
        "ad": 1,
        "campaign_gen_age": 2,
        "campaign_pf": 3,
    }

    return sorted(
        rows,
        key=lambda r: (
            r[2],
            scope_order.get(
                r[1],
                99,
            ),
            r[3] if r[3] != "" else 0,
            r[4],
            r[5],
            r[6],
            r[7],
            r[8],
            r[9],
        ),
    )


# =========================================================
# gitreport3
# adset_gen_age / adset_pm
# past 2 months
# normal metrics + default CV + attribution CV/sales
# =========================================================

def build_gitreport3_rows(
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

        # ---------------------------------------------
        # adset_gen_age
        # ---------------------------------------------
        rows.extend(
            build_gitreport3_scope(
                act_id=act_id,
                token=token,
                month_range=month_range,
                month_serial=month_serial,
                scope="adset_gen_age",
                breakdowns=[
                    "gender",
                    "age",
                ],
            )
        )

        # ---------------------------------------------
        # adset_pm
        # ---------------------------------------------
        rows.extend(
            build_gitreport3_scope(
                act_id=act_id,
                token=token,
                month_range=month_range,
                month_serial=month_serial,
                scope="adset_pm",
                breakdowns=[
                    "publisher_platform",
                    "platform_position",
                    "device_platform",
                ],
            )
        )

    return sort_gitreport3(
        rows
    )


def build_gitreport3_scope(
    act_id,
    token,
    month_range,
    month_serial,
    scope,
    breakdowns,
):
    print(
        "[gitreport3] "
        f"Fetching default metrics: "
        f"{scope} / "
        f"{month_range['label']}"
    )

    # 通常指標・デフォルトアトリビューション
    default_rows = fetch_meta_insights(
        act_id=act_id,
        token=token,
        since=month_range["since"],
        until=month_range["until"],
        level="adset",
        fields=[
            "campaign_id",
            "campaign_name",
            "adset_id",
            "adset_name",
            "impressions",
            "inline_link_clicks",
            "spend",
            "clicks",
            "actions",
            "action_values",
        ],
        breakdowns=breakdowns,
        action_attribution_windows=[
            "default",
        ],
    )

    print(
        "[gitreport3] "
        f"Fetching attribution metrics: "
        f"{scope} / "
        f"{month_range['label']}"
    )

    # Incrementalityを含むアトリビューション別指標は
    # 通常指標と分けて取得し、Python側で結合。
    attr_rows = fetch_meta_insights(
        act_id=act_id,
        token=token,
        since=month_range["since"],
        until=month_range["until"],
        level="adset",
        fields=[
            "campaign_id",
            "campaign_name",
            "adset_id",
            "adset_name",
            "actions",
            "action_values",
        ],
        breakdowns=breakdowns,
        action_attribution_windows=
            ATTRIBUTION_WINDOWS,
    )

    merged = {}

    for item in default_rows:
        key = gitreport3_key(
            item,
            scope,
        )

        merged[key] = {
            "item":
                item,
            "default":
                extract_default_metrics(
                    item
                ),
            "attr":
                empty_attr_metrics(),
        }

    for item in attr_rows:
        key = gitreport3_key(
            item,
            scope,
        )

        if key not in merged:
            merged[key] = {
                "item":
                    item,
                "default":
                    empty_default_metrics(),
                "attr":
                    empty_attr_metrics(),
            }

        merged[key][
            "attr"
        ] = extract_attribution_metrics(
            item
        )

    output = []

    for data in merged.values():
        item = data["item"]
        default = data["default"]
        attr = data["attr"]

        output.append([
            "meta",
            scope,
            month_serial,
            "",  # blank day column
            item.get(
                "campaign_name",
                "",
            ),
            item.get(
                "adset_name",
                "",
            ),
            "",  # blank column
            item.get(
                "gender",
                "",
            ),
            item.get(
                "age",
                "",
            ),
            item.get(
                "publisher_platform",
                "",
            ),
            item.get(
                "platform_position",
                "",
            ),
            item.get(
                "device_platform",
                "",
            ),
            default[
                "impressions"
            ],
            default[
                "link_clicks"
            ],
            default[
                "spend"
            ],
            default[
                "purchase"
            ],
            default[
                "sales"
            ],
            default[
                "add_to_cart"
            ],
            default[
                "clicks_all"
            ],
            default[
                "post_reactions"
            ],
            default[
                "post_comments"
            ],
            default[
                "post_saves"
            ],
            default[
                "post_shares"
            ],
            *attribution_metric_values(
                attr
            ),
        ])

    return output


def gitreport3_key(
    item,
    scope,
):
    base = (
        str(
            item.get(
                "campaign_id",
                "",
            )
        ),
        str(
            item.get(
                "adset_id",
                "",
            )
        ),
    )

    if scope == "adset_gen_age":
        return base + (
            str(
                item.get(
                    "gender",
                    "",
                )
            ),
            str(
                item.get(
                    "age",
                    "",
                )
            ),
        )

    return base + (
        str(
            item.get(
                "publisher_platform",
                "",
            )
        ),
        str(
            item.get(
                "platform_position",
                "",
            )
        ),
        str(
            item.get(
                "device_platform",
                "",
            )
        ),
    )


def empty_default_metrics():
    return {
        "impressions": 0,
        "link_clicks": 0.0,
        "spend": 0.0,
        "purchase": 0.0,
        "sales": 0.0,
        "add_to_cart": 0.0,
        "clicks_all": 0.0,
        "post_reactions": 0.0,
        "post_comments": 0.0,
        "post_saves": 0.0,
        "post_shares": 0.0,
    }


def sort_gitreport3(
    rows,
):
    scope_order = {
        "adset_gen_age": 0,
        "adset_pm": 1,
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
        ),
    )


# =========================================================
# gitreport4
# campaign x audience_segment
# past 2 months
# impressions / spend x1.2 / cv
# =========================================================

def build_gitreport4_rows(
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
            "[gitreport4] "
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
                    "[gitreport4] "
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
                "[gitreport4] "
                f"breakdown='{breakdown}' "
                "returned no usable values"
            )

        if not chosen_breakdown:
            raise RuntimeError(
                "gitreport4 failed: none of "
                "the Audience Segment breakdown "
                "candidates returned usable data. "
                f"month={month_range['label']}; "
                "candidates="
                f"{AUDIENCE_SEGMENT_BREAKDOWN_CANDIDATES}"
            )

        print(
            "[gitreport4] "
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
        "A1",
        output,
        raw=True,
    )

    print(
        "Write success: "
        f"spreadsheet={spreadsheet.id}, "
        f"sheet={sheet_name}, "
        f"rows={len(rows)}"
    )


def write_gitreport1(
    spreadsheet,
    rows,
):
    header = [
        "month",
        "campaign_name",
        "reach",
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
        SHEET_GITREPORT2,
        header,
        rows,
    )


def write_gitreport3(
    spreadsheet,
    rows,
):
    header = [
        "media",
        "scope",
        "month",
        "",
        "campaign_name",
        "adset_name",
        "",
        "gender",
        "age",
        "platform",
        "placement",
        "device_platform",
        "impressions",
        "link_clicks",
        "spend",
        "purchase",
        "sales",
        "add_to_cart",
        "clicks_all",
        "post_reactions",
        "post_comments",
        "post_saves",
        "post_shares",
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
        SHEET_GITREPORT3,
        header,
        rows,
    )


def write_gitreport4(
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
        SHEET_GITREPORT4,
        header,
        rows,
    )


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
