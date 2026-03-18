from flask import Flask, render_template, request, redirect, url_for, session, Response, flash, abort, jsonify
import requests
import os
import json
import logging
import math
import base64
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

try:
    import psycopg2
except Exception:
    psycopg2 = None

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "replace-this-with-your-secure-secret-key")
app.logger.setLevel(logging.INFO)

START_ADDRESS = "서울특별시 종로구 율곡로2길 19"
RETURN_ADDRESS = "서울특별시 종로구 율곡로2길 19"

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "cpskqrhksfleks12#")
SETTINGS_FILE = "admin_settings.json"
GEOCODE_CACHE_FILE = "geocode_cache.json"
ROUTE_CACHE_FILE = "route_cache.json"

DEFAULT_TEAM_USERS = {"1조": []}
GUEST_TEAM_NAME = "게스트"
GUEST_USER_NAME = "게스트"
TMAP_DEFAULT_APP_KEY = os.getenv("TMAP_APP_KEY", "DBAKOdGMlm8X0TANyuGFI3GP7aMYWmb77v2JfnAA")
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
GOOGLE_VISION_API_KEY = (os.getenv("GOOGLE_VISION_API_KEY") or "").strip()

DEFAULT_SETTINGS = {
    "mail": {
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
        "smtp_user": "",
        "smtp_password": "",
        "mail_from": "",
        "default_recipient_email": "",
        "email_subject_template": "[경로결과] {team_no}({user_name}) - {trip_date}",
        "email_body_template": "경로 결과 PDF를 첨부합니다."
    },
    "api": {
        "client_id": "",
        "client_secret": "",
        "tmap_app_key": TMAP_DEFAULT_APP_KEY,
        "google_vision_api_key": ""
    },
    "user": {
        "start_address": START_ADDRESS,
        "return_address": RETURN_ADDRESS,
        "return_same_as_start": True,
        "team_users": DEFAULT_TEAM_USERS,
        "enable_guest_user": True
    },
    "restaurant": {
        "items": []
    },
    "parking": {
        "items": []
    },
    "admin": {
        "admin_password": ADMIN_PASSWORD
    }
}

DAY_START = 10 * 60
NO_LUNCH_IF_DONE_BY = 12 * 60
LUNCH_START_MIN = 11 * 60 + 30
LUNCH_START_MAX = 13 * 60 + 30
LUNCH_DURATION = 60
LUNCH_SKIP_IF_RETURN_BY = 13 * 60
LUNCH_SKIP_IF_DEPART_AFTER = 12 * 60 + 30
RETURN_LIMIT = 16 * 60 + 30

BEAM_WIDTH = 24
LOCAL_IMPROVE_ITER = 80
MAX_PARTIAL_CANDIDATES = 1200
APP_STATE_TABLE = "app_state"
ROUTE_CACHE_MEMORY = None
ROUTE_CACHE_DIRTY = False
USE_TRAFFIC_FOR_PLANNING = (os.getenv("USE_TRAFFIC_FOR_PLANNING", "0").strip() == "1")
PARKING_RESOLVED_CACHE_KEY = None
PARKING_RESOLVED_CACHE = []


def load_json_file(path, default_value):
    if not os.path.exists(path):
        return default_value
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default_value


def save_json_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def has_database_storage():
    if not DATABASE_URL:
        return False
    if "://" not in DATABASE_URL:
        app.logger.warning("DATABASE_URL format is invalid; falling back to file storage.")
        return False
    if psycopg2 is None:
        app.logger.warning("psycopg2 is unavailable; falling back to file storage.")
        return False
    return True


def _remove_query_param(url, param_name):
    parts = urlsplit(url)
    query_pairs = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != param_name]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query_pairs), parts.fragment))


def _database_url_candidates():
    if not has_database_storage():
        return []
    candidates = [DATABASE_URL]
    if "channel_binding=" in DATABASE_URL:
        candidates.append(_remove_query_param(DATABASE_URL, "channel_binding"))
    return candidates


def _connect_postgres():
    last_error = None
    for index, candidate in enumerate(_database_url_candidates()):
        try:
            conn = psycopg2.connect(candidate)
            conn.autocommit = True
            if index > 0:
                app.logger.warning("Connected to Postgres after removing unsupported DATABASE_URL options.")
            return conn
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error
    raise RuntimeError("Database storage is not configured.")


def _ensure_postgres_state_table():
    with _connect_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {APP_STATE_TABLE} (
                    state_key TEXT PRIMARY KEY,
                    state_value JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )


def load_persistent_json(state_key, file_path, default_value):
    if has_database_storage():
        try:
            _ensure_postgres_state_table()
            with _connect_postgres() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT state_value FROM {APP_STATE_TABLE} WHERE state_key = %s",
                        (state_key,),
                    )
                    row = cur.fetchone()
            if row:
                value = row[0]
                if isinstance(value, str):
                    return json.loads(value)
                return value

            file_value = load_json_file(file_path, None)
            if file_value is not None:
                save_persistent_json(state_key, file_path, file_value)
                return file_value
            return default_value
        except Exception:
            app.logger.exception("Failed to load '%s' from Postgres. Falling back to file storage.", state_key)
            return load_json_file(file_path, default_value)

    return load_json_file(file_path, default_value)


def save_persistent_json(state_key, file_path, data):
    if has_database_storage():
        try:
            _ensure_postgres_state_table()
            with _connect_postgres() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {APP_STATE_TABLE} (state_key, state_value, updated_at)
                        VALUES (%s, %s::jsonb, NOW())
                        ON CONFLICT (state_key)
                        DO UPDATE SET
                            state_value = EXCLUDED.state_value,
                            updated_at = NOW()
                        """,
                        (state_key, json.dumps(data, ensure_ascii=False)),
                    )
            return
        except Exception:
            app.logger.exception("Failed to save '%s' to Postgres. Falling back to file storage.", state_key)

    save_json_file(file_path, data)


def initialize_storage():
    if not has_database_storage():
        app.logger.warning("Using file storage because DATABASE_URL Postgres storage is unavailable.")
        return
    try:
        _ensure_postgres_state_table()
        app.logger.info("Postgres storage is active.")
    except Exception:
        app.logger.exception("Postgres storage initialization failed. Falling back to file storage.")


def normalize_team_users(team_users):
    normalized = {}

    if isinstance(team_users, dict):
        for team_name, users in team_users.items():
            key = str(team_name).strip()
            if not key or key == GUEST_TEAM_NAME:
                continue
            if isinstance(users, list):
                clean_users = []
                for x in users:
                    v = str(x).strip()
                    if not v or v == GUEST_USER_NAME:
                        continue
                    clean_users.append(v)
                normalized[key] = clean_users

    if not normalized:
        normalized = {"1조": []}

    return normalized


def is_guest_enabled(settings):
    return bool((settings or {}).get("user", {}).get("enable_guest_user", True))


def get_effective_team_users(settings=None):
    settings = settings or load_settings()
    base = normalize_team_users((settings.get("user") or {}).get("team_users", {}))
    if is_guest_enabled(settings):
        effective = {GUEST_TEAM_NAME: [GUEST_USER_NAME]}
        effective.update(base)
        return effective
    return base


def is_valid_team_user_selection(team_users, team_no, user_name):
    team_no = (team_no or "").strip()
    user_name = (user_name or "").strip()
    if not team_no or not user_name:
        return False
    users = team_users.get(team_no)
    return isinstance(users, list) and user_name in users


def deep_copy_default_settings():
    return json.loads(json.dumps(DEFAULT_SETTINGS, ensure_ascii=False))


def migrate_legacy_settings(data):
    merged = deep_copy_default_settings()

    if not isinstance(data, dict):
        return merged

    has_new_structure = any(k in data for k in ["mail", "api", "user", "restaurant", "parking", "admin"])
    if has_new_structure:
        for section in ["mail", "api", "user", "restaurant", "parking", "admin"]:
            if isinstance(data.get(section), dict):
                merged[section].update(data[section])
    else:
        merged["api"]["client_id"] = str(data.get("client_id", "") or "").strip()
        merged["api"]["client_secret"] = str(data.get("client_secret", "") or "").strip()
        merged["mail"]["default_recipient_email"] = str(data.get("default_recipient_email", "") or "").strip()
        merged["mail"]["email_subject_template"] = str(
            data.get("email_subject_template", DEFAULT_SETTINGS["mail"]["email_subject_template"]) or ""
        ).strip()
        merged["mail"]["email_body_template"] = str(
            data.get("email_body_template", DEFAULT_SETTINGS["mail"]["email_body_template"]) or ""
        ).strip()
        merged["user"]["team_users"] = data.get("team_users", DEFAULT_TEAM_USERS)
        merged["user"]["start_address"] = START_ADDRESS
        merged["user"]["return_address"] = RETURN_ADDRESS
        merged["user"]["return_same_as_start"] = True

    merged["user"]["team_users"] = normalize_team_users(merged["user"].get("team_users", {}))
    if "enable_guest_user" not in merged["user"]:
        merged["user"]["enable_guest_user"] = True

    if not merged["user"].get("start_address"):
        merged["user"]["start_address"] = START_ADDRESS
    if not merged["user"].get("return_address"):
        merged["user"]["return_address"] = merged["user"].get("start_address", START_ADDRESS)
    if "return_same_as_start" not in merged["user"]:
        merged["user"]["return_same_as_start"] = True
    if not merged["api"].get("tmap_app_key"):
        merged["api"]["tmap_app_key"] = TMAP_DEFAULT_APP_KEY
    if not merged["admin"].get("admin_password"):
        merged["admin"]["admin_password"] = ADMIN_PASSWORD
    restaurant_items = merged.get("restaurant", {}).get("items", [])
    normalized_restaurants = []
    if isinstance(restaurant_items, list):
        for item in restaurant_items:
            if not isinstance(item, dict):
                continue
            normalized_restaurants.append({
                "name": str(item.get("name", "") or "").strip(),
                "menu": str(item.get("menu", "") or "").strip(),
                "price": str(item.get("price", "") or "").strip(),
                "address": str(item.get("address", "") or "").strip(),
                "parking": str(item.get("parking", "") or "unknown").strip() if str(item.get("parking", "") or "").strip() in {"1", "0", "unknown"} else "unknown",
                "payment_card": bool(item.get("payment_card")),
                "payment_cash": bool(item.get("payment_cash")),
                "note": str(item.get("note", "") or "").strip(),
            })
    merged["restaurant"]["items"] = normalized_restaurants
    parking_items = merged.get("parking", {}).get("items", [])
    normalized_parking = []
    if isinstance(parking_items, list):
        for item in parking_items:
            if not isinstance(item, dict):
                continue
            normalized_parking.append({
                "name": str(item.get("name", "") or "").strip(),
                "address": str(item.get("address", "") or "").strip(),
            })
    merged["parking"]["items"] = normalized_parking
    if not merged["mail"].get("smtp_host"):
        merged["mail"]["smtp_host"] = "smtp.gmail.com"
    if not merged["mail"].get("smtp_port"):
        merged["mail"]["smtp_port"] = 587

    try:
        merged["mail"]["smtp_port"] = int(merged["mail"].get("smtp_port", 587))
    except Exception:
        merged["mail"]["smtp_port"] = 587

    merged["user"]["return_same_as_start"] = bool(merged["user"].get("return_same_as_start"))
    return merged


def load_settings():
    data = load_persistent_json("settings", SETTINGS_FILE, None)
    if data is None:
        settings = deep_copy_default_settings()
        save_settings(settings)
        return settings

    return migrate_legacy_settings(data)


def save_settings(data):
    merged = migrate_legacy_settings(data)
    save_persistent_json("settings", SETTINGS_FILE, merged)


def get_start_address():
    settings = load_settings()
    return (settings.get("user", {}).get("start_address") or START_ADDRESS).strip() or START_ADDRESS


def get_return_address():
    settings = load_settings()
    user = settings.get("user", {})
    start_address = (user.get("start_address") or START_ADDRESS).strip() or START_ADDRESS
    if user.get("return_same_as_start"):
        return start_address
    return (user.get("return_address") or start_address).strip() or start_address


def get_mail_config():
    settings = load_settings()
    mail = settings.get("mail", {})
    return {
        "smtp_host": (mail.get("smtp_host") or "smtp.gmail.com").strip() or "smtp.gmail.com",
        "smtp_port": int(mail.get("smtp_port") or 587),
        "smtp_user": (mail.get("smtp_user") or "").strip(),
        "smtp_password": (mail.get("smtp_password") or "").strip(),
        "mail_from": (mail.get("mail_from") or "").strip(),
    }


def get_tmap_app_key():
    settings = load_settings()
    return (settings.get("api", {}).get("tmap_app_key") or TMAP_DEFAULT_APP_KEY).strip()


def get_google_vision_api_key():
    settings = load_settings()
    configured = (settings.get("api", {}).get("google_vision_api_key") or "").strip()
    return configured or GOOGLE_VISION_API_KEY


def get_admin_password():
    settings = load_settings()
    return (settings.get("admin", {}).get("admin_password") or ADMIN_PASSWORD).strip()


def get_api_headers():
    settings = load_settings()
    api = settings.get("api", {})
    return {
        "X-NCP-APIGW-API-KEY-ID": (api.get("client_id") or "").strip(),
        "X-NCP-APIGW-API-KEY": (api.get("client_secret") or "").strip()
    }


def is_mobile_request():
    ua = (request.headers.get("User-Agent") or "").lower()
    keywords = ["iphone", "android", "ipad", "mobile", "windows phone"]
    return any(k in ua for k in keywords)


def minutes_to_str(m: int) -> str:
    m = int(m)
    return f"{m // 60:02d}:{m % 60:02d}"


def str_to_minutes(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    if ':' not in s:
        try:
            return int(float(s))
        except Exception:
            return None
    parts = s.split(':')
    if len(parts) < 2:
        return None
    try:
        hh = int(parts[0])
        mm = int(parts[1])
    except Exception:
        return None
    return hh * 60 + mm


def parse_appointment_minute(hour_str: str, minute_str: str):
    hour_str = (hour_str or "").strip()
    minute_str = (minute_str or "").strip()
    if hour_str == "" or minute_str == "":
        return None
    try:
        hh = int(hour_str)
        mm = int(minute_str)
    except Exception:
        return None
    return hh * 60 + mm


def get_geocode_cache():
    return load_persistent_json("geocode_cache", GEOCODE_CACHE_FILE, {})


def save_geocode_cache(cache):
    save_persistent_json("geocode_cache", GEOCODE_CACHE_FILE, cache)


def get_route_cache():
    global ROUTE_CACHE_MEMORY
    if ROUTE_CACHE_MEMORY is None:
        ROUTE_CACHE_MEMORY = load_persistent_json("route_cache", ROUTE_CACHE_FILE, {})
    return ROUTE_CACHE_MEMORY


def save_route_cache(cache=None, force=False):
    global ROUTE_CACHE_MEMORY, ROUTE_CACHE_DIRTY
    if cache is not None:
        ROUTE_CACHE_MEMORY = cache
        ROUTE_CACHE_DIRTY = True
    if force or ROUTE_CACHE_DIRTY:
        save_persistent_json("route_cache", ROUTE_CACHE_FILE, ROUTE_CACHE_MEMORY or {})
        ROUTE_CACHE_DIRTY = False


def cache_route_result(route_cache, cache_key, distance_m, duration_min, prediction_time):
    global ROUTE_CACHE_DIRTY
    route_cache[cache_key] = {
        "distance_m": int(distance_m),
        "duration_min": int(duration_min),
        "prediction_time": prediction_time,
        "updated_at": datetime.now().isoformat(),
    }
    ROUTE_CACHE_DIRTY = True


def extract_building_name(geocode_item):
    if not isinstance(geocode_item, dict):
        return ""

    for element in geocode_item.get("addressElements") or []:
        types = element.get("types") or element.get("type") or []
        if isinstance(types, str):
            types = [types]
        if "BUILDING_NAME" in types:
            name = str(element.get("longName") or element.get("shortName") or "").strip()
            if name:
                return name
    return ""


def format_display_address(address, building_name=""):
    address = (address or "").strip()
    building_name = (building_name or "").strip()
    if not address or not building_name:
        return address
    if building_name in address:
        return address
    return f"{address} ({building_name})"


def geocode_with_meta(address: str):
    query = (address or "").strip()
    coord, err = geocode(query)
    if not coord:
        return None, None, err

    cache = get_geocode_cache()
    item = cache.get(query) or {}
    building_name = (item.get("building_name") or "").strip()
    if building_name:
        return coord, {"building_name": building_name, "display_address": format_display_address(query, building_name)}, None

    url = "https://maps.apigw.ntruss.com/map-geocode/v2/geocode"
    headers = get_api_headers()
    if headers["X-NCP-APIGW-API-KEY-ID"] and headers["X-NCP-APIGW-API-KEY"]:
        try:
            resp = requests.get(url, headers=headers, params={"query": query}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                addresses = data.get("addresses") or []
                if addresses:
                    building_name = extract_building_name(addresses[0])
                    if building_name:
                        item.update({
                            "x": coord[0],
                            "y": coord[1],
                            "building_name": building_name,
                            "updated_at": datetime.now().isoformat(),
                        })
                        cache[query] = item
                        save_geocode_cache(cache)
        except Exception:
            pass

    return coord, {"building_name": building_name, "display_address": format_display_address(query, building_name)}, None


def geocode(address: str):
    url = "https://maps.apigw.ntruss.com/map-geocode/v2/geocode"
    query = (address or "").strip()
    params = {"query": query}
    headers = get_api_headers()

    if not query:
        return None, "주소가 비어 있습니다."

    if not headers["X-NCP-APIGW-API-KEY-ID"] or not headers["X-NCP-APIGW-API-KEY"]:
        return None, "관리자 설정에서 API 키가 입력되지 않았습니다."

    cache = get_geocode_cache()
    if query in cache:
        item = cache[query]
        try:
            return (float(item["x"]), float(item["y"])), None
        except Exception:
            pass

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
    except requests.RequestException as e:
        return None, f"지오코딩 요청 실패: {e}"

    if resp.status_code != 200:
        try:
            data = resp.json()
            msg = data.get("errorMessage") or data.get("message") or resp.text
        except Exception:
            msg = resp.text
        return None, f"지오코딩 HTTP {resp.status_code}: {msg}"

    try:
        data = resp.json()
    except Exception:
        return None, "지오코딩 응답이 JSON 형식이 아닙니다."

    addresses = data.get("addresses") or []
    if not addresses:
        return None, f"지오코딩 결과 없음: {query}"

    try:
        x = float(addresses[0]["x"])
        y = float(addresses[0]["y"])
        cache[query] = {"x": x, "y": y, "updated_at": datetime.now().isoformat()}
        save_geocode_cache(cache)
        return (x, y), None
    except Exception:
        return None, f"지오코딩 결과 파싱 실패: {query}"


def normalize_ocr_line(text):
    return re.sub(r"\s+", " ", str(text or "").replace("\r", "\n")).strip()


def extract_ocr_address_candidates(text):
    normalized = normalize_ocr_line(text)
    if not normalized:
        return []

    source_lines = [
        normalize_ocr_line(line)
        for line in str(text or "").splitlines()
        if normalize_ocr_line(line)
    ]
    source = list(dict.fromkeys([normalized] + source_lines))
    patterns = [
        re.compile(r"(?:서울|부산|대구|인천|광주|대전|울산|세종|경기|강원|충북|충남|전북|전남|경북|경남|제주)[^,\n]{4,80}?(?:로|길|대로)\s*\d[\d-]*"),
        re.compile(r"(?:[가-힣]+\s+)?[가-힣]+(?:시|도)?\s*[가-힣]+(?:구|군|시)\s+[가-힣0-9]+(?:로|길|대로)\s*\d[\d-]*"),
        re.compile(r"[가-힣0-9]+(?:로|길|대로)\s*\d[\d-]*(?:\s*[가-힣A-Za-z0-9-]+){0,3}"),
    ]

    candidates = []
    for item in source:
        for pattern in patterns:
            for match in pattern.findall(item):
                candidate = normalize_ocr_line(match)
                if len(candidate) >= 6:
                    candidates.append(candidate)
    return list(dict.fromkeys(candidates))


def extract_ocr_address_like_lines(text):
    lines = []
    for line in str(text or "").splitlines():
        normalized = normalize_ocr_line(line)
        if not normalized:
            continue
        if not re.search(r"[가-힣]", normalized) or not re.search(r"\d", normalized):
            continue
        if re.search(r"(?:로|길|대로)\s*\d", normalized) or re.fullmatch(r"[가-힣0-9\s-]{6,}", normalized):
            lines.append(normalized)
    return list(dict.fromkeys(lines))


def call_google_vision_ocr(image_bytes):
    api_key = get_google_vision_api_key()
    if not api_key:
        return None, "GOOGLE_VISION_API_KEY가 설정되지 않았습니다."

    encoded = base64.b64encode(image_bytes).decode("ascii")
    payload = {
        "requests": [{
            "image": {"content": encoded},
            "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
            "imageContext": {"languageHints": ["ko", "en"]},
        }]
    }

    try:
        resp = requests.post(
            "https://vision.googleapis.com/v1/images:annotate",
            params={"key": api_key},
            json=payload,
            timeout=45,
        )
    except requests.RequestException as e:
        return None, f"Google Vision OCR 요청 실패: {e}"

    if resp.status_code != 200:
        try:
            data = resp.json()
            message = (
                (data.get("error") or {}).get("message")
                or (data.get("error") or {}).get("status")
                or resp.text
            )
        except Exception:
            message = resp.text
        return None, f"Google Vision OCR HTTP {resp.status_code}: {message}"

    try:
        data = resp.json()
    except Exception:
        return None, "Google Vision OCR 응답이 JSON 형식이 아닙니다."

    response = ((data.get("responses") or [{}])[0]) if isinstance(data, dict) else {}
    if response.get("error"):
        return None, (response.get("error") or {}).get("message") or "Google Vision OCR 응답 오류"

    text = (
        ((response.get("fullTextAnnotation") or {}).get("text"))
        or (((response.get("textAnnotations") or [{}])[0]).get("description"))
        or ""
    ).strip()
    return text, None


def extract_valid_ocr_addresses(text):
    candidates = extract_ocr_address_candidates(text)
    if not candidates:
        candidates = extract_ocr_address_like_lines(text)

    validated = []
    seen = set()
    for candidate in candidates[:20]:
        coord, meta, _ = geocode_with_meta(candidate)
        if not coord:
            continue
        display_address = ((meta or {}).get("display_address") or candidate).strip()
        if display_address and display_address not in seen:
            seen.add(display_address)
            validated.append(display_address)

    if validated:
        return validated

    fallback = []
    for candidate in candidates[:20]:
        if candidate not in seen:
            seen.add(candidate)
            fallback.append(candidate)
    return fallback


def estimate_matrix_leg(start, goal):
    lon1, lat1 = start
    lon2, lat2 = goal
    rad = math.pi / 180.0
    d_lat = (lat2 - lat1) * rad
    d_lon = (lon2 - lon1) * rad
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(lat1 * rad) * math.cos(lat2 * rad) * (math.sin(d_lon / 2) ** 2)
    )
    surface_m = 6371000 * 2 * math.asin(min(1, math.sqrt(a)))
    road_m = max(500, int(surface_m * 1.28))
    duration_min = max(3, int(math.ceil((road_m / 1000) / 28 * 60)))
    return road_m, duration_min


def straight_distance_m(start, goal):
    lon1, lat1 = start
    lon2, lat2 = goal
    rad = math.pi / 180.0
    d_lat = (lat2 - lat1) * rad
    d_lon = (lon2 - lon1) * rad
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(lat1 * rad) * math.cos(lat2 * rad) * (math.sin(d_lon / 2) ** 2)
    )
    return 6371000 * 2 * math.asin(min(1, math.sqrt(a)))


def estimate_walk_minutes(distance_m):
    if distance_m <= 0:
        return 1
    # average walking speed: 4.2km/h
    return max(1, int(math.ceil((distance_m / 1000.0) / 4.2 * 60)))


def estimate_parking_drive_walk_minutes(direct_distance_m):
    direct_distance_m = max(0.0, float(direct_distance_m or 0.0))
    if direct_distance_m <= 60:
        return 1, 1

    # Parking-to-visit legs are short local moves. Use lighter detour factors than full route estimates.
    drive_distance_m = max(direct_distance_m * 1.12, direct_distance_m + 20)
    walk_distance_m = max(direct_distance_m * 1.06, direct_distance_m)

    drive_min = max(1, int(math.ceil((drive_distance_m / 1000.0) / 18.0 * 60)))
    walk_min = max(1, int(math.ceil((walk_distance_m / 1000.0) / 4.5 * 60)))

    # For very short hops, avoid unrealistic "driving much slower than walking" output.
    if direct_distance_m < 300 and drive_min > walk_min:
        drive_min = max(1, walk_min)
    return drive_min, walk_min


def enrich_route_with_nearby_parking(route_view, visits, visit_coords, parking_items, radius_m=1000, max_items=5):
    global PARKING_RESOLVED_CACHE_KEY, PARKING_RESOLVED_CACHE
    if not isinstance(route_view, list):
        return route_view
    radius_m = max(100, int(radius_m or 1000))
    max_items = max(1, int(max_items or 5))

    # Always expose the key for template safety.
    for item in route_view:
        if isinstance(item, dict) and item.get("type") == "visit":
            item["nearby_parkings"] = []

    visit_coord_map = {}
    for idx, visit in enumerate(visits):
        if idx >= len(visit_coords):
            continue
        visit_id = visit.get("visit_id")
        if visit_id is None:
            continue
        visit_coord_map[visit_id] = visit_coords[idx]

    parking_pairs = []
    for item in (parking_items or [])[:200]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        address = str(item.get("address") or "").strip()
        if name and address:
            parking_pairs.append((name, address))
    cache_key = tuple(parking_pairs)

    if cache_key == PARKING_RESOLVED_CACHE_KEY:
        resolved_parking = PARKING_RESOLVED_CACHE
    else:
        resolved_parking = []
        for name, address in parking_pairs:
            try:
                coord, _, _ = geocode_with_meta(address)
            except Exception:
                continue
            if not coord:
                continue
            resolved_parking.append({
                "name": name,
                "address": address,
                "coord": coord,
            })
        PARKING_RESOLVED_CACHE_KEY = cache_key
        PARKING_RESOLVED_CACHE = resolved_parking

    for item in route_view:
        if not isinstance(item, dict) or item.get("type") != "visit":
            continue

        visit_id = item.get("visit_id")
        visit_coord = visit_coord_map.get(visit_id)
        if not visit_coord:
            item["nearby_parkings"] = []
            continue

        nearby = []
        for parking in resolved_parking:
            try:
                direct_m = straight_distance_m(visit_coord, parking["coord"])
            except Exception:
                continue
            if direct_m > radius_m:
                continue

            drive_min, walk_min = estimate_parking_drive_walk_minutes(direct_m)
            nearby.append({
                "name": parking["name"],
                "address": parking["address"],
                "drive_min": int(drive_min),
                "walk_min": int(walk_min),
                "distance_m": int(round(direct_m)),
            })

        nearby.sort(key=lambda x: (x["distance_m"], x["drive_min"], x["name"]))
        item["nearby_parkings"] = nearby[:max_items]

    return route_view


def fallback_route_info(start, goal):
    return estimate_matrix_leg(start, goal)


def build_prediction_time(trip_date, departure_min, bucket_minutes=10):
    trip_date = (trip_date or "").strip()
    if not trip_date:
        return None
    try:
        base = datetime.strptime(trip_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("Asia/Seoul"))
    except Exception:
        return None

    rounded_minute = max(0, int(departure_min // bucket_minutes) * bucket_minutes)
    dt = base.replace(hour=0, minute=0, second=0, microsecond=0)
    dt = dt.replace(hour=rounded_minute // 60, minute=rounded_minute % 60)
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


def get_route_info(start, goal, prediction_time=None, route_cache=None):
    start_key = f"{round(start[0], 6)},{round(start[1], 6)}"
    goal_key = f"{round(goal[0], 6)},{round(goal[1], 6)}"
    time_key = prediction_time or "realtime"
    cache_key = f"{start_key}|{goal_key}|{time_key}"

    route_cache = route_cache if route_cache is not None else get_route_cache()
    if cache_key in route_cache:
        item = route_cache[cache_key]
        return int(item["distance_m"]), int(item["duration_min"])

    tmap_app_key = get_tmap_app_key()
    if tmap_app_key and prediction_time:
        url = "https://apis.openapi.sk.com/tmap/routes/prediction?version=1&format=json"
        headers = {
            "appKey": tmap_app_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "routesInfo": {
                "departure": {
                    "name": "출발지",
                    "lon": str(start[0]),
                    "lat": str(start[1]),
                    "type": "s",
                },
                "destination": {
                    "name": "도착지",
                    "lon": str(goal[0]),
                    "lat": str(goal[1]),
                    "type": "e",
                },
                "predictionType": "departure",
                "predictionTime": prediction_time,
                "searchOption": "00",
                "trafficInfo": "Y",
            }
        }

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                features = data.get("features") or []
                properties = (features[0] or {}).get("properties") if features else {}
                if properties:
                    distance_m = int(properties.get("totalDistance", 99999999))
                    total_seconds = int(properties.get("totalTime", 999999))
                    duration_min = max(1, int(math.ceil(total_seconds / 60)))
                    cache_route_result(route_cache, cache_key, distance_m, duration_min, prediction_time)
                    return distance_m, duration_min
        except requests.RequestException:
            pass
        except Exception:
            pass

    url = "https://maps.apigw.ntruss.com/map-direction/v1/driving"
    headers = get_api_headers()
    if not headers["X-NCP-APIGW-API-KEY-ID"] or not headers["X-NCP-APIGW-API-KEY"]:
        return fallback_route_info(start, goal)

    params = {
        "start": f"{start[0]},{start[1]}",
        "goal": f"{goal[0]},{goal[1]}",
        "option": "trafast",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
    except requests.RequestException:
        return fallback_route_info(start, goal)

    if resp.status_code != 200:
        return fallback_route_info(start, goal)

    try:
        data = resp.json()
        summary = data["route"]["trafast"][0]["summary"]
        distance_m = int(summary["distance"])
        duration_min = int(summary["duration"]) // 60000
        cache_route_result(route_cache, cache_key, distance_m, duration_min, prediction_time)
        return distance_m, duration_min
    except Exception:
        return fallback_route_info(start, goal)


def get_trip_meta():
    return {
        "user_name": session.get("user_name", ""),
        "team_no": session.get("team_no", ""),
        "trip_date": session.get("trip_date", "")
    }


def route_distance_with_return(order, dist_matrix):
    if not order:
        return 0
    total = dist_matrix[0][order[0]]
    for i in range(len(order) - 1):
        total += dist_matrix[order[i]][order[i + 1]]
    total += dist_matrix[order[-1]][0]
    return total


def nearest_neighbor_seed(visits, dist_matrix):
    n = len(visits)
    remaining = set(range(1, n + 1))
    current = 0
    order = []

    while remaining:
        nxt = min(remaining, key=lambda x: (dist_matrix[current][x], x))
        order.append(nxt)
        remaining.remove(nxt)
        current = nxt

    return order


def greedy_appointment_seed(visits, dist_matrix, time_matrix):
    remaining = set(range(1, len(visits) + 1))
    order = []
    current = 0
    current_time = DAY_START

    while remaining:
        scored = []
        for node in remaining:
            travel = time_matrix[current][node]
            arrival = current_time + travel
            visit = visits[node - 1]

            penalty = 0
            if visit["has_appointment"]:
                target = visit["appointment_minute"]
                if arrival > target:
                    penalty += (arrival - target) * 1000

            score = dist_matrix[current][node] + penalty
            scored.append((score, node))

        scored.sort()
        nxt = scored[0][1]
        visit = visits[nxt - 1]
        arrival = current_time + time_matrix[current][nxt]

        current_time = arrival + visit["service_time"]
        order.append(nxt)
        remaining.remove(nxt)
        current = nxt

    return order


def estimate_order_metrics(path, visits, dist_matrix, time_matrix):
    if not path:
        return {
            "appointment_violations": 0,
            "appointment_late_total": 0,
            "finish_time": DAY_START,
            "total_distance": 0,
            "locality_penalty": 0,
            "wait_total": 0,
        }

    current_time = DAY_START
    last = 0
    total_dist = 0
    locality_penalty = 0
    appointment_violations = 0
    appointment_late_total = 0
    wait_total = 0

    for idx, node in enumerate(path):
        travel = time_matrix[last][node]
        total_dist += dist_matrix[last][node]
        arrival = current_time + travel
        visit = visits[node - 1]

        if visit["has_appointment"]:
            target = visit["appointment_minute"]
            if arrival > target:
                appointment_violations += 1
                appointment_late_total += arrival - target

        current_time = arrival + visit["service_time"]

        if idx >= 1:
            locality_penalty += int(dist_matrix[path[idx - 1]][node] * 0.03)

        last = node

    finish_time = current_time + time_matrix[last][0]
    total_dist += dist_matrix[last][0]

    return {
        "appointment_violations": appointment_violations,
        "appointment_late_total": appointment_late_total,
        "finish_time": finish_time,
        "total_distance": total_dist,
        "locality_penalty": locality_penalty,
        "wait_total": wait_total,
    }


def partial_path_score(path, visits, dist_matrix, time_matrix):
    metrics = estimate_order_metrics(path, visits, dist_matrix, time_matrix)
    return (
        metrics["appointment_violations"] * 10**9
        + metrics["appointment_late_total"] * 10**6
        + metrics["finish_time"] * 10**3
        + metrics["total_distance"]
        + metrics["locality_penalty"]
        + metrics["wait_total"] * 5
    )


def beam_search_route(visits, dist_matrix, time_matrix):
    n = len(visits)
    if n == 0:
        return []

    seeds = []
    nn = nearest_neighbor_seed(visits, dist_matrix)
    ga = greedy_appointment_seed(visits, dist_matrix, time_matrix)
    seeds.append(nn)
    if ga != nn:
        seeds.append(ga)

    appointment_nodes = [i for i, v in enumerate(visits, start=1) if v["has_appointment"]]
    if appointment_nodes:
        earliest_appt_node = min(appointment_nodes, key=lambda x: visits[x - 1]["appointment_minute"])
        remaining = [x for x in range(1, n + 1) if x != earliest_appt_node]
        remaining.sort(key=lambda x: dist_matrix[earliest_appt_node][x])
        seeds.append([earliest_appt_node] + remaining)

    best_seed = min(seeds, key=lambda order: partial_path_score(order, visits, dist_matrix, time_matrix))

    first_candidates = best_seed[:min(BEAM_WIDTH, len(best_seed))]
    init_beams = []
    for node in first_candidates:
        init_beams.append(([node], partial_path_score([node], visits, dist_matrix, time_matrix)))
    init_beams.sort(key=lambda x: x[1])
    beams = init_beams[:BEAM_WIDTH]

    for _step in range(1, n):
        candidates = []

        for path, _score in beams:
            visited = set(path)
            last = path[-1]

            remaining = [x for x in range(1, n + 1) if x not in visited]
            remaining.sort(key=lambda x: dist_matrix[last][x])
            remaining = remaining[:min(len(remaining), 6)]

            for node in range(1, n + 1):
                if node not in visited and visits[node - 1]["has_appointment"] and node not in remaining:
                    remaining.append(node)

            for nxt in remaining:
                new_path = path + [nxt]
                score = partial_path_score(new_path, visits, dist_matrix, time_matrix)
                candidates.append((new_path, score))

        candidates.sort(key=lambda x: x[1])
        beams = candidates[:BEAM_WIDTH]

        if len(candidates) > MAX_PARTIAL_CANDIDATES:
            beams = beams[:BEAM_WIDTH]

    return min(beams, key=lambda x: x[1])[0]


def two_opt(order, dist_matrix, time_matrix, visits, max_iter=LOCAL_IMPROVE_ITER):
    if len(order) <= 3:
        return order[:]

    def score(candidate):
        return partial_path_score(candidate, visits, dist_matrix, time_matrix) + route_distance_with_return(candidate, dist_matrix)

    best = order[:]
    best_score = score(best)
    improved = True
    iter_count = 0

    while improved and iter_count < max_iter:
        improved = False
        iter_count += 1

        for i in range(len(best) - 1):
            for j in range(i + 2, len(best) + 1):
                if j - i <= 1:
                    continue
                candidate = best[:i] + list(reversed(best[i:j])) + best[j:]
                cand_score = score(candidate)

                if cand_score < best_score:
                    best = candidate
                    best_score = cand_score
                    improved = True
                    break
            if improved:
                break

    return best


def relocate_improve(order, dist_matrix, time_matrix, visits, max_iter=LOCAL_IMPROVE_ITER):
    if len(order) <= 2:
        return order[:]

    def score(candidate):
        return partial_path_score(candidate, visits, dist_matrix, time_matrix) + route_distance_with_return(candidate, dist_matrix)

    best = order[:]
    best_score = score(best)
    improved = True
    iter_count = 0

    while improved and iter_count < max_iter:
        improved = False
        iter_count += 1

        for i in range(len(best)):
            node = best[i]
            reduced = best[:i] + best[i + 1:]

            for j in range(len(reduced) + 1):
                candidate = reduced[:j] + [node] + reduced[j:]
                cand_score = score(candidate)

                if cand_score < best_score:
                    best = candidate
                    best_score = cand_score
                    improved = True
                    break
            if improved:
                break

    return best


def or_opt_improve(order, dist_matrix, time_matrix, visits, max_iter=LOCAL_IMPROVE_ITER):
    if len(order) <= 3:
        return order[:]

    def score(candidate):
        return partial_path_score(candidate, visits, dist_matrix, time_matrix) + route_distance_with_return(candidate, dist_matrix)

    best = order[:]
    best_score = score(best)
    improved = True
    iter_count = 0

    while improved and iter_count < max_iter:
        improved = False
        iter_count += 1

        for seg_len in (1, 2, 3):
            if seg_len >= len(best):
                continue
            for i in range(len(best) - seg_len + 1):
                segment = best[i:i + seg_len]
                reduced = best[:i] + best[i + seg_len:]
                for j in range(len(reduced) + 1):
                    if j == i:
                        continue
                    candidate = reduced[:j] + segment + reduced[j:]
                    cand_score = score(candidate)
                    if cand_score < best_score:
                        best = candidate
                        best_score = cand_score
                        improved = True
                        break
                if improved:
                    break
            if improved:
                break

    return best


def optimize_route(visits, dist_matrix, time_matrix):
    order = beam_search_route(visits, dist_matrix, time_matrix)
    order = two_opt(order, dist_matrix, time_matrix, visits)
    order = relocate_improve(order, dist_matrix, time_matrix, visits)
    order = or_opt_improve(order, dist_matrix, time_matrix, visits)
    order = two_opt(order, dist_matrix, time_matrix, visits)
    return order


def append_wait_block(route_view, start_min, end_min, name="대기"):
    if end_min <= start_min:
        return 0

    if route_view and route_view[-1].get("type") == "wait":
        prev = route_view[-1]
        prev_end_h, prev_end_m = map(int, prev["end_time"].split(":"))
        prev_end = prev_end_h * 60 + prev_end_m

        prev_start_h, prev_start_m = map(int, prev["arrival"].split(":"))
        prev_start = prev_start_h * 60 + prev_start_m

        if start_min <= prev_end:
            new_end = max(prev_end, end_min)
            prev["end_time"] = minutes_to_str(new_end)
            prev["service_time"] = new_end - prev_start
            return 0

    route_view.append({
        "type": "wait",
        "label": "W",
        "name": name,
        "address": "",
        "arrival": minutes_to_str(start_min),
        "end_time": minutes_to_str(end_min),
        "service_time": end_min - start_min,
        "travel_km": None,
        "travel_min": None,
    })
    return 1


def append_lunch_block(route_view, start_min):
    route_view.append({
        "type": "lunch",
        "label": "L",
        "name": "점심",
        "address": "",
        "arrival": minutes_to_str(start_min),
        "end_time": minutes_to_str(start_min + LUNCH_DURATION),
        "service_time": LUNCH_DURATION,
        "travel_km": None,
        "travel_min": None,
    })


def add_visit_block(route_view, visit_no, visit, arrival_min, travel_m, travel_min):
    route_view.append({
        "type": "visit",
        "label": str(visit_no),
        "visit_id": visit.get("visit_id"),
        "name": visit["name"],
        "address": visit.get("display_address") or visit["address"],
        "arrival": minutes_to_str(arrival_min),
        "end_time": minutes_to_str(arrival_min + visit["service_time"]),
        "service_time": visit["service_time"],
        "travel_km": round(travel_m / 1000, 1),
        "travel_min": int(travel_min),
        "appointment_time": minutes_to_str(visit["appointment_minute"]) if visit["has_appointment"] else None,
    })


def add_return_block(route_view, arrival_min, travel_m, travel_min, return_address=None):
    route_view.append({
        "type": "return",
        "label": "R",
        "name": "복귀",
        "address": return_address or get_return_address(),
        "arrival": minutes_to_str(arrival_min),
        "end_time": minutes_to_str(arrival_min),
        "service_time": 0,
        "travel_km": round(travel_m / 1000, 1),
        "travel_min": int(travel_min),
    })


def clone_route(route_view):
    return [dict(item) for item in route_view]


def count_intra_wait_blocks(route_view):
    penalty = 0
    for i in range(1, len(route_view) - 1):
        prev = route_view[i - 1]
        cur = route_view[i]
        nxt = route_view[i + 1]
        if prev["type"] == "visit" and cur["type"] == "wait" and nxt["type"] == "visit":
            penalty += 1
    return penalty


def maybe_insert_lunch(route_view, current_time, lunch_used, wait_label):
    options = []
    if lunch_used:
        return options

    if LUNCH_START_MIN <= current_time <= LUNCH_START_MAX:
        new_route = clone_route(route_view)
        append_lunch_block(new_route, current_time)
        options.append({
            "time_after_pre": current_time + LUNCH_DURATION,
            "lunch_used": True,
            "route": new_route,
            "wait_count": 0,
            "wait_total": 0
        })

    if current_time < LUNCH_START_MIN:
        new_route = clone_route(route_view)
        wc = append_wait_block(new_route, current_time, LUNCH_START_MIN, wait_label)
        append_lunch_block(new_route, LUNCH_START_MIN)
        options.append({
            "time_after_pre": LUNCH_START_MIN + LUNCH_DURATION,
            "lunch_used": True,
            "route": new_route,
            "wait_count": wc,
            "wait_total": LUNCH_START_MIN - current_time
        })

    options.sort(key=lambda x: (x["wait_total"], x["wait_count"], 0 if x["lunch_used"] else 1))
    return options


def best_depart_with_lunch(route_view, current_time, depart_time, lunch_used, wait_label):
    if depart_time < current_time:
        return [{
            "time_after_pre": current_time,
            "lunch_used": lunch_used,
            "route": clone_route(route_view),
            "wait_count": 0,
            "wait_total": 0
        }]

    candidates = []

    plain_route = clone_route(route_view)
    wc = append_wait_block(plain_route, current_time, depart_time, wait_label)
    candidates.append({
        "time_after_pre": depart_time,
        "lunch_used": lunch_used,
        "route": plain_route,
        "wait_count": wc,
        "wait_total": depart_time - current_time
    })

    if lunch_used:
        candidates.sort(key=lambda x: (x["wait_total"], x["wait_count"]))
        return candidates

    slack = depart_time - current_time

    if slack >= LUNCH_DURATION:
        lunch_start_a = max(current_time, LUNCH_START_MIN)
        if lunch_start_a <= LUNCH_START_MAX and lunch_start_a + LUNCH_DURATION <= depart_time:
            route_a = clone_route(route_view)
            wait_count_a = 0
            wait_total_a = 0

            if lunch_start_a > current_time:
                wait_count_a += append_wait_block(route_a, current_time, lunch_start_a, wait_label)
                wait_total_a += lunch_start_a - current_time

            append_lunch_block(route_a, lunch_start_a)
            lunch_end_a = lunch_start_a + LUNCH_DURATION

            if depart_time > lunch_end_a:
                wait_count_a += append_wait_block(route_a, lunch_end_a, depart_time, wait_label)
                wait_total_a += depart_time - lunch_end_a

            if lunch_start_a == current_time:
                candidates.append({
                    "time_after_pre": depart_time,
                    "lunch_used": True,
                    "route": route_a,
                    "wait_count": wait_count_a,
                    "wait_total": wait_total_a
                })

        lunch_start_b = depart_time - LUNCH_DURATION
        if LUNCH_START_MIN <= lunch_start_b <= LUNCH_START_MAX and lunch_start_b >= current_time:
            route_b = clone_route(route_view)
            wait_count_b = 0
            wait_total_b = 0

            if lunch_start_b > current_time:
                wait_count_b += append_wait_block(route_b, current_time, lunch_start_b, wait_label)
                wait_total_b += lunch_start_b - current_time

            append_lunch_block(route_b, lunch_start_b)

            candidates.append({
                "time_after_pre": depart_time,
                "lunch_used": True,
                "route": route_b,
                "wait_count": wait_count_b,
                "wait_total": wait_total_b
            })

    candidates.sort(key=lambda x: (x["wait_total"], x["wait_count"], 0 if x["lunch_used"] else 1))
    return candidates


def compress_route_view(route_view):
    if not route_view:
        return route_view

    compressed = []
    for item in route_view:
        if item.get("type") != "wait":
            compressed.append(dict(item))
            continue

        if compressed and compressed[-1].get("type") == "wait":
            prev = compressed[-1]

            prev_start_h, prev_start_m = map(int, prev["arrival"].split(":"))
            prev_end_h, prev_end_m = map(int, prev["end_time"].split(":"))
            cur_start_h, cur_start_m = map(int, item["arrival"].split(":"))
            cur_end_h, cur_end_m = map(int, item["end_time"].split(":"))

            prev_start = prev_start_h * 60 + prev_start_m
            prev_end = prev_end_h * 60 + prev_end_m
            cur_start = cur_start_h * 60 + cur_start_m
            cur_end = cur_end_h * 60 + cur_end_m

            if cur_start <= prev_end:
                new_end = max(prev_end, cur_end)
                prev["end_time"] = minutes_to_str(new_end)
                prev["service_time"] = new_end - prev_start
            else:
                compressed.append(dict(item))
        else:
            compressed.append(dict(item))

    return compressed


def normalize_pre_lunch_wait(route_view):
    if not route_view:
        return route_view

    normalized = [dict(item) for item in route_view]
    start_idx = next((i for i, item in enumerate(normalized) if item.get("type") == "start"), None)
    if start_idx is None:
        return normalized

    movable_wait = 0
    remove_indexes = []

    for idx in range(1, len(normalized) - 1):
        item = normalized[idx]
        if item.get("type") != "wait":
            continue
        prev_item = normalized[idx - 1]
        next_item = normalized[idx + 1]
        wait_start = str_to_minutes(item.get("arrival"))
        wait_end = str_to_minutes(item.get("end_time"))
        if wait_start >= LUNCH_START_MAX:
            continue
        if next_item.get("type") == "visit" and next_item.get("appointment_time"):
            continue
        if prev_item.get("type") in ("visit", "wait") and next_item.get("type") in ("visit", "lunch", "return"):
            movable_wait += max(0, wait_end - wait_start)
            remove_indexes.append(idx)

    if not movable_wait:
        return normalized

    normalized = [item for i, item in enumerate(normalized) if i not in remove_indexes]
    start_item = normalized[start_idx]
    start_time = str_to_minutes(start_item.get("arrival"))
    insert_wait = {
        "type": "wait",
        "label": "W",
        "name": "출발지 대기",
        "address": "",
        "arrival": minutes_to_str(start_time),
        "end_time": minutes_to_str(start_time + movable_wait),
        "service_time": movable_wait,
        "travel_km": None,
        "travel_min": None,
    }

    normalized.insert(start_idx + 1, insert_wait)
    return compress_route_view(normalized)


def is_lunch_required(departure_time, return_time):
    departure_time = int(departure_time)
    return_time = int(return_time)
    if return_time <= LUNCH_SKIP_IF_RETURN_BY:
        return False
    if departure_time >= LUNCH_SKIP_IF_DEPART_AFTER:
        return False
    return True


def build_departure_candidates(order, visits, time_matrix):
    candidates = {DAY_START}
    if not order:
        return [DAY_START]

    has_appointment = any(visits[node - 1]["has_appointment"] for node in order)
    if not has_appointment:
        return [DAY_START]

    cumulative_min = 0
    for idx, node in enumerate(order):
        prev_node = 0 if idx == 0 else order[idx - 1]
        cumulative_min += time_matrix[prev_node][node]
        visit = visits[node - 1]
        if visit["has_appointment"] and visit.get("appointment_minute") is not None:
            latest_departure = visit["appointment_minute"] - cumulative_min
            for offset in (0, -15, -30, -45, -60):
                shifted = latest_departure + offset
                if shifted >= DAY_START:
                    candidates.add(shifted)
        cumulative_min += visit["service_time"]

    total_route_min = cumulative_min + time_matrix[order[-1]][0]
    latest_by_return = RETURN_LIMIT - total_route_min
    for offset in (0, -15, -30):
        shifted = latest_by_return + offset
        if shifted >= DAY_START:
            candidates.add(shifted)

    if any(candidate >= LUNCH_SKIP_IF_DEPART_AFTER for candidate in candidates):
        candidates.add(LUNCH_SKIP_IF_DEPART_AFTER)

    bounded = {
        min(RETURN_LIMIT, max(DAY_START, int(candidate)))
        for candidate in candidates
    }
    ordered = sorted(bounded)
    if len(ordered) <= 4:
        return ordered

    reduced = []
    for candidate in reversed(ordered):
        if all(abs(candidate - existing) >= 15 for existing in reduced):
            reduced.append(candidate)
        if len(reduced) >= 4:
            break

    reduced.append(DAY_START)
    return sorted(set(reduced))


def simulate_order(order, visits, time_matrix, distance_matrix, start_display_address=None, return_display_address=None, coords=None, trip_date=None, start_time=DAY_START, leg_cache=None, route_cache=None):
    best_result = None
    leg_cache = leg_cache if leg_cache is not None else {}
    route_cache = route_cache if route_cache is not None else get_route_cache()

    def resolve_leg(from_node, to_node, departure_min):
        cache_key = (from_node, to_node, int(departure_min))
        if cache_key in leg_cache:
            return leg_cache[cache_key]

        prediction_time = build_prediction_time(trip_date, departure_min)
        if USE_TRAFFIC_FOR_PLANNING and coords and prediction_time:
            resolved = get_route_info(coords[from_node], coords[to_node], prediction_time, route_cache=route_cache)
        else:
            resolved = (distance_matrix[from_node][to_node], time_matrix[from_node][to_node])

        leg_cache[cache_key] = resolved
        return resolved

    def consider_result(result):
        nonlocal best_result
        result["route_view"] = compress_route_view(result["route_view"])
        result["intra_wait_count"] = count_intra_wait_blocks(result["route_view"])
        result["locality_penalty"] = int(result["total_distance_m"] * 0.03) + (result["intra_wait_count"] * 500)
        result["departure_time"] = start_time
        result["lunch_required"] = is_lunch_required(start_time, result["return_time"])
        result["lunch_used"] = any(item.get("type") == "lunch" for item in result["route_view"])
        result["lunch_penalty"] = 0 if (not result["lunch_required"] and not result["lunch_used"]) else 1

        score = (
            result["appointment_violation_count"],
            result["appointment_late_total"],
            result["lunch_penalty"],
            -result["departure_time"],
            result["return_time"],
            result["total_distance_m"],
            result["locality_penalty"],
            result["wait_total"],
            result["wait_count"],
        )

        result["score"] = score
        if best_result is None or score < best_result["score"]:
            best_result = result

    def dfs(idx, last_node, current_time, lunch_used, route_view, total_distance_m, total_travel_min,
            wait_count, wait_total, appointment_violation_count, appointment_late_total, visit_no):
        if idx == len(order):
            end_route = clone_route(route_view)
            effective_end = current_time
            if order:
                dist_back_preview, travel_back_preview = resolve_leg(last_node, 0, effective_end)
            else:
                dist_back_preview = 0
                travel_back_preview = 0

            preview_return_time = effective_end + travel_back_preview
            lunch_optional = not is_lunch_required(start_time, preview_return_time)

            if not lunch_used and not lunch_optional:
                if current_time > LUNCH_START_MAX:
                    return

                added_wait = 0
                added_wait_total = 0

                if current_time < LUNCH_START_MIN:
                    added_wait = append_wait_block(end_route, current_time, LUNCH_START_MIN, "대기")
                    added_wait_total = LUNCH_START_MIN - current_time
                    lunch_start = LUNCH_START_MIN
                else:
                    lunch_start = current_time

                append_lunch_block(end_route, lunch_start)
                effective_end = lunch_start + LUNCH_DURATION
                wait_count += added_wait
                wait_total += added_wait_total

            if order:
                dist_back, travel_back = resolve_leg(last_node, 0, effective_end)
            else:
                dist_back = 0
                travel_back = 0

            return_time = effective_end + travel_back
            add_return_block(end_route, return_time, dist_back, travel_back, return_display_address)

            consider_result({
                "route_view": end_route,
                "return_time": return_time,
                "return_late": max(0, return_time - RETURN_LIMIT),
                "wait_count": wait_count,
                "wait_total": wait_total,
                "total_distance_m": total_distance_m + dist_back,
                "total_travel_min": total_travel_min + travel_back,
                "appointment_violation_count": appointment_violation_count,
                "appointment_late_total": appointment_late_total,
                "intra_wait_count": 0,
            })
            return

        node = order[idx]
        visit = visits[node - 1]
        travel_m, travel_min = resolve_leg(last_node, node, current_time)
        wait_label = "출발지 대기" if idx == 0 and last_node == 0 else "대기"

        pre_options = [{
            "time_after_pre": current_time,
            "lunch_used": lunch_used,
            "route": clone_route(route_view),
            "wait_count": 0,
            "wait_total": 0
        }]

        expanded = []
        for pre in pre_options:
            expanded.append(pre)
            expanded.extend(maybe_insert_lunch(
                pre["route"],
                pre["time_after_pre"],
                pre["lunch_used"],
                wait_label
            ))
        pre_options = expanded

        for pre in pre_options:
            depart_time = pre["time_after_pre"]
            arrival_time = depart_time + travel_min

            appt_violation = appointment_violation_count
            appt_late = appointment_late_total

            if visit["has_appointment"]:
                target = visit["appointment_minute"]
                if arrival_time > target:
                    appt_violation += 1
                    appt_late += arrival_time - target

            new_route = clone_route(pre["route"])
            add_visit_block(new_route, visit_no + 1, visit, arrival_time, travel_m, travel_min)
            new_time = arrival_time + visit["service_time"]

            dfs(
                idx + 1, node, new_time, pre["lunch_used"], new_route,
                total_distance_m + travel_m, total_travel_min + travel_min,
                wait_count + pre["wait_count"], wait_total + pre["wait_total"],
                appt_violation, appt_late, visit_no + 1
            )

    initial_route = [{
        "type": "start",
        "label": "S",
        "name": "출발",
        "address": start_display_address or get_start_address(),
        "arrival": minutes_to_str(start_time),
        "end_time": minutes_to_str(start_time),
        "service_time": 0,
        "travel_km": None,
        "travel_min": None
    }]

    dfs(0, 0, start_time, False, initial_route, 0, 0, 0, 0, 0, 0, 0)
    return best_result


def choose_best_schedule(visits, distance_matrix, time_matrix, start_display_address=None, return_display_address=None, coords=None, trip_date=None):
    route_cache = get_route_cache()
    shared_leg_cache = {}
    if not visits:
        result = simulate_order([], visits, time_matrix, distance_matrix, start_display_address, return_display_address, coords, trip_date, leg_cache=shared_leg_cache, route_cache=route_cache)
        save_route_cache(force=True)
        return [], result

    order = optimize_route(visits, distance_matrix, time_matrix)
    best = None
    for start_time in build_departure_candidates(order, visits, time_matrix):
        candidate = simulate_order(
            order,
            visits,
            time_matrix,
            distance_matrix,
            start_display_address,
            return_display_address,
            coords,
            trip_date,
            start_time=start_time,
            leg_cache=shared_leg_cache,
            route_cache=route_cache,
        )
        if candidate is None:
            continue
        if best is None or candidate["score"] < best["score"]:
            best = candidate
    if best is None:
        best = simulate_order(
            order,
            visits,
            time_matrix,
            distance_matrix,
            start_display_address,
            return_display_address,
            coords,
            trip_date,
            start_time=DAY_START,
            leg_cache=shared_leg_cache,
            route_cache=route_cache,
        )
    save_route_cache(force=True)
    return order, best


initialize_storage()


@app.route("/", methods=["GET", "POST"])
def start():
    settings = load_settings()
    team_users = get_effective_team_users(settings)
    team_options = list(team_users.keys())

    if request.method == "POST":
        user_name = request.form.get("user_name", "").strip()
        team_no = request.form.get("team_no", "").strip()
        trip_date = request.form.get("trip_date", "").strip()

        if not user_name or not team_no or not trip_date or not is_valid_team_user_selection(team_users, team_no, user_name):
            return render_template(
                "start.html",
                user_name="",
                team_no="",
                trip_date="",
                team_options=team_options,
                team_users=team_users
            )

        session["user_name"] = user_name
        session["team_no"] = team_no
        session["trip_date"] = trip_date
        session.pop("last_result_payload", None)
        return redirect(url_for("planner"))

    session.pop("user_name", None)
    session.pop("team_no", None)
    session.pop("trip_date", None)
    session.pop("last_result_payload", None)

    return render_template(
        "start.html",
        user_name="",
        team_no="",
        trip_date="",
        team_options=team_options,
        team_users=team_users
    )


@app.route("/reset")
def reset():
    session.clear()
    return redirect(url_for("start"))


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if session.get("is_admin"):
        return redirect(url_for("admin_settings_page"))

    if is_mobile_request():
        return render_template("admin_login.html", mobile_blocked=True)

    if request.method == "POST":
        password = (request.form.get("password") or "").strip()
        if password == get_admin_password():
            session["is_admin"] = True
            return redirect(url_for("admin_settings_page"))
        flash("비밀번호가 올바르지 않습니다.")
    return render_template("admin_login.html", mobile_blocked=False)


@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    return redirect(url_for("start"))


@app.route("/admin/settings", methods=["GET"])
def admin_settings_page():
    if not session.get("is_admin"):
        return redirect(url_for("admin_login"))
    if is_mobile_request():
        abort(403)
    settings = load_settings()
    return render_template("admin_settings.html", settings=settings)


@app.route("/admin/settings/verify-password", methods=["POST"])
def verify_admin_password():
    if not session.get("is_admin"):
        return jsonify({"success": False, "message": "관리자 로그인이 필요합니다."}), 401

    if is_mobile_request():
        return jsonify({"success": False, "message": "모바일에서는 관리자 설정을 변경할 수 없습니다."}), 403

    current_password = (request.form.get("current_admin_password") or "").strip()
    if not current_password:
        return jsonify({"success": False, "message": "현재 관리자 비밀번호를 입력해 주세요."}), 400

    if current_password != get_admin_password():
        return jsonify({"success": False, "message": "현재 관리자 비밀번호가 올바르지 않습니다."}), 400

    return jsonify({"success": True, "message": "비밀번호가 확인되었습니다."})


@app.route("/admin/settings/search-parking-place", methods=["GET"])
def admin_search_parking_place():
    if not session.get("is_admin"):
        return jsonify({"success": False, "message": "관리자 로그인이 필요합니다."}), 401

    if is_mobile_request():
        return jsonify({"success": False, "message": "모바일에서는 관리자 설정을 변경할 수 없습니다."}), 403

    query = (request.args.get("query") or "").strip()
    if len(query) < 2:
        return jsonify({"success": False, "message": "검색어를 2자 이상 입력해 주세요."}), 400

    tmap_app_key = get_tmap_app_key()
    if not tmap_app_key:
        return jsonify({"success": False, "message": "API 설정에서 TMAP App Key를 먼저 입력해 주세요."}), 400

    try:
        response = requests.get(
            "https://apis.openapi.sk.com/tmap/pois",
            headers={
                "Accept": "application/json",
                "appKey": tmap_app_key,
            },
            params={
                "version": "1",
                "query": query,
                "searchKeyword": query,
                "searchType": "all",
                "page": 1,
                "count": 8,
                "resCoordType": "WGS84GEO",
                "reqCoordType": "WGS84GEO",
                "multiPoint": "N",
                "searchtypCd": "A",
                "poiGroupYn": "N",
            },
            timeout=10,
        )
    except Exception:
        return jsonify({"success": False, "message": "TMAP 장소 검색 API 호출 중 오류가 발생했습니다."}), 502

    if response.status_code != 200:
        detail = ""
        try:
            detail = (response.json() or {}).get("errorMessage") or ""
        except Exception:
            detail = ""
        return jsonify({"success": False, "message": f"TMAP 장소 검색 API 응답 오류입니다. {detail}".strip()}), 502

    try:
        payload = response.json() or {}
    except Exception:
        return jsonify({"success": False, "message": "TMAP 장소 검색 API 응답을 해석하지 못했습니다."}), 502

    poi_list = (((payload.get("searchPoiInfo") or {}).get("pois") or {}).get("poi") or [])
    if isinstance(poi_list, dict):
        poi_list = [poi_list]

    items = []
    for raw in poi_list:
        if not isinstance(raw, dict):
            continue

        name = str(raw.get("name") or "").strip()
        jibun_parts = [
            str(raw.get("upperAddrName") or "").strip(),
            str(raw.get("middleAddrName") or "").strip(),
            str(raw.get("lowerAddrName") or "").strip(),
        ]
        jibun_base = " ".join([x for x in jibun_parts if x])
        first_no = str(raw.get("firstNo") or "").strip()
        second_no = str(raw.get("secondNo") or "").strip()
        jibun_no = ""
        if first_no:
            # 부지번이 0이면 '-0'은 표기하지 않음 (예: 845-0 -> 845)
            jibun_no = first_no if (not second_no or second_no == "0") else f"{first_no}-{second_no}"
        jibun_address = " ".join([x for x in [jibun_base, jibun_no] if x]).strip()

        address = jibun_address
        if not name or not jibun_address:
            continue

        items.append({
            "name": name,
            "address": address,
            "road_address": "",
            "jibun_address": jibun_address,
            "display_address": jibun_address,
            "category": str(raw.get("upperBizName") or "").strip(),
        })

    return jsonify({"success": True, "items": items})


@app.route("/admin/settings/save-section", methods=["POST"])
def save_admin_settings_section():
    if not session.get("is_admin"):
        return jsonify({"success": False, "message": "관리자 로그인이 필요합니다."}), 401

    if is_mobile_request():
        return jsonify({"success": False, "message": "모바일에서는 관리자 설정을 변경할 수 없습니다."}), 403

    settings = load_settings()
    section = (request.form.get("section") or "").strip()

    try:
        if section == "mail":
            settings["mail"]["smtp_host"] = (request.form.get("smtp_host") or "smtp.gmail.com").strip() or "smtp.gmail.com"

            smtp_port_raw = (request.form.get("smtp_port") or "587").strip()
            try:
                settings["mail"]["smtp_port"] = int(smtp_port_raw)
            except Exception:
                return jsonify({"success": False, "message": "SMTP 포트는 숫자로 입력해 주세요."})

            settings["mail"]["smtp_user"] = (request.form.get("smtp_user") or "").strip()
            settings["mail"]["smtp_password"] = (request.form.get("smtp_password") or "").strip()
            settings["mail"]["mail_from"] = (request.form.get("mail_from") or "").strip()
            settings["mail"]["default_recipient_email"] = (request.form.get("default_recipient_email") or "").strip()
            settings["mail"]["email_subject_template"] = (request.form.get("email_subject_template") or DEFAULT_SETTINGS["mail"]["email_subject_template"]).strip()
            settings["mail"]["email_body_template"] = (request.form.get("email_body_template") or DEFAULT_SETTINGS["mail"]["email_body_template"]).strip()

        elif section == "api":
            settings["api"]["client_id"] = (request.form.get("client_id") or "").strip()
            settings["api"]["client_secret"] = (request.form.get("client_secret") or "").strip()
            settings["api"]["tmap_app_key"] = (request.form.get("tmap_app_key") or TMAP_DEFAULT_APP_KEY).strip()

        elif section == "user":
            start_address = (request.form.get("start_address") or "").strip()
            return_address = (request.form.get("return_address") or "").strip()
            return_same_as_start = (request.form.get("return_same_as_start") or "").strip() == "1"

            settings["user"]["start_address"] = start_address or START_ADDRESS
            settings["user"]["return_same_as_start"] = return_same_as_start
            settings["user"]["return_address"] = settings["user"]["start_address"] if return_same_as_start else (return_address or settings["user"]["start_address"])
            settings["user"]["enable_guest_user"] = (request.form.get("enable_guest_user") or "1") == "1"

            team_names = request.form.getlist("team_name")
            team_user_blocks = request.form.getlist("team_users_block")

            team_users = {}
            for idx, raw_name in enumerate(team_names):
                team_name = (raw_name or "").strip()
                if not team_name:
                    return jsonify({"success": False, "message": "조 이름은 필수 입력 항목입니다."})
                if team_name == GUEST_TEAM_NAME:
                    return jsonify({"success": False, "message": "게스트는 카드 목록에서 수정할 수 없습니다."})

                raw_users = team_user_blocks[idx] if idx < len(team_user_blocks) else ""
                users = [x.strip() for x in raw_users.splitlines() if x.strip() and x.strip() != GUEST_USER_NAME]
                if not users:
                    return jsonify({"success": False, "message": "사용자 목록은 필수 입력 항목입니다."})
                team_users[team_name] = users

            if not team_users:
                return jsonify({"success": False, "message": "최소 1개의 카드를 유지해 주세요."})

            settings["user"]["team_users"] = normalize_team_users(team_users)

        elif section == "restaurant":
            names = request.form.getlist("restaurant_name")
            menus = request.form.getlist("restaurant_menu")
            prices = request.form.getlist("restaurant_price")
            addresses = request.form.getlist("restaurant_address")
            parkings = request.form.getlist("restaurant_parking")
            payment_cards = request.form.getlist("restaurant_payment_card")
            payment_cashes = request.form.getlist("restaurant_payment_cash")
            notes = request.form.getlist("restaurant_note")

            items = []
            for idx, raw_name in enumerate(names):
                name = (raw_name or "").strip()
                menu = (menus[idx] if idx < len(menus) else "").strip()
                price = (prices[idx] if idx < len(prices) else "").strip()
                address = (addresses[idx] if idx < len(addresses) else "").strip()
                raw_parking = (parkings[idx] if idx < len(parkings) else "unknown").strip()
                parking = raw_parking if raw_parking in {"1", "0", "unknown"} else "unknown"
                payment_card = (payment_cards[idx] if idx < len(payment_cards) else "0") == "1"
                payment_cash = (payment_cashes[idx] if idx < len(payment_cashes) else "0") == "1"
                note = (notes[idx] if idx < len(notes) else "").strip()

                if not any([name, menu, price, address]):
                    continue
                if not name or not menu or not price or not address:
                    return jsonify({"success": False, "message": "식당명, 메뉴, 가격, 주소는 모두 입력해 주세요."})

                items.append({
                    "name": name,
                    "menu": menu,
                    "price": price,
                    "address": address,
                    "parking": parking,
                    "payment_card": payment_card,
                    "payment_cash": payment_cash,
                    "note": note,
                })

            settings["restaurant"]["items"] = items

        elif section == "parking":
            names = request.form.getlist("parking_name")
            addresses = request.form.getlist("parking_address")

            items = []
            for idx, raw_name in enumerate(names):
                name = (raw_name or "").strip()
                address = (addresses[idx] if idx < len(addresses) else "").strip()

                if not any([name, address]):
                    continue
                if not name or not address:
                    return jsonify({"success": False, "message": "주차장명과 주소를 모두 입력해 주세요."})

                items.append({
                    "name": name,
                    "address": address,
                })

            settings["parking"]["items"] = items

        elif section == "admin":
            current_password = (request.form.get("current_admin_password") or "").strip()
            new_password = (request.form.get("new_admin_password") or "").strip()
            confirm_password = (request.form.get("confirm_admin_password") or "").strip()

            saved_password = get_admin_password()

            if not current_password:
                return jsonify({"success": False, "message": "현재 관리자 비밀번호를 입력해 주세요."})

            if current_password != saved_password:
                return jsonify({"success": False, "message": "현재 관리자 비밀번호가 올바르지 않습니다."})

            if not new_password:
                return jsonify({"success": False, "message": "새 관리자 비밀번호를 입력해 주세요."})

            if new_password != confirm_password:
                return jsonify({"success": False, "message": "새 관리자 비밀번호와 확인 값이 일치하지 않습니다."})

            settings["admin"]["admin_password"] = new_password

        else:
            return jsonify({"success": False, "message": "잘못된 설정 항목입니다."}), 400

        save_settings(settings)
        return jsonify({"success": True, "message": "변경사항이 저장되었습니다."})

    except Exception as e:
        return jsonify({"success": False, "message": f"저장 중 오류가 발생했습니다: {e}"}), 500


@app.route("/planner/ocr-addresses", methods=["POST"])
def planner_ocr_addresses():
    file = request.files.get("image")
    if not file or not getattr(file, "filename", ""):
        return jsonify({"success": False, "message": "OCR할 이미지 파일을 선택해 주세요."}), 400

    try:
        image_bytes = file.read()
    except Exception:
        image_bytes = b""

    if not image_bytes:
        return jsonify({"success": False, "message": "업로드된 이미지 내용을 읽지 못했습니다."}), 400

    if len(image_bytes) > 12 * 1024 * 1024:
        return jsonify({"success": False, "message": "이미지 파일이 너무 큽니다. 12MB 이하 파일을 사용해 주세요."}), 400

    text, error = call_google_vision_ocr(image_bytes)
    if error:
        return jsonify({"success": False, "message": error}), 502

    addresses = extract_valid_ocr_addresses(text)
    return jsonify({
        "success": True,
        "addresses": addresses,
        "raw_text": text,
    })


@app.route("/planner/resolve-qr-items", methods=["POST"])
def planner_resolve_qr_items():
    payload = request.get_json(silent=True) or {}
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        return jsonify({"success": False, "message": "QR 항목이 없습니다."}), 400

    resolved_items = []
    for raw_item in items[:15]:
        if not isinstance(raw_item, dict):
            continue

        name = str(raw_item.get("name") or "").strip()
        address = str(raw_item.get("address") or "").strip()
        if not name and not address:
            continue

        coord, meta, err = geocode_with_meta(address)
        if coord:
            resolved_items.append({
                "name": name,
                "address": address,
                "display_address": (meta or {}).get("display_address") or address,
                "ok": True,
                "message": "",
            })
        else:
            resolved_items.append({
                "name": name,
                "address": address,
                "display_address": address,
                "ok": False,
                "message": err or "주소 확인에 실패했습니다.",
            })

    return jsonify({"success": True, "items": resolved_items})


@app.route("/planner", methods=["GET", "POST"])
def planner():
    trip_meta = get_trip_meta()

    if not trip_meta["user_name"] or not trip_meta["team_no"] or not trip_meta["trip_date"]:
        return redirect(url_for("start"))

    if request.method == "POST":
        names = request.form.getlist("name")
        addresses = request.form.getlist("address")
        service_times_raw = request.form.getlist("service_time")
        has_appointment_flags = request.form.getlist("has_appointment_flag")
        visit_hours = request.form.getlist("visit_hour")
        visit_minutes = request.form.getlist("visit_minute")

        visits = []
        for i in range(len(addresses)):
            name = names[i].strip() if i < len(names) else ""
            address = addresses[i].strip() if i < len(addresses) else ""

            if not name or not address:
                continue

            try:
                service_time = int(service_times_raw[i]) if i < len(service_times_raw) else 0
            except Exception:
                service_time = 0

            has_appt = i < len(has_appointment_flags) and str(has_appointment_flags[i]).strip() == "1"

            appointment_minute = None
            if has_appt:
                hour = visit_hours[i] if i < len(visit_hours) else ""
                minute = visit_minutes[i] if i < len(visit_minutes) else ""
                appointment_minute = parse_appointment_minute(hour, minute)

            visits.append({
                "visit_id": i,
                "name": name,
                "address": address,
                "service_time": service_time,
                "has_appointment": has_appt,
                "appointment_minute": appointment_minute
            })

        if not visits:
            payload = {
                "route": [],
                "total_count": 0,
                "total_distance": "--",
                "total_time": "--",
                "end_time": "--:--",
                "warning_message": "",
                "team_no": trip_meta["team_no"],
                "user_name": trip_meta["user_name"],
                "trip_date": trip_meta["trip_date"]
            }
            session["last_result_payload"] = payload
            return redirect(url_for("result_page"))

        start_coord, start_meta, start_err = geocode_with_meta(get_start_address())
        if not start_coord:
            payload = {
                "route": [],
                "total_count": 0,
                "total_distance": "--",
                "total_time": "--",
                "end_time": "--:--",
                "warning_message": f"출발지 좌표를 불러오지 못했습니다. ({start_err})",
                "team_no": trip_meta["team_no"],
                "user_name": trip_meta["user_name"],
                "trip_date": trip_meta["trip_date"]
            }
            session["last_result_payload"] = payload
            return redirect(url_for("result_page"))

        start_display_address = (start_meta or {}).get("display_address") or get_start_address()
        _, return_meta, _ = geocode_with_meta(get_return_address())
        return_display_address = (return_meta or {}).get("display_address") or get_return_address()

        coords = [start_coord]
        failed_addresses = []

        for visit in visits:
            coord, meta, err = geocode_with_meta(visit["address"])
            coords.append(coord)
            if coord is None:
                failed_addresses.append(f"{visit['name']} / {visit['address']} / {err}")
            else:
                visit["display_address"] = (meta or {}).get("display_address") or visit["address"]

        if failed_addresses:
            payload = {
                "route": [],
                "total_count": len(visits),
                "total_distance": "--",
                "total_time": "--",
                "end_time": "--:--",
                "warning_message": "일부 주소의 좌표를 불러오지 못했습니다. " + " | ".join(failed_addresses[:3]),
                "team_no": trip_meta["team_no"],
                "user_name": trip_meta["user_name"],
                "trip_date": trip_meta["trip_date"]
            }
            session["last_result_payload"] = payload
            return redirect(url_for("result_page"))

        size = len(coords)
        dist_matrix = [[0] * size for _ in range(size)]
        time_matrix = [[0] * size for _ in range(size)]

        for i in range(size):
            for j in range(i + 1, size):
                d, t = estimate_matrix_leg(coords[i], coords[j])
                dist_matrix[i][j] = d
                time_matrix[i][j] = t
                dist_matrix[j][i] = d
                time_matrix[j][i] = t

        _, best = choose_best_schedule(
            visits,
            dist_matrix,
            time_matrix,
            start_display_address=start_display_address,
            return_display_address=return_display_address,
            coords=coords,
            trip_date=trip_meta["trip_date"],
        )

        if best is None:
            payload = {
                "route": [],
                "total_count": len(visits),
                "total_distance": "--",
                "total_time": "--",
                "end_time": "--:--",
                "warning_message": "조건에 맞는 경로를 계산하지 못했습니다.",
                "team_no": trip_meta["team_no"],
                "user_name": trip_meta["user_name"],
                "trip_date": trip_meta["trip_date"]
            }
            session["last_result_payload"] = payload
            return redirect(url_for("result_page"))

        warning_message = ""
        if best["return_late"] > 0:
            warning_message = f"복귀시간이 16:30보다 {best['return_late']}분 늦습니다."

        try:
            parking_items = (load_settings().get("parking", {}) or {}).get("items", [])
            best["route_view"] = enrich_route_with_nearby_parking(
                best.get("route_view", []),
                visits,
                coords[1:],
                parking_items,
                radius_m=1000,
                max_items=5,
            )
        except Exception:
            app.logger.exception("Failed to enrich nearby parking data; serving route without parking info.")

        total_time_min = int(best["return_time"] - best.get("departure_time", DAY_START))
        total_distance_km = round(best["total_distance_m"] / 1000, 2)

        payload = {
            "route": best["route_view"],
            "total_count": len(visits),
            "total_distance": total_distance_km,
            "total_time": total_time_min,
            "end_time": minutes_to_str(best["return_time"]),
            "warning_message": warning_message,
            "team_no": trip_meta["team_no"],
            "user_name": trip_meta["user_name"],
            "trip_date": trip_meta["trip_date"]
        }
        session["last_result_payload"] = payload

        return redirect(url_for("result_page"))

    return render_template("index.html", team_no=trip_meta["team_no"], user_name=trip_meta["user_name"], trip_date=trip_meta["trip_date"])


@app.route("/result", methods=["GET"])
def result_page():
    trip_meta = get_trip_meta()
    payload = session.get("last_result_payload")
    if not trip_meta["user_name"] or not trip_meta["team_no"] or not trip_meta["trip_date"] or not payload:
        return redirect(url_for("start"))
    return render_template("result.html", **payload, tmap_app_key=get_tmap_app_key())


@app.route("/result/demo", methods=["GET"])
def result_demo_page():
    payload = {
        "team_no": "1조",
        "user_name": "데모 사용자",
        "trip_date": "2026-03-18",
        "total_count": 2,
        "total_distance": 32.4,
        "total_time": 385,
        "end_time": "16:25",
        "warning_message": "복귀 시간이 16:30에 근접합니다. 방문 순서를 확인해 주세요.",
        "route": [
            {
                "type": "start",
                "label": "S",
                "name": "출발지",
                "address": "서울특별시 종로구 사직로 161",
                "arrival": "10:00",
                "end_time": "10:00",
                "service_time": 0,
                "travel_km": None,
                "travel_min": None,
            },
            {
                "type": "visit",
                "label": "1",
                "name": "홍길동",
                "address": "서울특별시 용산구 한강대로 405",
                "arrival": "10:35",
                "end_time": "11:10",
                "service_time": 35,
                "travel_km": 8.4,
                "travel_min": 35,
                "appointment_time": "10:40",
                "nearby_parkings": [
                    {
                        "name": "서울역 공영주차장",
                        "address": "서울특별시 용산구 청파로 378",
                        "distance_m": 320,
                        "drive_min": 3,
                        "walk_min": 5,
                    },
                    {
                        "name": "남영역 민영주차장",
                        "address": "서울특별시 용산구 한강대로 270",
                        "distance_m": 870,
                        "drive_min": 5,
                        "walk_min": 11,
                    },
                ],
            },
            {
                "type": "wait",
                "label": "W",
                "name": "대기",
                "address": "",
                "arrival": "11:10",
                "end_time": "11:30",
                "service_time": 20,
                "travel_km": None,
                "travel_min": None,
            },
            {
                "type": "lunch",
                "label": "L",
                "name": "점심",
                "address": "",
                "arrival": "11:30",
                "end_time": "12:30",
                "service_time": 60,
                "travel_km": None,
                "travel_min": None,
            },
            {
                "type": "visit",
                "label": "2",
                "name": "김철수",
                "address": "서울특별시 송파구 올림픽로 300",
                "arrival": "13:10",
                "end_time": "14:00",
                "service_time": 50,
                "travel_km": 14.2,
                "travel_min": 40,
                "appointment_time": None,
                "nearby_parkings": [],
            },
            {
                "type": "return",
                "label": "R",
                "name": "복귀",
                "address": "서울특별시 종로구 사직로 161",
                "arrival": "16:25",
                "end_time": "16:25",
                "service_time": 0,
                "travel_km": 9.8,
                "travel_min": 55,
            },
        ],
    }
    return render_template("result.html", **payload, tmap_app_key=get_tmap_app_key())


@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
