from flask import Flask, render_template, request, redirect, url_for, session, Response, flash, abort, jsonify
import requests
from io import BytesIO
import smtplib
from email.message import EmailMessage
import os
import json
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer

app = Flask(__name__)
app.secret_key = "replace-this-with-your-secure-secret-key"

START_ADDRESS = "서울특별시 종로구 율곡로2길 19"
RETURN_ADDRESS = "서울특별시 종로구 율곡로2길 19"

ADMIN_PASSWORD = "cpskqrhksfleks12#"
SETTINGS_FILE = "admin_settings.json"
GEOCODE_CACHE_FILE = "geocode_cache.json"
ROUTE_CACHE_FILE = "route_cache.json"

DEFAULT_TEAM_USERS = {"1조": []}

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
        "tmap_app_key": ""
    },
    "user": {
        "start_address": START_ADDRESS,
        "return_address": RETURN_ADDRESS,
        "return_same_as_start": True,
        "team_users": DEFAULT_TEAM_USERS
    },
    "admin": {
        "admin_password": ADMIN_PASSWORD
    }
}

DAY_START = 10 * 60
NO_LUNCH_IF_DONE_BY = 12 * 60
LUNCH_START_MIN = 11 * 60 + 30
LUNCH_START_MAX = 12 * 60 + 30
LUNCH_DURATION = 60
RETURN_LIMIT = 16 * 60 + 30

BEAM_WIDTH = 24
LOCAL_IMPROVE_ITER = 80
MAX_PARTIAL_CANDIDATES = 1200


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


def normalize_team_users(team_users):
    normalized = {}

    if isinstance(team_users, dict):
        for team_name, users in team_users.items():
            key = str(team_name).strip()
            if not key:
                continue
            if isinstance(users, list):
                normalized[key] = [str(x).strip() for x in users if str(x).strip()]

    if not normalized:
        normalized = {"1조": []}

    return normalized


def deep_copy_default_settings():
    return json.loads(json.dumps(DEFAULT_SETTINGS, ensure_ascii=False))


def migrate_legacy_settings(data):
    merged = deep_copy_default_settings()

    if not isinstance(data, dict):
        return merged

    has_new_structure = any(k in data for k in ["mail", "api", "user", "admin"])
    if has_new_structure:
        for section in ["mail", "api", "user", "admin"]:
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

    if not merged["user"].get("start_address"):
        merged["user"]["start_address"] = START_ADDRESS
    if not merged["user"].get("return_address"):
        merged["user"]["return_address"] = merged["user"].get("start_address", START_ADDRESS)
    if "return_same_as_start" not in merged["user"]:
        merged["user"]["return_same_as_start"] = True
    if not merged["admin"].get("admin_password"):
        merged["admin"]["admin_password"] = ADMIN_PASSWORD
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
    if not os.path.exists(SETTINGS_FILE):
        settings = deep_copy_default_settings()
        save_settings(settings)
        return settings

    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}

    return migrate_legacy_settings(data)


def save_settings(data):
    merged = migrate_legacy_settings(data)
    save_json_file(SETTINGS_FILE, merged)


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
    return (settings.get("api", {}).get("tmap_app_key") or "").strip()


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
    return load_json_file(GEOCODE_CACHE_FILE, {})


def save_geocode_cache(cache):
    save_json_file(GEOCODE_CACHE_FILE, cache)


def get_route_cache():
    return load_json_file(ROUTE_CACHE_FILE, {})


def save_route_cache(cache):
    save_json_file(ROUTE_CACHE_FILE, cache)


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


def get_route_info(start, goal):
    url = "https://maps.apigw.ntruss.com/map-direction/v1/driving"
    headers = get_api_headers()

    if not headers["X-NCP-APIGW-API-KEY-ID"] or not headers["X-NCP-APIGW-API-KEY"]:
        return 99999999, 9999

    start_key = f"{round(start[0], 6)},{round(start[1], 6)}"
    goal_key = f"{round(goal[0], 6)},{round(goal[1], 6)}"
    cache_key = f"{start_key}|{goal_key}"

    route_cache = get_route_cache()
    if cache_key in route_cache:
        item = route_cache[cache_key]
        return int(item["distance_m"]), int(item["duration_min"])

    params = {
        "start": f"{start[0]},{start[1]}",
        "goal": f"{goal[0]},{goal[1]}",
        "option": "trafast",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
    except requests.RequestException:
        return 99999999, 9999

    if resp.status_code != 200:
        return 99999999, 9999

    try:
        data = resp.json()
        summary = data["route"]["trafast"][0]["summary"]
        distance_m = int(summary["distance"])
        duration_min = int(summary["duration"]) // 60000

        route_cache[cache_key] = {
            "distance_m": distance_m,
            "duration_min": duration_min,
            "updated_at": datetime.now().isoformat()
        }
        save_route_cache(route_cache)

        return distance_m, duration_min
    except Exception:
        return 99999999, 9999


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
                else:
                    penalty += max(0, (target - arrival - 20)) * 2

            score = dist_matrix[current][node] + penalty
            scored.append((score, node))

        scored.sort()
        nxt = scored[0][1]
        visit = visits[nxt - 1]
        arrival = current_time + time_matrix[current][nxt]
        if visit["has_appointment"] and arrival < visit["appointment_minute"]:
            arrival = visit["appointment_minute"]

        current_time = arrival + visit["service_time"]
        order.append(nxt)
        remaining.remove(nxt)
        current = nxt

    return order


def partial_path_score(path, visits, dist_matrix, time_matrix):
    if not path:
        return 0

    current_time = DAY_START
    last = 0
    total_dist = 0
    lateness_penalty = 0
    early_wait_penalty = 0
    locality_penalty = 0

    for idx, node in enumerate(path):
        total_dist += dist_matrix[last][node]
        travel = time_matrix[last][node]
        arrival = current_time + travel
        visit = visits[node - 1]

        if visit["has_appointment"]:
            target = visit["appointment_minute"]
            if arrival > target:
                lateness_penalty += (arrival - target) * 5000
            else:
                wait = target - arrival
                if wait > 30:
                    early_wait_penalty += (wait - 30) * 15
                arrival = target

        current_time = arrival + visit["service_time"]

        if idx >= 1:
            locality_penalty += dist_matrix[path[idx - 1]][node] * 0.03

        last = node

    return total_dist + lateness_penalty + early_wait_penalty + locality_penalty


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


def optimize_route(visits, dist_matrix, time_matrix):
    order = beam_search_route(visits, dist_matrix, time_matrix)
    order = two_opt(order, dist_matrix, time_matrix, visits)
    order = relocate_improve(order, dist_matrix, time_matrix, visits)
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
        "name": visit["name"],
        "address": visit["address"],
        "arrival": minutes_to_str(arrival_min),
        "end_time": minutes_to_str(arrival_min + visit["service_time"]),
        "service_time": visit["service_time"],
        "travel_km": round(travel_m / 1000, 2),
        "travel_min": int(travel_min),
        "appointment_time": minutes_to_str(visit["appointment_minute"]) if visit["has_appointment"] else None,
    })


def add_return_block(route_view, arrival_min, travel_m, travel_min):
    route_view.append({
        "type": "return",
        "label": "R",
        "name": "복귀",
        "address": get_return_address(),
        "arrival": minutes_to_str(arrival_min),
        "end_time": minutes_to_str(arrival_min),
        "service_time": 0,
        "travel_km": round(travel_m / 1000, 2),
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


def simulate_order(order, visits, time_matrix, distance_matrix):
    best_result = None

    def consider_result(result):
        nonlocal best_result
        result["route_view"] = compress_route_view(result["route_view"])
        result["intra_wait_count"] = count_intra_wait_blocks(result["route_view"])

        score = (
            result["appointment_violation_count"],
            1 if result["return_late"] > 0 else 0,
            result["appointment_late_total"],
            result["intra_wait_count"],
            result["wait_total"],
            result["wait_count"],
            result["total_distance_m"],
            result["total_travel_min"],
            result["return_time"],
        )

        result["score"] = score
        if best_result is None or score < best_result["score"]:
            best_result = result

    def dfs(idx, last_node, current_time, lunch_used, route_view, total_distance_m, total_travel_min,
            wait_count, wait_total, appointment_violation_count, appointment_late_total, visit_no):
        if idx == len(order):
            end_route = clone_route(route_view)
            effective_end = current_time
            lunch_optional = current_time <= NO_LUNCH_IF_DONE_BY

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
                dist_back = distance_matrix[last_node][0]
                travel_back = time_matrix[last_node][0]
            else:
                dist_back = 0
                travel_back = 0

            return_time = effective_end + travel_back
            add_return_block(end_route, return_time, dist_back, travel_back)

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
        travel_min = time_matrix[last_node][node]
        travel_m = distance_matrix[last_node][node]
        wait_label = "출발지 대기" if idx == 0 and last_node == 0 else "대기"

        pre_options = [{
            "time_after_pre": current_time,
            "lunch_used": lunch_used,
            "route": clone_route(route_view),
            "wait_count": 0,
            "wait_total": 0
        }]

        if visit["has_appointment"]:
            depart_target = visit["appointment_minute"] - travel_min
            pre_options = best_depart_with_lunch(
                route_view=route_view,
                current_time=current_time,
                depart_time=depart_target,
                lunch_used=lunch_used,
                wait_label=wait_label
            )
        else:
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
                elif arrival_time < target:
                    arrival_time = target

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
        "address": get_start_address(),
        "arrival": minutes_to_str(DAY_START),
        "end_time": minutes_to_str(DAY_START),
        "service_time": 0,
        "travel_km": None,
        "travel_min": None
    }]

    dfs(0, 0, DAY_START, False, initial_route, 0, 0, 0, 0, 0, 0, 0)
    return best_result


def choose_best_schedule(visits, distance_matrix, time_matrix):
    if not visits:
        return [], simulate_order([], visits, time_matrix, distance_matrix)

    order = optimize_route(visits, distance_matrix, time_matrix)
    best = simulate_order(order, visits, time_matrix, distance_matrix)
    return order, best


def build_pdf_bytes(payload):
    buffer = BytesIO()
    page_size = landscape(A4)
    doc = SimpleDocTemplate(
        buffer, pagesize=page_size,
        leftMargin=8 * mm, rightMargin=8 * mm, topMargin=8 * mm, bottomMargin=8 * mm
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("TitleSmall", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=12, leading=14, spaceAfter=4)
    info_style = ParagraphStyle("InfoSmall", parent=styles["Normal"], fontName="Helvetica", fontSize=8, leading=10)
    cell_style = ParagraphStyle("CellSmall", parent=styles["Normal"], fontName="Helvetica", fontSize=7, leading=8)
    addr_style = ParagraphStyle("AddrBold", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=8, leading=9)

    story = []
    story.append(Paragraph("국세청 체납관리단 - SMART 경로탐색 결과", title_style))
    meta_text = f"{payload.get('team_no', '')}({payload.get('user_name', '')}) - {payload.get('trip_date', '').replace('-', '.')}"
    story.append(Paragraph(meta_text, info_style))
    story.append(Spacer(1, 2 * mm))

    summary_rows = [[
        Paragraph("<b>총 체납자 수</b>", cell_style),
        Paragraph("<b>총 이동 거리</b>", cell_style),
        Paragraph("<b>총 소요 시간</b>", cell_style),
        Paragraph("<b>예상 종료 시간</b>", cell_style),
    ], [
        Paragraph(str(payload.get("total_count", "")), cell_style),
        Paragraph(f"{payload.get('total_distance', '')} km", cell_style),
        Paragraph(f"{payload.get('total_time', '')} 분", cell_style),
        Paragraph(str(payload.get("end_time", "")), cell_style),
    ]]

    summary_table = Table(summary_rows, colWidths=[55 * mm, 55 * mm, 55 * mm, 55 * mm])
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eaf2ff")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
        ("INNERGRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(summary_table)

    warning_message = payload.get("warning_message", "")
    if warning_message:
        story.append(Spacer(1, 2 * mm))
        warning_table = Table([[Paragraph(f"<b>{warning_message}</b>", cell_style)]], colWidths=[220 * mm])
        warning_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#fff7ed")),
            ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#fed7aa")),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(warning_table)

    story.append(Spacer(1, 2 * mm))

    header = [
        Paragraph("<b>구분</b>", cell_style),
        Paragraph("<b>시간</b>", cell_style),
        Paragraph("<b>주소 / 내용</b>", cell_style),
        Paragraph("<b>약속</b>", cell_style),
        Paragraph("<b>방문</b>", cell_style),
        Paragraph("<b>이동</b>", cell_style),
        Paragraph("<b>거리</b>", cell_style),
    ]

    table_rows = [header]
    for v in payload.get("route", []):
        kind = {
            "start": "출발",
            "visit": f"방문 {v.get('label', '')}",
            "wait": "대기",
            "lunch": "점심",
            "return": "복귀",
        }.get(v.get("type"), v.get("type", ""))

        addr_content = Paragraph(v.get("address", "") if v.get("type") == "visit" else v.get("name", ""), addr_style if v.get("type") == "visit" else cell_style)

        time_text = f"{v.get('arrival', '')}" if v.get("type") in ("start", "return") else f"{v.get('arrival', '')}~{v.get('end_time', '')}"

        table_rows.append([
            Paragraph(kind, cell_style),
            Paragraph(time_text, cell_style),
            addr_content,
            Paragraph(v.get("appointment_time", "") or "-", cell_style),
            Paragraph(f"{v.get('service_time', 0)}분" if v.get("type") in ("visit", "wait", "lunch") else "-", cell_style),
            Paragraph(f"{v.get('travel_min', '')}분" if v.get("travel_min") is not None else "-", cell_style),
            Paragraph(f"{v.get('travel_km', '')}km" if v.get("travel_km") is not None else "-", cell_style),
        ])

    detail_table = Table(table_rows, colWidths=[20 * mm, 28 * mm, 95 * mm, 18 * mm, 18 * mm, 18 * mm, 18 * mm], repeatRows=1)
    detail_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dbeafe")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#cbd5e1")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (1, -1), "CENTER"),
        ("ALIGN", (3, 1), (-1, -1), "CENTER"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(detail_table)

    doc.build(story)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf


def send_result_email(recipient, payload):
    mail_config = get_mail_config()
    smtp_host = mail_config["smtp_host"]
    smtp_port = mail_config["smtp_port"]
    smtp_user = mail_config["smtp_user"]
    smtp_password = mail_config["smtp_password"]
    mail_from = mail_config["mail_from"]

    if not smtp_user or not smtp_password or not mail_from:
        return False

    settings = load_settings()
    mail_settings = settings.get("mail", {})

    subject_template = mail_settings.get("email_subject_template", DEFAULT_SETTINGS["mail"]["email_subject_template"])
    body_template = mail_settings.get("email_body_template", DEFAULT_SETTINGS["mail"]["email_body_template"])

    try:
        subject = subject_template.format(
            team_no=payload.get("team_no", ""),
            user_name=payload.get("user_name", ""),
            trip_date=payload.get("trip_date", "")
        )
    except Exception:
        subject = f"[경로결과] {payload.get('team_no', '')}({payload.get('user_name', '')}) - {payload.get('trip_date', '')}"

    try:
        body = body_template.format(
            team_no=payload.get("team_no", ""),
            user_name=payload.get("user_name", ""),
            trip_date=payload.get("trip_date", ""),
            total_count=payload.get("total_count", ""),
            total_distance=payload.get("total_distance", ""),
            total_time=payload.get("total_time", ""),
            end_time=payload.get("end_time", "")
        )
    except Exception:
        body = "경로 결과 PDF를 첨부합니다."

    try:
        pdf_bytes = build_pdf_bytes(payload)
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = mail_from
        msg["To"] = recipient
        msg.set_content(body)
        msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename="route_result.pdf")

        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.starttls()
            smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
        return True
    except Exception:
        return False


@app.route("/", methods=["GET", "POST"])
def start():
    settings = load_settings()
    team_users = settings.get("user", {}).get("team_users", {"1조": []})
    team_options = list(team_users.keys())

    if request.method == "POST":
        user_name = request.form.get("user_name", "").strip()
        team_no = request.form.get("team_no", "").strip()
        trip_date = request.form.get("trip_date", "").strip()

        if not user_name or not team_no or not trip_date:
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
            settings["api"]["tmap_app_key"] = (request.form.get("tmap_app_key") or "").strip()

        elif section == "user":
            start_address = (request.form.get("start_address") or "").strip()
            return_address = (request.form.get("return_address") or "").strip()
            return_same_as_start = (request.form.get("return_same_as_start") or "").strip() == "1"

            settings["user"]["start_address"] = start_address or START_ADDRESS
            settings["user"]["return_same_as_start"] = return_same_as_start
            settings["user"]["return_address"] = settings["user"]["start_address"] if return_same_as_start else (return_address or settings["user"]["start_address"])

            team_names = request.form.getlist("team_name")
            team_user_blocks = request.form.getlist("team_users_block")

            team_users = {}
            for idx, raw_name in enumerate(team_names):
                team_name = (raw_name or "").strip()
                if not team_name:
                    continue

                raw_users = team_user_blocks[idx] if idx < len(team_user_blocks) else ""
                users = [x.strip() for x in raw_users.splitlines() if x.strip()]
                team_users[team_name] = users

            settings["user"]["team_users"] = normalize_team_users(team_users)

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
            return render_template("result.html", **payload, tmap_app_key=get_tmap_app_key())

        start_coord, start_err = geocode(get_start_address())
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
            return render_template("result.html", **payload, tmap_app_key=get_tmap_app_key())

        coords = [start_coord]
        failed_addresses = []

        for visit in visits:
            coord, err = geocode(visit["address"])
            coords.append(coord)
            if coord is None:
                failed_addresses.append(f"{visit['name']} / {visit['address']} / {err}")

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
            return render_template("result.html", **payload, tmap_app_key=get_tmap_app_key())

        size = len(coords)
        dist_matrix = [[0] * size for _ in range(size)]
        time_matrix = [[0] * size for _ in range(size)]

        for i in range(size):
            for j in range(size):
                if i != j:
                    d, t = get_route_info(coords[i], coords[j])
                    dist_matrix[i][j] = d
                    time_matrix[i][j] = t

        _, best = choose_best_schedule(visits, dist_matrix, time_matrix)

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
            return render_template("result.html", **payload, tmap_app_key=get_tmap_app_key())

        warning_message = ""
        if best["return_late"] > 0:
            warning_message = f"복귀시간이 16:30보다 {best['return_late']}분 늦습니다."

        total_time_min = int(best["return_time"] - DAY_START)
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

        return render_template("result.html", **payload, tmap_app_key=get_tmap_app_key())

    return render_template("index.html", team_no=trip_meta["team_no"], user_name=trip_meta["user_name"], trip_date=trip_meta["trip_date"])


@app.route("/send-result-email", methods=["POST"])
def send_result_email_route():
    settings = load_settings()
    recipient = (request.form.get("email") or "").strip()
    if not recipient:
        recipient = settings.get("mail", {}).get("default_recipient_email", "").strip()

    payload = session.get("last_result_payload")
    if recipient and payload:
        send_result_email(recipient, payload)

    return Response(status=204)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
