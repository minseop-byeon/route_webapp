from flask import Flask, render_template, request, redirect, url_for, session, jsonify
import os
import json

app = Flask(__name__)
app.secret_key = "tax_collection_secret"

SETTINGS_FILE = "settings.json"


def load_settings():
    default_settings = {
        "mail": {
            "smtp_server": "",
            "smtp_port": "",
            "sender_email": "",
            "sender_name": ""
        },
        "api": {
            "tmap_api_key": "",
            "google_maps_api_key": "",
            "kakao_api_key": ""
        },
        "user": {
            "default_team": "",
            "default_username": "",
            "default_start_location": ""
        },
        "admin": {
            "admin_password": ""
        }
    }

    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)

            for section in default_settings:
                if section not in saved:
                    saved[section] = default_settings[section]
                else:
                    for key in default_settings[section]:
                        if key not in saved[section]:
                            saved[section][key] = default_settings[section][key]
            return saved
        except Exception:
            return default_settings

    return default_settings


def save_settings(data):
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@app.route("/")
def start():
    return render_template("start.html")


@app.route("/start_process", methods=["POST"])
def start_process():
    team = request.form.get("team")
    username = request.form.get("username")
    date = request.form.get("date")

    session["team"] = team
    session["username"] = username
    session["date"] = date

    return redirect(url_for("index"))


@app.route("/index")
def index():
    return render_template(
        "index.html",
        team=session.get("team"),
        username=session.get("username"),
        date=session.get("date")
    )


@app.route("/optimize", methods=["POST"])
def optimize():
    addresses = request.form.get("addresses")

    if not addresses:
        return redirect(url_for("index"))

    address_list = [a.strip() for a in addresses.split("\n") if a.strip()]
    optimized_route = address_list

    return render_template(
        "result.html",
        route=optimized_route,
        team=session.get("team"),
        username=session.get("username"),
        date=session.get("date")
    )


@app.route("/admin")
def admin():
    settings = load_settings()
    return render_template("admin_settings.html", settings=settings)


@app.route("/save_section_settings", methods=["POST"])
def save_section_settings():
    section = request.form.get("section")
    settings = load_settings()

    if section == "mail":
        settings["mail"]["smtp_server"] = request.form.get("smtp_server", "").strip()
        settings["mail"]["smtp_port"] = request.form.get("smtp_port", "").strip()
        settings["mail"]["sender_email"] = request.form.get("sender_email", "").strip()
        settings["mail"]["sender_name"] = request.form.get("sender_name", "").strip()

    elif section == "api":
        settings["api"]["tmap_api_key"] = request.form.get("tmap_api_key", "").strip()
        settings["api"]["google_maps_api_key"] = request.form.get("google_maps_api_key", "").strip()
        settings["api"]["kakao_api_key"] = request.form.get("kakao_api_key", "").strip()

    elif section == "user":
        settings["user"]["default_team"] = request.form.get("default_team", "").strip()
        settings["user"]["default_username"] = request.form.get("default_username", "").strip()
        settings["user"]["default_start_location"] = request.form.get("default_start_location", "").strip()

    elif section == "admin":
        new_password = request.form.get("admin_password", "").strip()
        confirm_password = request.form.get("admin_password_confirm", "").strip()

        if new_password != confirm_password:
            return jsonify({
                "success": False,
                "message": "관리자 비밀번호와 비밀번호 확인이 일치하지 않습니다."
            })

        settings["admin"]["admin_password"] = new_password

    else:
        return jsonify({
            "success": False,
            "message": "잘못된 설정 구분입니다."
        })

    save_settings(settings)

    return jsonify({
        "success": True,
        "message": "변경사항이 저장되었습니다."
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
