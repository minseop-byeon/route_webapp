from flask import Flask, render_template, request, redirect, url_for, session
import os
from datetime import datetime

app = Flask(__name__)
app.secret_key = "tax_collection_secret"

# 관리자 설정 저장 파일
SETTINGS_FILE = "settings.txt"


def load_settings():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            return f.read()
    return ""


def save_settings(data):
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        f.write(data)


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

    team = session.get("team")
    username = session.get("username")
    date = session.get("date")

    return render_template(
        "index.html",
        team=team,
        username=username,
        date=date
    )


@app.route("/optimize", methods=["POST"])
def optimize():

    addresses = request.form.get("addresses")

    if not addresses:
        return redirect(url_for("index"))

    address_list = addresses.split("\n")

    optimized_route = address_list  # 현재는 단순 반환 (추후 알고리즘 적용)

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

    return render_template(
        "admin_settings.html",
        settings=settings
    )


@app.route("/save_settings", methods=["POST"])
def save_admin_settings():

    data = request.form.get("settings")

    save_settings(data)

    return redirect(url_for("admin"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
