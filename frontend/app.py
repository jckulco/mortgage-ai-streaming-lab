"""
Frontend web de la demo — formulario de solicitud de hipoteca.
Equivalente visual al Parte 1 del lab original ("River Bank" en el PDF).
Al enviar el formulario, publica un mensaje JSON al topic
'mortgage_applications', exactamente igual que producer/submit_application.py,
solo que desde una interfaz web en vez de la línea de comandos.
"""
import json
import os
import time

from confluent_kafka import Producer
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "broker:29092")
TOPIC = os.environ.get("APPLICATIONS_TOPIC", "mortgage_applications")

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/submit", methods=["POST"])
def submit():
    data = request.get_json(force=True)

    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip()
    try:
        property_value = float(data.get("property_value"))
        loan_amount = float(data.get("loan_amount"))
        annual_income = float(data.get("annual_income"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Todos los montos deben ser números."}), 400

    if not name:
        return jsonify({"ok": False, "error": "El nombre completo es obligatorio."}), 400

    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "El email es obligatorio y debe ser válido."}), 400

    payload = {
        "applicant_name": name,
        "applicant_email": email,
        "property_value": property_value,
        "loan_amount": loan_amount,
        "annual_income": annual_income,
        "submitted_at": int(time.time() * 1000),
    }

    try:
        producer.produce(
            TOPIC,
            key=name.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
        )
        producer.flush(timeout=10)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": f"No se pudo publicar en Kafka: {exc}"}), 500

    return jsonify({"ok": True, "payload": payload})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)
