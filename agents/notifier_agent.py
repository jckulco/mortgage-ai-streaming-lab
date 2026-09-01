"""
Agente de notificación — consume 'mortgage_decisions' y envía un correo al
solicitante (aprobado/rechazado) usando el 'email_body' ya redactado por el
ai-decision-agent. No requiere ningún servicio de correo externo: por
defecto apunta a Mailpit, un servidor SMTP falso que corre dentro del mismo
docker-compose y expone una bandeja de entrada web (sin enviar correos
reales a Internet). Cambiando SMTP_HOST/PORT/USER/PASSWORD en .env se puede
apuntar a un SMTP real (Gmail, SES, etc.) sin tocar código.
"""
import json
import os
import smtplib
from email.message import EmailMessage

from common import log, make_consumer

SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", "mortgage_decisions")

SMTP_HOST = os.environ.get("SMTP_HOST", "mailpit")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "1025"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "false").lower() == "true"
EMAIL_FROM = os.environ.get("EMAIL_FROM", "River Bank <notificaciones@riverbank.demo>")

DECISION_SUBJECTS = {
    "approved": "✅ Tu solicitud de hipoteca fue aprobada",
    "denied": "❌ Actualización sobre tu solicitud de hipoteca",
}


def send_email(to_addr: str, applicant_name: str, decision: dict) -> None:
    subject = DECISION_SUBJECTS.get(
        decision.get("decision"), "Actualización sobre tu solicitud de hipoteca"
    )
    body = decision.get("email_body") or (
        f"Hola {applicant_name}, tu solicitud fue procesada con resultado: "
        f"{decision.get('decision')}."
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = to_addr
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
        if SMTP_USE_TLS:
            smtp.starttls()
        if SMTP_USER:
            smtp.login(SMTP_USER, SMTP_PASSWORD)
        smtp.send_message(msg)


def main() -> None:
    consumer = make_consumer("ai-email-notifier-group", SOURCE_TOPIC)
    log(f"Escuchando '{SOURCE_TOPIC}' -> enviando correos vía {SMTP_HOST}:{SMTP_PORT}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log(f"Error de consumo: {msg.error()}")
                continue

            try:
                decision = json.loads(msg.value())
            except json.JSONDecodeError:
                log(f"Mensaje no es JSON válido, se omite: {msg.value()!r}")
                continue

            applicant_name = decision.get("applicant_name") or "solicitante"
            to_addr = decision.get("applicant_email")

            if not to_addr:
                log(f"{applicant_name}: sin applicant_email en el mensaje, se omite notificación.")
                continue

            try:
                send_email(to_addr, applicant_name, decision)
                log(f"{applicant_name}: correo enviado a {to_addr} (decisión={decision.get('decision')})")
            except Exception as exc:  # noqa: BLE001
                log(f"{applicant_name}: fallo al enviar correo a {to_addr}: {exc}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
