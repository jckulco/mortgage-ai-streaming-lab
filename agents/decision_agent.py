"""
Agente de decisión — equivalente al Flink Statement 'mortgage_decisions' del
lab original. Consume el resultado de riesgo y decide aprobación/rechazo,
la tasa de interés, y redacta el email al solicitante, usando un LLM. Cada
ejecución se reporta a Marquez vía OpenLineage.
"""
import json

from openlineage.client.run import RunState

from common import (
    LineageEmitter,
    call_llm,
    log,
    make_consumer,
    make_producer,
    parse_json_response,
    produce_json,
)

SOURCE_TOPIC = "mortgage_validated_apps"
SINK_TOPIC = "mortgage_decisions"

SYSTEM_PROMPT = (
    "Eres un oficial de decisiones hipotecarias. A partir de la evaluación "
    "de riesgo, decide si la solicitud se aprueba o rechaza, define una tasa "
    "de interés (si aplica) y redacta el cuerpo de un email breve y "
    "profesional dirigido al solicitante explicando la decisión. Responde "
    "ÚNICAMENTE con un objeto JSON con las claves 'decision' (approved o "
    "denied), 'interest_rate' (número, o null si se rechaza) y 'email_body' "
    "(texto del correo). No incluyas texto fuera del JSON."
)


def build_user_prompt(app: dict) -> str:
    return (
        f"Solicitante: {app.get('applicant_name')}\n"
        f"Nivel de riesgo: {app.get('risk_level')}\n"
        f"Resumen del análisis de riesgo: {app.get('risk_summary')}\n"
    )


def main() -> None:
    consumer = make_consumer("ai-decision-agent-group", SOURCE_TOPIC)
    producer = make_producer()
    lineage = LineageEmitter()
    log(f"Escuchando '{SOURCE_TOPIC}' -> escribiendo en '{SINK_TOPIC}'")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log(f"Error de consumo: {msg.error()}")
                continue

            try:
                app = json.loads(msg.value())
            except json.JSONDecodeError:
                log(f"Mensaje no es JSON válido, se omite: {msg.value()!r}")
                continue

            applicant_name = app.get("applicant_name") or (
                msg.key().decode("utf-8") if msg.key() else "unknown"
            )

            run_id = lineage.new_run_id()
            lineage.emit(run_id, RunState.START, input_topic=SOURCE_TOPIC)

            try:
                llm_text = call_llm(SYSTEM_PROMPT, build_user_prompt(app))
                result = parse_json_response(llm_text)
            except Exception as exc:  # noqa: BLE001
                log(f"Fallo al decidir para {applicant_name}: {exc}")
                lineage.emit(run_id, RunState.FAIL, input_topic=SOURCE_TOPIC)
                continue

            output = {
                "applicant_name": applicant_name,
                "decision": result.get("decision", "unknown"),
                "interest_rate": result.get("interest_rate"),
                "email_body": result.get("email_body", ""),
            }
            produce_json(producer, SINK_TOPIC, applicant_name, output)
            lineage.emit(
                run_id,
                RunState.COMPLETE,
                input_topic=SOURCE_TOPIC,
                output_topic=SINK_TOPIC,
            )
            log(f"{applicant_name}: decisión={output['decision']} tasa={output['interest_rate']}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
