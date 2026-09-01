"""
Agente de riesgo — equivalente al Flink Statement 'mortgage-risk-agent' del
lab original de Confluent Cloud. Consume el stream enriquecido y clasifica
el riesgo del solicitante como low/medium/high usando un LLM. Cada
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

SOURCE_TOPIC = "enriched_mortgage_applications"
SINK_TOPIC = "mortgage_validated_apps"

SYSTEM_PROMPT = (
    "Eres un analista de riesgo hipotecario. Analiza los datos del "
    "solicitante y responde ÚNICAMENTE con un objeto JSON con las claves "
    "'risk_level' (uno de: low, medium, high) y 'risk_summary' "
    "(explicación breve en una o dos frases). No incluyas texto fuera del JSON."
)


def build_user_prompt(app: dict) -> str:
    return (
        f"Solicitante: {app.get('applicant_name')}\n"
        f"Valor de la propiedad: {app.get('property_value')}\n"
        f"Monto del préstamo: {app.get('loan_amount')}\n"
        f"Ingreso anual: {app.get('annual_income')}\n"
        f"Score crediticio: {app.get('credit_score')}\n"
        f"Relación deuda/ingreso: {app.get('debt_to_income')}\n"
        f"Pagos a tiempo: {app.get('on_time_payments')}\n"
        f"Pagos atrasados: {app.get('late_payments')}\n"
    )


def main() -> None:
    consumer = make_consumer("ai-risk-agent-group", SOURCE_TOPIC)
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
                log(f"Fallo al evaluar riesgo para {applicant_name}: {exc}")
                lineage.emit(run_id, RunState.FAIL, input_topic=SOURCE_TOPIC)
                continue

            output = {
                "applicant_name": applicant_name,
                "applicant_email": app.get("applicant_email"),
                "risk_level": result.get("risk_level", "unknown"),
                "risk_summary": result.get("risk_summary", ""),
            }
            produce_json(producer, SINK_TOPIC, applicant_name, output)
            lineage.emit(
                run_id,
                RunState.COMPLETE,
                input_topic=SOURCE_TOPIC,
                output_topic=SINK_TOPIC,
            )
            log(f"{applicant_name}: riesgo={output['risk_level']}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
