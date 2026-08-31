"""
Utilidades compartidas por los agentes de IA (risk_agent.py y decision_agent.py).

Cada agente es un consumidor/productor de Kafka normal que:
  1. Lee un mensaje del topic de entrada.
  2. Llama a un LLM para hacer su parte del trabajo (evaluar riesgo o decidir).
  3. Escribe el resultado en el topic de salida.
  4. Emite un evento OpenLineage a Marquez describiendo esa ejecución como
     un "job" con datasets de entrada/salida — así el agente aparece como un
     nodo más en el grafo de lineage, igual que los topics y los conectores.
"""
import json
import os
import time
import uuid

import requests
from confluent_kafka import Consumer, Producer

from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import HttpConfig, HttpTransport
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "broker:29092")
LLM_API_BASE = os.environ.get("LLM_API_BASE", "https://api.openai.com/v1")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")

OPENLINEAGE_URL = os.environ.get("OPENLINEAGE_URL", "http://marquez:5000")
OPENLINEAGE_NAMESPACE = os.environ.get("OPENLINEAGE_NAMESPACE", "mortgage-lab")
OPENLINEAGE_JOB_NAME = os.environ.get("OPENLINEAGE_JOB_NAME", "unnamed-agent")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def make_consumer(group_id: str, topic: str) -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([topic])
    return c


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def produce_json(producer: Producer, topic: str, key: str, value: dict) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
    )
    producer.flush()


def call_llm(system_prompt: str, user_prompt: str) -> str:
    """
    Llama a cualquier endpoint compatible con la API de chat completions de
    OpenAI (OpenAI, Azure OpenAI vía gateway compatible, watsonx con proxy
    OpenAI-compatible, Ollama, LM Studio, etc.).
    """
    if not LLM_API_KEY and "localhost" not in LLM_API_BASE and "ollama" not in LLM_API_BASE:
        log("ADVERTENCIA: LLM_API_KEY vacío. Configúralo en .env si tu endpoint lo requiere.")

    url = f"{LLM_API_BASE.rstrip('/')}/chat/completions"
    headers = {"Content-Type": "application/json"}
    if LLM_API_KEY:
        headers["Authorization"] = f"Bearer {LLM_API_KEY}"

    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]


def parse_json_response(text: str) -> dict:
    """Los LLMs a veces envuelven el JSON en ```json ... ``` — lo limpiamos."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.split("\n", 1)[-1] if "\n" in cleaned else cleaned
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:]
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        log(f"No se pudo parsear la respuesta del LLM como JSON: {text!r}")
        raise


class LineageEmitter:
    """
    Envoltorio delgado sobre el cliente OpenLineage para emitir un evento
    START y uno COMPLETE (o FAIL) por cada mensaje procesado, con el topic
    de entrada y el de salida como datasets. Marquez los agrupa
    automáticamente en un grafo de lineage por namespace.
    """

    def __init__(self, job_name: str = None):
        self.job_name = job_name or OPENLINEAGE_JOB_NAME
        transport = HttpTransport(HttpConfig(url=OPENLINEAGE_URL))
        self.client = OpenLineageClient(transport=transport)

    def _job(self) -> Job:
        return Job(namespace=OPENLINEAGE_NAMESPACE, name=self.job_name)

    def emit(
        self,
        run_id: str,
        state: RunState,
        input_topic: str = None,
        output_topic: str = None,
    ) -> None:
        """
        Emite un evento de lineage. Se llama dos veces por mensaje procesado:
        una vez con RunState.START al empezar, y otra con RunState.COMPLETE
        (o RunState.FAIL) al terminar. Nunca lanza excepción hacia arriba —
        si Marquez no está disponible, solo se loguea y el pipeline sigue.
        """
        inputs = [Dataset(namespace="kafka", name=input_topic)] if input_topic else []
        outputs = [Dataset(namespace="kafka", name=output_topic)] if output_topic else []

        event = RunEvent(
            eventType=state,
            eventTime=time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            run=Run(runId=run_id),
            job=self._job(),
            inputs=inputs,
            outputs=outputs,
            producer="https://github.com/OWNER/mortgage-ai-streaming-lab",
        )
        try:
            self.client.emit(event)
        except Exception as exc:  # noqa: BLE001
            # El lineage nunca debe tumbar el pipeline principal — solo se loguea.
            log(f"No se pudo emitir evento OpenLineage: {exc}")

    def new_run_id(self) -> str:
        return str(uuid.uuid4())
