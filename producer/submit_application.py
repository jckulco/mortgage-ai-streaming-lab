"""
Simula el frontend que envía una solicitud de hipoteca (Parte 1 del lab
original). Publica un mensaje al topic 'mortgage_applications'.

Uso:
    pip install confluent-kafka
    python producer/submit_application.py --name "John Doe" \\
        --property-value 200000 --loan-amount 150000 --annual-income 500000
"""
import argparse
import json
import time

from confluent_kafka import Producer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--email", required=True)
    parser.add_argument("--property-value", type=float, required=True)
    parser.add_argument("--loan-amount", type=float, required=True)
    parser.add_argument("--annual-income", type=float, required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="mortgage_applications")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    payload = {
        "applicant_name": args.name,
        "applicant_email": args.email,
        "property_value": args.property_value,
        "loan_amount": args.loan_amount,
        "annual_income": args.annual_income,
        "submitted_at": int(time.time() * 1000),
    }

    producer.produce(
        args.topic,
        key=args.name.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.flush()
    print(f"Solicitud enviada: {payload}")


if __name__ == "__main__":
    main()
