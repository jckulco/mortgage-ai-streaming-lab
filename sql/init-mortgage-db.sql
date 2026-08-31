-- Se ejecuta una sola vez, al crear el volumen del contenedor postgres-mortgage.

CREATE TABLE IF NOT EXISTS credit_scores (
    id              SERIAL PRIMARY KEY,
    applicant_name  VARCHAR(255) NOT NULL,
    credit_score    INT NOT NULL,
    debt_to_income  NUMERIC(5,2) NOT NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payment_history (
    id                SERIAL PRIMARY KEY,
    applicant_name    VARCHAR(255) NOT NULL,
    on_time_payments  INT NOT NULL,
    late_payments     INT NOT NULL,
    updated_at        TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO credit_scores (applicant_name, credit_score, debt_to_income) VALUES
    ('John Doe', 780, 18.50),
    ('Emmet Wisoky', 610, 42.30);

INSERT INTO payment_history (applicant_name, on_time_payments, late_payments) VALUES
    ('John Doe', 48, 0),
    ('Emmet Wisoky', 30, 6);
