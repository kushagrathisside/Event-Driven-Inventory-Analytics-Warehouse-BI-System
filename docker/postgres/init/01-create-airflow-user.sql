\connect airflow

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
        CREATE ROLE airflow LOGIN PASSWORD 'airflow';
    ELSE
        ALTER ROLE airflow WITH LOGIN PASSWORD 'airflow';
    END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
