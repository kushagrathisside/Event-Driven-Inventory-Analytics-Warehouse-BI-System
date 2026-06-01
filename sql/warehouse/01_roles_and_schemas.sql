CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{{FDW_READER_USER}}') THEN
        EXECUTE format(
            'CREATE ROLE %I LOGIN PASSWORD %L',
            '{{FDW_READER_USER}}',
            '{{FDW_READER_PASSWORD}}'
        );
    ELSE
        EXECUTE format(
            'ALTER ROLE %I WITH LOGIN PASSWORD %L',
            '{{FDW_READER_USER}}',
            '{{FDW_READER_PASSWORD}}'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{{SPARK_WRITER_USER}}') THEN
        EXECUTE format(
            'CREATE ROLE %I LOGIN PASSWORD %L',
            '{{SPARK_WRITER_USER}}',
            '{{SPARK_WRITER_PASSWORD}}'
        );
    ELSE
        EXECUTE format(
            'ALTER ROLE %I WITH LOGIN PASSWORD %L',
            '{{SPARK_WRITER_USER}}',
            '{{SPARK_WRITER_PASSWORD}}'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{{ANALYTICS_READER_USER}}') THEN
        EXECUTE format(
            'CREATE ROLE %I LOGIN PASSWORD %L',
            '{{ANALYTICS_READER_USER}}',
            '{{ANALYTICS_READER_PASSWORD}}'
        );
    ELSE
        EXECUTE format(
            'ALTER ROLE %I WITH LOGIN PASSWORD %L',
            '{{ANALYTICS_READER_USER}}',
            '{{ANALYTICS_READER_PASSWORD}}'
        );
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS master;
CREATE SCHEMA IF NOT EXISTS analytics;

REVOKE ALL ON SCHEMA master FROM PUBLIC;
REVOKE ALL ON SCHEMA analytics FROM PUBLIC;

GRANT USAGE ON SCHEMA analytics TO {{SPARK_WRITER_USER}};
GRANT USAGE ON SCHEMA analytics TO {{ANALYTICS_READER_USER}};
