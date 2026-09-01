-- Least-privilege role for risk-alert-service in the strict-security profile.
--
-- The dev profile runs the service as the database superuser, which is fine for a local stack and
-- wrong for anything modelling a cloud deployment. Here the service owns its schema and nothing
-- else: no CREATEDB, no CREATEROLE, no access to any other schema (ADR-028, ADR-030).
--
-- POSTGRES_USER in the strict overlay is "postgres", not "risk_alert_service" (see
-- docker-compose.strict-security.yml): PostgreSQL bootstraps whatever POSTGRES_USER names as the
-- initdb superuser, and refuses to let that specific bootstrap role ever strip its own SUPERUSER
-- attribute ("permission denied to alter role: The bootstrap user must have the SUPERUSER
-- attribute", confirmed against postgres:16-alpine). So risk_alert_service is created here instead,
-- as a genuinely separate, non-superuser role, rather than hardened in place after the fact.
--
-- The role's password is read from the container's own POSTGRES_PASSWORD environment variable via
-- \getenv, so the secret the overlay's environment: block already carries is the one this role
-- gets, and no credential is ever written to this tracked file.
--
-- Flyway still needs DDL on this schema, because the migration runs as the service at startup.

\getenv risk_alert_password POSTGRES_PASSWORD

CREATE ROLE risk_alert_service LOGIN PASSWORD :'risk_alert_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE;

REVOKE ALL ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA IF NOT EXISTS risk_alert AUTHORIZATION risk_alert_service;
ALTER ROLE risk_alert_service SET search_path TO risk_alert;

GRANT USAGE, CREATE ON SCHEMA risk_alert TO risk_alert_service;
