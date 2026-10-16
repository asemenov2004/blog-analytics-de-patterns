-- Служебные БД и настройки для CDC. Скрипт идемпотентный: выполняется и при первом
-- старте (initdb), и сервисом pg-init на уже существующем томе.

SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SELECT 'CREATE DATABASE dwh' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dwh')\gexec

-- Debezium: в событиях UPDATE/DELETE нужен полный образ строки «до», а не только ключ.
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE articles REPLICA IDENTITY FULL;
