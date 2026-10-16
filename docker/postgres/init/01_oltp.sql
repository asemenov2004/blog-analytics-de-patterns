-- OLTP-источник платформы блогов. Данные заливает `blogsim bootstrap`.
-- updated_at поддерживается триггером: на нём держится инкрементальная загрузка.

CREATE TABLE IF NOT EXISTS users (
    user_id       bigint PRIMARY KEY,
    email         text        NOT NULL UNIQUE,
    first_name    text        NOT NULL,
    last_name     text        NOT NULL,
    birth_date    date,
    country       char(2),
    registered_at timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS blogs (
    blog_id       integer PRIMARY KEY,
    name          text        NOT NULL,
    owner_user_id bigint,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS articles (
    article_id   integer PRIMARY KEY,
    blog_id      integer     NOT NULL REFERENCES blogs (blog_id),
    title        text        NOT NULL,
    category     text        NOT NULL,
    published_at timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS users_updated_at_idx ON users (updated_at);
CREATE INDEX IF NOT EXISTS articles_updated_at_idx ON articles (updated_at);

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE OR REPLACE TRIGGER blogs_updated_at BEFORE UPDATE ON blogs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE OR REPLACE TRIGGER articles_updated_at BEFORE UPDATE ON articles
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
