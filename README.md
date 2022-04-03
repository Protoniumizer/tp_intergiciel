# Team

FOSSEY Mathis

WATIER Julie


## CREATE TOPIC

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install foobar
```

## CREATE DATABASE

-- Table: covid19.global

-- DROP TABLE IF EXISTS covid19.global;

```SQL
CREATE TABLE IF NOT EXISTS covid19.global
(
    covid19_id integer NOT NULL DEFAULT nextval('covid19.global_covid19_id_seq'::regclass),
    data jsonb,
    CONSTRAINT global_pkey PRIMARY KEY (covid19_id)
)
```

## USE
