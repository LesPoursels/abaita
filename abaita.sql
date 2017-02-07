--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: abaita; Type: DATABASE; Schema: -; Owner: $USER
--

CREATE DATABASE abaita WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8';


ALTER DATABASE abaita OWNER TO $USER;

\connect abaita

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: abaita; Type: TABLE; Schema: public; Owner: $USER; Tablespace: 
--

CREATE TABLE abaita (
    date date NOT NULL,
    "time" time without time zone NOT NULL,
    badge character varying NOT NULL,
    uscita boolean NOT NULL,
    raw character varying
);


ALTER TABLE abaita OWNER TO $USER;

--
-- Name: abaita_pkey; Type: CONSTRAINT; Schema: public; Owner: $USER; Tablespace: 
--

ALTER TABLE ONLY abaita
    ADD CONSTRAINT abaita_pkey PRIMARY KEY (date, "time", badge);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

