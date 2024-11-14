--
-- PostgreSQL database dump
--

-- Dumped from database version 14.4
-- Dumped by pg_dump version 14.13 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


ALTER TABLE public.alembic_version OWNER TO postgres;

--
-- Name: applyworkflow; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.applyworkflow (
    start_timestamp timestamp with time zone NOT NULL,
    end_timestamp timestamp with time zone,
    worker_init character varying,
    id integer NOT NULL,
    project_id integer,
    input_dataset_id integer,
    output_dataset_id integer,
    workflow_id integer,
    working_dir character varying,
    working_dir_user character varying,
    status character varying NOT NULL,
    log character varying,
    first_task_index integer NOT NULL,
    last_task_index integer NOT NULL,
    workflow_dump json NOT NULL,
    user_email character varying NOT NULL,
    input_dataset_dump json NOT NULL,
    output_dataset_dump json NOT NULL,
    slurm_account character varying,
    project_dump json NOT NULL
);


ALTER TABLE public.applyworkflow OWNER TO postgres;

--
-- Name: applyworkflow_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.applyworkflow_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.applyworkflow_id_seq OWNER TO postgres;

--
-- Name: applyworkflow_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.applyworkflow_id_seq OWNED BY public.applyworkflow.id;


--
-- Name: dataset; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dataset (
    meta json,
    name character varying NOT NULL,
    type character varying,
    read_only boolean NOT NULL,
    id integer NOT NULL,
    project_id integer NOT NULL,
    history json DEFAULT '[]'::json NOT NULL,
    timestamp_created timestamp with time zone NOT NULL
);


ALTER TABLE public.dataset OWNER TO postgres;

--
-- Name: dataset_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dataset_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.dataset_id_seq OWNER TO postgres;

--
-- Name: dataset_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.dataset_id_seq OWNED BY public.dataset.id;


--
-- Name: datasetv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.datasetv2 (
    id integer NOT NULL,
    name character varying NOT NULL,
    project_id integer NOT NULL,
    history json DEFAULT '[]'::json NOT NULL,
    timestamp_created timestamp with time zone NOT NULL,
    zarr_dir character varying NOT NULL,
    images json DEFAULT '[]'::json NOT NULL,
    filters json DEFAULT '{"attributes": {}, "types": {}}'::json NOT NULL
);


ALTER TABLE public.datasetv2 OWNER TO postgres;

--
-- Name: datasetv2_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.datasetv2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.datasetv2_id_seq OWNER TO postgres;

--
-- Name: datasetv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.datasetv2_id_seq OWNED BY public.datasetv2.id;


--
-- Name: jobv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.jobv2 (
    id integer NOT NULL,
    project_id integer,
    workflow_id integer,
    dataset_id integer,
    user_email character varying NOT NULL,
    slurm_account character varying,
    dataset_dump json NOT NULL,
    workflow_dump json NOT NULL,
    project_dump json NOT NULL,
    worker_init character varying,
    working_dir character varying,
    working_dir_user character varying,
    first_task_index integer NOT NULL,
    last_task_index integer NOT NULL,
    start_timestamp timestamp with time zone NOT NULL,
    end_timestamp timestamp with time zone,
    status character varying NOT NULL,
    log character varying
);


ALTER TABLE public.jobv2 OWNER TO postgres;

--
-- Name: jobv2_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.jobv2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.jobv2_id_seq OWNER TO postgres;

--
-- Name: jobv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.jobv2_id_seq OWNED BY public.jobv2.id;


--
-- Name: linkusergroup; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.linkusergroup (
    group_id integer NOT NULL,
    user_id integer NOT NULL,
    timestamp_created timestamp with time zone DEFAULT '2000-01-01 01:00:00+01'::timestamp with time zone NOT NULL
);


ALTER TABLE public.linkusergroup OWNER TO postgres;

--
-- Name: linkuserproject; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.linkuserproject (
    project_id integer NOT NULL,
    user_id integer NOT NULL
);


ALTER TABLE public.linkuserproject OWNER TO postgres;

--
-- Name: linkuserprojectv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.linkuserprojectv2 (
    project_id integer NOT NULL,
    user_id integer NOT NULL
);


ALTER TABLE public.linkuserprojectv2 OWNER TO postgres;

--
-- Name: oauthaccount; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.oauthaccount (
    id integer NOT NULL,
    user_id integer NOT NULL,
    oauth_name character varying NOT NULL,
    access_token character varying NOT NULL,
    expires_at integer,
    refresh_token character varying,
    account_id character varying NOT NULL,
    account_email character varying NOT NULL
);


ALTER TABLE public.oauthaccount OWNER TO postgres;

--
-- Name: oauthaccount_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.oauthaccount_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.oauthaccount_id_seq OWNER TO postgres;

--
-- Name: oauthaccount_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.oauthaccount_id_seq OWNED BY public.oauthaccount.id;


--
-- Name: project; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.project (
    name character varying NOT NULL,
    read_only boolean NOT NULL,
    id integer NOT NULL,
    timestamp_created timestamp with time zone NOT NULL
);


ALTER TABLE public.project OWNER TO postgres;

--
-- Name: project_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.project_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.project_id_seq OWNER TO postgres;

--
-- Name: project_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.project_id_seq OWNED BY public.project.id;


--
-- Name: projectv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.projectv2 (
    id integer NOT NULL,
    name character varying NOT NULL,
    timestamp_created timestamp with time zone NOT NULL
);


ALTER TABLE public.projectv2 OWNER TO postgres;

--
-- Name: projectv2_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.projectv2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.projectv2_id_seq OWNER TO postgres;

--
-- Name: projectv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.projectv2_id_seq OWNED BY public.projectv2.id;


--
-- Name: resource; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.resource (
    path character varying NOT NULL,
    id integer NOT NULL,
    dataset_id integer NOT NULL
);


ALTER TABLE public.resource OWNER TO postgres;

--
-- Name: resource_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.resource_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.resource_id_seq OWNER TO postgres;

--
-- Name: resource_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.resource_id_seq OWNED BY public.resource.id;


--
-- Name: state; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.state (
    data json,
    "timestamp" timestamp with time zone,
    id integer NOT NULL
);


ALTER TABLE public.state OWNER TO postgres;

--
-- Name: state_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.state_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.state_id_seq OWNER TO postgres;

--
-- Name: state_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.state_id_seq OWNED BY public.state.id;


--
-- Name: task; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.task (
    meta json,
    source character varying NOT NULL,
    id integer NOT NULL,
    name character varying NOT NULL,
    command character varying NOT NULL,
    input_type character varying NOT NULL,
    output_type character varying NOT NULL,
    owner character varying,
    version character varying,
    args_schema json,
    args_schema_version character varying,
    docs_info character varying,
    docs_link character varying
);


ALTER TABLE public.task OWNER TO postgres;

--
-- Name: task_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.task_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.task_id_seq OWNER TO postgres;

--
-- Name: task_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.task_id_seq OWNED BY public.task.id;


--
-- Name: taskgroupactivityv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.taskgroupactivityv2 (
    id integer NOT NULL,
    user_id integer NOT NULL,
    taskgroupv2_id integer,
    timestamp_started timestamp with time zone NOT NULL,
    pkg_name character varying NOT NULL,
    version character varying NOT NULL,
    status character varying NOT NULL,
    action character varying NOT NULL,
    log character varying,
    timestamp_ended timestamp with time zone
);


ALTER TABLE public.taskgroupactivityv2 OWNER TO postgres;

--
-- Name: taskgroupactivityv2_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.taskgroupactivityv2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.taskgroupactivityv2_id_seq OWNER TO postgres;

--
-- Name: taskgroupactivityv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.taskgroupactivityv2_id_seq OWNED BY public.taskgroupactivityv2.id;


--
-- Name: taskgroupv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.taskgroupv2 (
    id integer NOT NULL,
    user_id integer NOT NULL,
    user_group_id integer,
    origin character varying NOT NULL,
    pkg_name character varying NOT NULL,
    version character varying,
    python_version character varying,
    path character varying,
    venv_path character varying,
    wheel_path character varying,
    pip_extras character varying,
    pinned_package_versions json DEFAULT '{}'::json,
    active boolean NOT NULL,
    timestamp_created timestamp with time zone NOT NULL,
    pip_freeze character varying,
    "venv_size_in_kB" integer,
    venv_file_number integer
);


ALTER TABLE public.taskgroupv2 OWNER TO postgres;

--
-- Name: taskgroupv2_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.taskgroupv2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.taskgroupv2_id_seq OWNER TO postgres;

--
-- Name: taskgroupv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.taskgroupv2_id_seq OWNED BY public.taskgroupv2.id;


--
-- Name: taskv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.taskv2 (
    id integer NOT NULL,
    name character varying NOT NULL,
    type character varying NOT NULL,
    command_non_parallel character varying,
    command_parallel character varying,
    source character varying,
    meta_non_parallel json DEFAULT '{}'::json NOT NULL,
    meta_parallel json DEFAULT '{}'::json NOT NULL,
    version character varying,
    args_schema_non_parallel json,
    args_schema_parallel json,
    args_schema_version character varying,
    docs_info character varying,
    docs_link character varying,
    input_types json,
    output_types json,
    taskgroupv2_id integer NOT NULL,
    category character varying,
    modality character varying,
    authors character varying,
    tags json DEFAULT '[]'::json NOT NULL
);


ALTER TABLE public.taskv2 OWNER TO postgres;

--
-- Name: taskv2_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.taskv2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.taskv2_id_seq OWNER TO postgres;

--
-- Name: taskv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.taskv2_id_seq OWNED BY public.taskv2.id;


--
-- Name: user_oauth; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.user_oauth (
    id integer NOT NULL,
    email character varying NOT NULL,
    hashed_password character varying NOT NULL,
    is_active boolean NOT NULL,
    is_superuser boolean NOT NULL,
    is_verified boolean NOT NULL,
    username character varying,
    user_settings_id integer
);


ALTER TABLE public.user_oauth OWNER TO postgres;

--
-- Name: user_oauth_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.user_oauth_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.user_oauth_id_seq OWNER TO postgres;

--
-- Name: user_oauth_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.user_oauth_id_seq OWNED BY public.user_oauth.id;


--
-- Name: user_settings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.user_settings (
    id integer NOT NULL,
    slurm_accounts json DEFAULT '[]'::json NOT NULL,
    ssh_host character varying,
    ssh_username character varying,
    ssh_private_key_path character varying,
    ssh_tasks_dir character varying,
    ssh_jobs_dir character varying,
    slurm_user character varying,
    cache_dir character varying,
    project_dir character varying
);


ALTER TABLE public.user_settings OWNER TO postgres;

--
-- Name: user_settings_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.user_settings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.user_settings_id_seq OWNER TO postgres;

--
-- Name: user_settings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.user_settings_id_seq OWNED BY public.user_settings.id;


--
-- Name: usergroup; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.usergroup (
    id integer NOT NULL,
    name character varying NOT NULL,
    timestamp_created timestamp with time zone NOT NULL,
    viewer_paths json DEFAULT '[]'::json NOT NULL
);


ALTER TABLE public.usergroup OWNER TO postgres;

--
-- Name: usergroup_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.usergroup_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.usergroup_id_seq OWNER TO postgres;

--
-- Name: usergroup_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.usergroup_id_seq OWNED BY public.usergroup.id;


--
-- Name: workflow; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.workflow (
    name character varying NOT NULL,
    id integer NOT NULL,
    project_id integer NOT NULL,
    timestamp_created timestamp with time zone NOT NULL
);


ALTER TABLE public.workflow OWNER TO postgres;

--
-- Name: workflow_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.workflow_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.workflow_id_seq OWNER TO postgres;

--
-- Name: workflow_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.workflow_id_seq OWNED BY public.workflow.id;


--
-- Name: workflowtask; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.workflowtask (
    meta json,
    args json,
    id integer NOT NULL,
    workflow_id integer NOT NULL,
    task_id integer NOT NULL,
    "order" integer
);


ALTER TABLE public.workflowtask OWNER TO postgres;

--
-- Name: workflowtask_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.workflowtask_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.workflowtask_id_seq OWNER TO postgres;

--
-- Name: workflowtask_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.workflowtask_id_seq OWNED BY public.workflowtask.id;


--
-- Name: workflowtaskv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.workflowtaskv2 (
    id integer NOT NULL,
    workflow_id integer NOT NULL,
    "order" integer,
    meta_parallel json,
    meta_non_parallel json,
    args_parallel json,
    args_non_parallel json,
    input_filters json DEFAULT '{"attributes": {}, "types": {}}'::json NOT NULL,
    task_type character varying NOT NULL,
    task_id integer NOT NULL
);


ALTER TABLE public.workflowtaskv2 OWNER TO postgres;

--
-- Name: workflowtaskv2_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.workflowtaskv2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.workflowtaskv2_id_seq OWNER TO postgres;

--
-- Name: workflowtaskv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.workflowtaskv2_id_seq OWNED BY public.workflowtaskv2.id;


--
-- Name: workflowv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.workflowv2 (
    id integer NOT NULL,
    name character varying NOT NULL,
    project_id integer NOT NULL,
    timestamp_created timestamp with time zone NOT NULL
);


ALTER TABLE public.workflowv2 OWNER TO postgres;

--
-- Name: workflowv2_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.workflowv2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.workflowv2_id_seq OWNER TO postgres;

--
-- Name: workflowv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.workflowv2_id_seq OWNED BY public.workflowv2.id;


--
-- Name: applyworkflow id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.applyworkflow ALTER COLUMN id SET DEFAULT nextval('public.applyworkflow_id_seq'::regclass);


--
-- Name: dataset id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dataset ALTER COLUMN id SET DEFAULT nextval('public.dataset_id_seq'::regclass);


--
-- Name: datasetv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.datasetv2 ALTER COLUMN id SET DEFAULT nextval('public.datasetv2_id_seq'::regclass);


--
-- Name: jobv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.jobv2 ALTER COLUMN id SET DEFAULT nextval('public.jobv2_id_seq'::regclass);


--
-- Name: oauthaccount id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.oauthaccount ALTER COLUMN id SET DEFAULT nextval('public.oauthaccount_id_seq'::regclass);


--
-- Name: project id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.project ALTER COLUMN id SET DEFAULT nextval('public.project_id_seq'::regclass);


--
-- Name: projectv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.projectv2 ALTER COLUMN id SET DEFAULT nextval('public.projectv2_id_seq'::regclass);


--
-- Name: resource id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.resource ALTER COLUMN id SET DEFAULT nextval('public.resource_id_seq'::regclass);


--
-- Name: state id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.state ALTER COLUMN id SET DEFAULT nextval('public.state_id_seq'::regclass);


--
-- Name: task id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.task ALTER COLUMN id SET DEFAULT nextval('public.task_id_seq'::regclass);


--
-- Name: taskgroupactivityv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupactivityv2 ALTER COLUMN id SET DEFAULT nextval('public.taskgroupactivityv2_id_seq'::regclass);


--
-- Name: taskgroupv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupv2 ALTER COLUMN id SET DEFAULT nextval('public.taskgroupv2_id_seq'::regclass);


--
-- Name: taskv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskv2 ALTER COLUMN id SET DEFAULT nextval('public.taskv2_id_seq'::regclass);


--
-- Name: user_oauth id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_oauth ALTER COLUMN id SET DEFAULT nextval('public.user_oauth_id_seq'::regclass);


--
-- Name: user_settings id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_settings ALTER COLUMN id SET DEFAULT nextval('public.user_settings_id_seq'::regclass);


--
-- Name: usergroup id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.usergroup ALTER COLUMN id SET DEFAULT nextval('public.usergroup_id_seq'::regclass);


--
-- Name: workflow id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflow ALTER COLUMN id SET DEFAULT nextval('public.workflow_id_seq'::regclass);


--
-- Name: workflowtask id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtask ALTER COLUMN id SET DEFAULT nextval('public.workflowtask_id_seq'::regclass);


--
-- Name: workflowtaskv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtaskv2 ALTER COLUMN id SET DEFAULT nextval('public.workflowtaskv2_id_seq'::regclass);


--
-- Name: workflowv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowv2 ALTER COLUMN id SET DEFAULT nextval('public.workflowv2_id_seq'::regclass);


--
-- Data for Name: alembic_version; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.alembic_version (version_num) FROM stdin;
3082479ac4ea
\.


--
-- Data for Name: applyworkflow; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.applyworkflow (start_timestamp, end_timestamp, worker_init, id, project_id, input_dataset_id, output_dataset_id, workflow_id, working_dir, working_dir_user, status, log, first_task_index, last_task_index, workflow_dump, user_email, input_dataset_dump, output_dataset_dump, slurm_account, project_dump) FROM stdin;
\.


--
-- Data for Name: dataset; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.dataset (meta, name, type, read_only, id, project_id, history, timestamp_created) FROM stdin;
\.


--
-- Data for Name: datasetv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.datasetv2 (id, name, project_id, history, timestamp_created, zarr_dir, images, filters) FROM stdin;
\.


--
-- Data for Name: jobv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.jobv2 (id, project_id, workflow_id, dataset_id, user_email, slurm_account, dataset_dump, workflow_dump, project_dump, worker_init, working_dir, working_dir_user, first_task_index, last_task_index, start_timestamp, end_timestamp, status, log) FROM stdin;
\.


--
-- Data for Name: linkusergroup; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.linkusergroup (group_id, user_id, timestamp_created) FROM stdin;
1	1	2024-11-14 13:00:07.980829+01
1	2	2024-11-14 13:00:37.744962+01
1	3	2024-11-14 13:00:39.452239+01
\.


--
-- Data for Name: linkuserproject; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.linkuserproject (project_id, user_id) FROM stdin;
\.


--
-- Data for Name: linkuserprojectv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.linkuserprojectv2 (project_id, user_id) FROM stdin;
\.


--
-- Data for Name: oauthaccount; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.oauthaccount (id, user_id, oauth_name, access_token, expires_at, refresh_token, account_id, account_email) FROM stdin;
2	3	openid	eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg3YWE1MWE1N2YzMzRhYzRmZjg5ODMxZjM3ODAwZDgyYTBmM2NmNjQifQ.eyJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjU1NTYvZGV4Iiwic3ViIjoiQ2cwd0xUTTROUzB5T0RBNE9TMHdFZ1J0YjJOciIsImF1ZCI6ImNsaWVudF90ZXN0X2lkIiwiZXhwIjoxNzMxNjcyMDM5LCJpYXQiOjE3MzE1ODU2MzksImF0X2hhc2giOiJhZFdrNmJNLVlYNVhjUlpVQ3lZTGRnIiwiZW1haWwiOiJraWxnb3JlQGtpbGdvcmUudHJvdXQiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZX0.Oq16p5kTRcAICTcIZwM1mdJH7Jy71VS_awah3zcHb8dhs3U4SV_W1280JGAQfqRxUP3gOcBLn0naGOsLSIJgtun2xSWldDF2snLhPBefNiMsEOzCLSs1Fw4S73rXFuGKO8ug5Uq30djOrh6RNUO9tsBR9OjkbQ0XElSxeWu5K1xyaBwAYeatYuTM5XkOKfhMJGWU3T1palipEUM8o8AY5gciVi5KV0XVw7vzKzcI2EbVnFena_OF1KrL6stgRsNT4_4Rp0U0RwGGFvblZSxOHwUJKeMZ6TlW79SdytUpbY2xPJgtfQDiNU12LNz6Hzqhmzmep-lB-Jq7V05pMZ18cg	1731672038	\N	Cg0wLTM4NS0yODA4OS0wEgRtb2Nr	kilgore@kilgore.trout
\.


--
-- Data for Name: project; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.project (name, read_only, id, timestamp_created) FROM stdin;
\.


--
-- Data for Name: projectv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.projectv2 (id, name, timestamp_created) FROM stdin;
\.


--
-- Data for Name: resource; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.resource (path, id, dataset_id) FROM stdin;
\.


--
-- Data for Name: state; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.state (data, "timestamp", id) FROM stdin;
\.


--
-- Data for Name: task; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.task (meta, source, id, name, command, input_type, output_type, owner, version, args_schema, args_schema_version, docs_info, docs_link) FROM stdin;
\.


--
-- Data for Name: taskgroupactivityv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.taskgroupactivityv2 (id, user_id, taskgroupv2_id, timestamp_started, pkg_name, version, status, action, log, timestamp_ended) FROM stdin;
\.


--
-- Data for Name: taskgroupv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.taskgroupv2 (id, user_id, user_group_id, origin, pkg_name, version, python_version, path, venv_path, wheel_path, pip_extras, pinned_package_versions, active, timestamp_created, pip_freeze, "venv_size_in_kB", venv_file_number) FROM stdin;
\.


--
-- Data for Name: taskv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.taskv2 (id, name, type, command_non_parallel, command_parallel, source, meta_non_parallel, meta_parallel, version, args_schema_non_parallel, args_schema_parallel, args_schema_version, docs_info, docs_link, input_types, output_types, taskgroupv2_id, category, modality, authors, tags) FROM stdin;
\.


--
-- Data for Name: user_oauth; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.user_oauth (id, email, hashed_password, is_active, is_superuser, is_verified, username, user_settings_id) FROM stdin;
1	admin@fractal.xy	$2b$12$kWrjFGSDHGvhL7omawD2E.YCJ1gqJgiWs32HWbClER97pe8Fu8bbO	t	t	t	admin	1
2	kilgore@fractal.xy	$2b$12$.mP7DF3lwAmVHaXfMyLvruF5KiMeIJASXx0EsIezeKbgVj/4uPFwK	t	f	f	\N	2
3	kilgore@kilgore.trout	$2b$12$nqFGzQioYU6o6zmOqIBkt.fIXjzdBKTdn1xAWVNL/rJ9ONf7lUQTi	t	f	f	\N	3
\.


--
-- Data for Name: user_settings; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.user_settings (id, slurm_accounts, ssh_host, ssh_username, ssh_private_key_path, ssh_tasks_dir, ssh_jobs_dir, slurm_user, cache_dir, project_dir) FROM stdin;
1	[]	\N	\N	\N	\N	\N	\N	\N	\N
2	[]	\N	\N	\N	\N	\N	\N	\N	\N
3	[]	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: usergroup; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.usergroup (id, name, timestamp_created, viewer_paths) FROM stdin;
1	All	2024-11-14 13:00:07.716621+01	[]
\.


--
-- Data for Name: workflow; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.workflow (name, id, project_id, timestamp_created) FROM stdin;
\.


--
-- Data for Name: workflowtask; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.workflowtask (meta, args, id, workflow_id, task_id, "order") FROM stdin;
\.


--
-- Data for Name: workflowtaskv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.workflowtaskv2 (id, workflow_id, "order", meta_parallel, meta_non_parallel, args_parallel, args_non_parallel, input_filters, task_type, task_id) FROM stdin;
\.


--
-- Data for Name: workflowv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.workflowv2 (id, name, project_id, timestamp_created) FROM stdin;
\.


--
-- Name: applyworkflow_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.applyworkflow_id_seq', 1, false);


--
-- Name: dataset_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.dataset_id_seq', 1, false);


--
-- Name: datasetv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.datasetv2_id_seq', 1, false);


--
-- Name: jobv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.jobv2_id_seq', 1, false);


--
-- Name: oauthaccount_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.oauthaccount_id_seq', 2, true);


--
-- Name: project_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.project_id_seq', 1, false);


--
-- Name: projectv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.projectv2_id_seq', 1, false);


--
-- Name: resource_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.resource_id_seq', 1, false);


--
-- Name: state_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.state_id_seq', 1, false);


--
-- Name: task_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.task_id_seq', 1, false);


--
-- Name: taskgroupactivityv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.taskgroupactivityv2_id_seq', 1, false);


--
-- Name: taskgroupv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.taskgroupv2_id_seq', 1, false);


--
-- Name: taskv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.taskv2_id_seq', 1, false);


--
-- Name: user_oauth_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.user_oauth_id_seq', 3, true);


--
-- Name: user_settings_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.user_settings_id_seq', 3, true);


--
-- Name: usergroup_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.usergroup_id_seq', 1, true);


--
-- Name: workflow_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.workflow_id_seq', 1, false);


--
-- Name: workflowtask_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.workflowtask_id_seq', 1, false);


--
-- Name: workflowtaskv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.workflowtaskv2_id_seq', 1, false);


--
-- Name: workflowv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.workflowv2_id_seq', 1, false);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: applyworkflow pk_applyworkflow; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.applyworkflow
    ADD CONSTRAINT pk_applyworkflow PRIMARY KEY (id);


--
-- Name: dataset pk_dataset; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dataset
    ADD CONSTRAINT pk_dataset PRIMARY KEY (id);


--
-- Name: datasetv2 pk_datasetv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.datasetv2
    ADD CONSTRAINT pk_datasetv2 PRIMARY KEY (id);


--
-- Name: jobv2 pk_jobv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.jobv2
    ADD CONSTRAINT pk_jobv2 PRIMARY KEY (id);


--
-- Name: linkusergroup pk_linkusergroup; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkusergroup
    ADD CONSTRAINT pk_linkusergroup PRIMARY KEY (group_id, user_id);


--
-- Name: linkuserproject pk_linkuserproject; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkuserproject
    ADD CONSTRAINT pk_linkuserproject PRIMARY KEY (project_id, user_id);


--
-- Name: linkuserprojectv2 pk_linkuserprojectv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkuserprojectv2
    ADD CONSTRAINT pk_linkuserprojectv2 PRIMARY KEY (project_id, user_id);


--
-- Name: oauthaccount pk_oauthaccount; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.oauthaccount
    ADD CONSTRAINT pk_oauthaccount PRIMARY KEY (id);


--
-- Name: project pk_project; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.project
    ADD CONSTRAINT pk_project PRIMARY KEY (id);


--
-- Name: projectv2 pk_projectv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.projectv2
    ADD CONSTRAINT pk_projectv2 PRIMARY KEY (id);


--
-- Name: resource pk_resource; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.resource
    ADD CONSTRAINT pk_resource PRIMARY KEY (id);


--
-- Name: state pk_state; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.state
    ADD CONSTRAINT pk_state PRIMARY KEY (id);


--
-- Name: task pk_task; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.task
    ADD CONSTRAINT pk_task PRIMARY KEY (id);


--
-- Name: taskgroupactivityv2 pk_taskgroupactivityv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupactivityv2
    ADD CONSTRAINT pk_taskgroupactivityv2 PRIMARY KEY (id);


--
-- Name: taskgroupv2 pk_taskgroupv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupv2
    ADD CONSTRAINT pk_taskgroupv2 PRIMARY KEY (id);


--
-- Name: taskv2 pk_taskv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskv2
    ADD CONSTRAINT pk_taskv2 PRIMARY KEY (id);


--
-- Name: user_oauth pk_user_oauth; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_oauth
    ADD CONSTRAINT pk_user_oauth PRIMARY KEY (id);


--
-- Name: user_settings pk_user_settings; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_settings
    ADD CONSTRAINT pk_user_settings PRIMARY KEY (id);


--
-- Name: usergroup pk_usergroup; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.usergroup
    ADD CONSTRAINT pk_usergroup PRIMARY KEY (id);


--
-- Name: workflow pk_workflow; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflow
    ADD CONSTRAINT pk_workflow PRIMARY KEY (id);


--
-- Name: workflowtask pk_workflowtask; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtask
    ADD CONSTRAINT pk_workflowtask PRIMARY KEY (id);


--
-- Name: workflowtaskv2 pk_workflowtaskv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtaskv2
    ADD CONSTRAINT pk_workflowtaskv2 PRIMARY KEY (id);


--
-- Name: workflowv2 pk_workflowv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowv2
    ADD CONSTRAINT pk_workflowv2 PRIMARY KEY (id);


--
-- Name: task uq_task_source; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.task
    ADD CONSTRAINT uq_task_source UNIQUE (source);


--
-- Name: usergroup uq_usergroup_name; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.usergroup
    ADD CONSTRAINT uq_usergroup_name UNIQUE (name);


--
-- Name: ix_oauthaccount_account_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_oauthaccount_account_id ON public.oauthaccount USING btree (account_id);


--
-- Name: ix_oauthaccount_oauth_name; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_oauthaccount_oauth_name ON public.oauthaccount USING btree (oauth_name);


--
-- Name: ix_user_oauth_email; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_user_oauth_email ON public.user_oauth USING btree (email);


--
-- Name: applyworkflow fk_applyworkflow_input_dataset_id_dataset; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.applyworkflow
    ADD CONSTRAINT fk_applyworkflow_input_dataset_id_dataset FOREIGN KEY (input_dataset_id) REFERENCES public.dataset(id);


--
-- Name: applyworkflow fk_applyworkflow_output_dataset_id_dataset; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.applyworkflow
    ADD CONSTRAINT fk_applyworkflow_output_dataset_id_dataset FOREIGN KEY (output_dataset_id) REFERENCES public.dataset(id);


--
-- Name: applyworkflow fk_applyworkflow_project_id_project; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.applyworkflow
    ADD CONSTRAINT fk_applyworkflow_project_id_project FOREIGN KEY (project_id) REFERENCES public.project(id);


--
-- Name: applyworkflow fk_applyworkflow_workflow_id_workflow; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.applyworkflow
    ADD CONSTRAINT fk_applyworkflow_workflow_id_workflow FOREIGN KEY (workflow_id) REFERENCES public.workflow(id);


--
-- Name: dataset fk_dataset_project_id_project; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dataset
    ADD CONSTRAINT fk_dataset_project_id_project FOREIGN KEY (project_id) REFERENCES public.project(id);


--
-- Name: datasetv2 fk_datasetv2_project_id_projectv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.datasetv2
    ADD CONSTRAINT fk_datasetv2_project_id_projectv2 FOREIGN KEY (project_id) REFERENCES public.projectv2(id);


--
-- Name: jobv2 fk_jobv2_dataset_id_datasetv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.jobv2
    ADD CONSTRAINT fk_jobv2_dataset_id_datasetv2 FOREIGN KEY (dataset_id) REFERENCES public.datasetv2(id);


--
-- Name: jobv2 fk_jobv2_project_id_projectv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.jobv2
    ADD CONSTRAINT fk_jobv2_project_id_projectv2 FOREIGN KEY (project_id) REFERENCES public.projectv2(id);


--
-- Name: jobv2 fk_jobv2_workflow_id_workflowv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.jobv2
    ADD CONSTRAINT fk_jobv2_workflow_id_workflowv2 FOREIGN KEY (workflow_id) REFERENCES public.workflowv2(id);


--
-- Name: linkusergroup fk_linkusergroup_group_id_usergroup; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkusergroup
    ADD CONSTRAINT fk_linkusergroup_group_id_usergroup FOREIGN KEY (group_id) REFERENCES public.usergroup(id);


--
-- Name: linkusergroup fk_linkusergroup_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkusergroup
    ADD CONSTRAINT fk_linkusergroup_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- Name: linkuserproject fk_linkuserproject_project_id_project; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkuserproject
    ADD CONSTRAINT fk_linkuserproject_project_id_project FOREIGN KEY (project_id) REFERENCES public.project(id);


--
-- Name: linkuserproject fk_linkuserproject_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkuserproject
    ADD CONSTRAINT fk_linkuserproject_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- Name: linkuserprojectv2 fk_linkuserprojectv2_project_id_projectv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkuserprojectv2
    ADD CONSTRAINT fk_linkuserprojectv2_project_id_projectv2 FOREIGN KEY (project_id) REFERENCES public.projectv2(id);


--
-- Name: linkuserprojectv2 fk_linkuserprojectv2_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkuserprojectv2
    ADD CONSTRAINT fk_linkuserprojectv2_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- Name: oauthaccount fk_oauthaccount_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.oauthaccount
    ADD CONSTRAINT fk_oauthaccount_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- Name: resource fk_resource_dataset_id_dataset; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.resource
    ADD CONSTRAINT fk_resource_dataset_id_dataset FOREIGN KEY (dataset_id) REFERENCES public.dataset(id);


--
-- Name: taskgroupactivityv2 fk_taskgroupactivityv2_taskgroupv2_id_taskgroupv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupactivityv2
    ADD CONSTRAINT fk_taskgroupactivityv2_taskgroupv2_id_taskgroupv2 FOREIGN KEY (taskgroupv2_id) REFERENCES public.taskgroupv2(id);


--
-- Name: taskgroupactivityv2 fk_taskgroupactivityv2_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupactivityv2
    ADD CONSTRAINT fk_taskgroupactivityv2_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- Name: taskgroupv2 fk_taskgroupv2_user_group_id_usergroup; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupv2
    ADD CONSTRAINT fk_taskgroupv2_user_group_id_usergroup FOREIGN KEY (user_group_id) REFERENCES public.usergroup(id);


--
-- Name: taskgroupv2 fk_taskgroupv2_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupv2
    ADD CONSTRAINT fk_taskgroupv2_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- Name: taskv2 fk_taskv2_taskgroupv2_id_taskgroupv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskv2
    ADD CONSTRAINT fk_taskv2_taskgroupv2_id_taskgroupv2 FOREIGN KEY (taskgroupv2_id) REFERENCES public.taskgroupv2(id);


--
-- Name: user_oauth fk_user_oauth_user_settings_id_user_settings; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_oauth
    ADD CONSTRAINT fk_user_oauth_user_settings_id_user_settings FOREIGN KEY (user_settings_id) REFERENCES public.user_settings(id);


--
-- Name: workflow fk_workflow_project_id_project; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflow
    ADD CONSTRAINT fk_workflow_project_id_project FOREIGN KEY (project_id) REFERENCES public.project(id);


--
-- Name: workflowtask fk_workflowtask_task_id_task; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtask
    ADD CONSTRAINT fk_workflowtask_task_id_task FOREIGN KEY (task_id) REFERENCES public.task(id);


--
-- Name: workflowtask fk_workflowtask_workflow_id_workflow; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtask
    ADD CONSTRAINT fk_workflowtask_workflow_id_workflow FOREIGN KEY (workflow_id) REFERENCES public.workflow(id);


--
-- Name: workflowtaskv2 fk_workflowtaskv2_task_id_taskv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtaskv2
    ADD CONSTRAINT fk_workflowtaskv2_task_id_taskv2 FOREIGN KEY (task_id) REFERENCES public.taskv2(id);


--
-- Name: workflowtaskv2 fk_workflowtaskv2_workflow_id_workflowv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtaskv2
    ADD CONSTRAINT fk_workflowtaskv2_workflow_id_workflowv2 FOREIGN KEY (workflow_id) REFERENCES public.workflowv2(id);


--
-- Name: workflowv2 fk_workflowv2_project_id_projectv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowv2
    ADD CONSTRAINT fk_workflowv2_project_id_projectv2 FOREIGN KEY (project_id) REFERENCES public.projectv2(id);


--
-- PostgreSQL database dump complete
--
