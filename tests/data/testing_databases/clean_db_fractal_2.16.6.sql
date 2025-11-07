--
-- PostgreSQL database dump
--

\restrict UFeLsxWLfUOdmFZHAh4GpZLTFKJlONtX9T1MNAVXB9nIfyq7nfiQkLOwJpUyivV

-- Dumped from database version 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)

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
-- Name: accountingrecord; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.accountingrecord (
    id integer NOT NULL,
    user_id integer NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    num_tasks integer NOT NULL,
    num_new_images integer NOT NULL
);


ALTER TABLE public.accountingrecord OWNER TO postgres;

--
-- Name: accountingrecord_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.accountingrecord_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.accountingrecord_id_seq OWNER TO postgres;

--
-- Name: accountingrecord_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.accountingrecord_id_seq OWNED BY public.accountingrecord.id;


--
-- Name: accountingrecordslurm; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.accountingrecordslurm (
    id integer NOT NULL,
    user_id integer NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    slurm_job_ids integer[]
);


ALTER TABLE public.accountingrecordslurm OWNER TO postgres;

--
-- Name: accountingrecordslurm_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.accountingrecordslurm_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.accountingrecordslurm_id_seq OWNER TO postgres;

--
-- Name: accountingrecordslurm_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.accountingrecordslurm_id_seq OWNED BY public.accountingrecordslurm.id;


--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


ALTER TABLE public.alembic_version OWNER TO postgres;

--
-- Name: datasetv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.datasetv2 (
    id integer NOT NULL,
    name character varying NOT NULL,
    project_id integer NOT NULL,
    history jsonb DEFAULT '[]'::json NOT NULL,
    timestamp_created timestamp with time zone NOT NULL,
    zarr_dir character varying NOT NULL,
    images jsonb DEFAULT '[]'::json NOT NULL
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


ALTER SEQUENCE public.datasetv2_id_seq OWNER TO postgres;

--
-- Name: datasetv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.datasetv2_id_seq OWNED BY public.datasetv2.id;


--
-- Name: historyimagecache; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.historyimagecache (
    zarr_url character varying NOT NULL,
    dataset_id integer NOT NULL,
    workflowtask_id integer NOT NULL,
    latest_history_unit_id integer NOT NULL
);


ALTER TABLE public.historyimagecache OWNER TO postgres;

--
-- Name: historyrun; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.historyrun (
    id integer NOT NULL,
    dataset_id integer NOT NULL,
    workflowtask_id integer,
    workflowtask_dump jsonb NOT NULL,
    task_group_dump jsonb NOT NULL,
    timestamp_started timestamp with time zone NOT NULL,
    status character varying NOT NULL,
    num_available_images integer NOT NULL,
    job_id integer NOT NULL,
    task_id integer
);


ALTER TABLE public.historyrun OWNER TO postgres;

--
-- Name: historyrun_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.historyrun_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.historyrun_id_seq OWNER TO postgres;

--
-- Name: historyrun_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.historyrun_id_seq OWNED BY public.historyrun.id;


--
-- Name: historyunit; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.historyunit (
    id integer NOT NULL,
    history_run_id integer NOT NULL,
    logfile character varying DEFAULT '__LOGFILE_PLACEHOLDER__'::character varying NOT NULL,
    status character varying NOT NULL,
    zarr_urls character varying[]
);


ALTER TABLE public.historyunit OWNER TO postgres;

--
-- Name: historyunit_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.historyunit_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.historyunit_id_seq OWNER TO postgres;

--
-- Name: historyunit_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.historyunit_id_seq OWNED BY public.historyunit.id;


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
    dataset_dump jsonb NOT NULL,
    workflow_dump jsonb NOT NULL,
    project_dump jsonb NOT NULL,
    worker_init character varying,
    working_dir character varying,
    working_dir_user character varying,
    first_task_index integer NOT NULL,
    last_task_index integer NOT NULL,
    start_timestamp timestamp with time zone NOT NULL,
    end_timestamp timestamp with time zone,
    status character varying NOT NULL,
    log character varying,
    attribute_filters jsonb DEFAULT '{}'::json NOT NULL,
    type_filters jsonb DEFAULT '{}'::json NOT NULL,
    executor_error_log character varying
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


ALTER SEQUENCE public.jobv2_id_seq OWNER TO postgres;

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


ALTER SEQUENCE public.oauthaccount_id_seq OWNER TO postgres;

--
-- Name: oauthaccount_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.oauthaccount_id_seq OWNED BY public.oauthaccount.id;


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


ALTER SEQUENCE public.projectv2_id_seq OWNER TO postgres;

--
-- Name: projectv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.projectv2_id_seq OWNED BY public.projectv2.id;


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


ALTER SEQUENCE public.taskgroupactivityv2_id_seq OWNER TO postgres;

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
    archive_path character varying,
    pip_extras character varying,
    pinned_package_versions_post jsonb DEFAULT '{}'::json,
    active boolean NOT NULL,
    timestamp_created timestamp with time zone NOT NULL,
    env_info character varying,
    "venv_size_in_kB" integer,
    venv_file_number integer,
    timestamp_last_used timestamp with time zone NOT NULL,
    pixi_version character varying,
    pinned_package_versions_pre jsonb DEFAULT '{}'::jsonb
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


ALTER SEQUENCE public.taskgroupv2_id_seq OWNER TO postgres;

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
    input_types jsonb,
    output_types jsonb,
    taskgroupv2_id integer NOT NULL,
    category character varying,
    modality character varying,
    authors character varying,
    tags jsonb DEFAULT '[]'::json NOT NULL
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


ALTER SEQUENCE public.taskv2_id_seq OWNER TO postgres;

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


ALTER SEQUENCE public.user_oauth_id_seq OWNER TO postgres;

--
-- Name: user_oauth_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.user_oauth_id_seq OWNED BY public.user_oauth.id;


--
-- Name: user_settings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.user_settings (
    id integer NOT NULL,
    slurm_accounts jsonb DEFAULT '[]'::json NOT NULL,
    ssh_host character varying,
    ssh_username character varying,
    ssh_private_key_path character varying,
    ssh_tasks_dir character varying,
    ssh_jobs_dir character varying,
    slurm_user character varying,
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


ALTER SEQUENCE public.user_settings_id_seq OWNER TO postgres;

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
    viewer_paths jsonb DEFAULT '[]'::json NOT NULL
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


ALTER SEQUENCE public.usergroup_id_seq OWNER TO postgres;

--
-- Name: usergroup_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.usergroup_id_seq OWNED BY public.usergroup.id;


--
-- Name: workflowtaskv2; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.workflowtaskv2 (
    id integer NOT NULL,
    workflow_id integer NOT NULL,
    "order" integer,
    meta_parallel json,
    meta_non_parallel json,
    args_parallel jsonb,
    args_non_parallel jsonb,
    task_type character varying NOT NULL,
    task_id integer NOT NULL,
    type_filters jsonb DEFAULT '{}'::json NOT NULL
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


ALTER SEQUENCE public.workflowtaskv2_id_seq OWNER TO postgres;

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


ALTER SEQUENCE public.workflowv2_id_seq OWNER TO postgres;

--
-- Name: workflowv2_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.workflowv2_id_seq OWNED BY public.workflowv2.id;


--
-- Name: accountingrecord id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.accountingrecord ALTER COLUMN id SET DEFAULT nextval('public.accountingrecord_id_seq'::regclass);


--
-- Name: accountingrecordslurm id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.accountingrecordslurm ALTER COLUMN id SET DEFAULT nextval('public.accountingrecordslurm_id_seq'::regclass);


--
-- Name: datasetv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.datasetv2 ALTER COLUMN id SET DEFAULT nextval('public.datasetv2_id_seq'::regclass);


--
-- Name: historyrun id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyrun ALTER COLUMN id SET DEFAULT nextval('public.historyrun_id_seq'::regclass);


--
-- Name: historyunit id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyunit ALTER COLUMN id SET DEFAULT nextval('public.historyunit_id_seq'::regclass);


--
-- Name: jobv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.jobv2 ALTER COLUMN id SET DEFAULT nextval('public.jobv2_id_seq'::regclass);


--
-- Name: oauthaccount id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.oauthaccount ALTER COLUMN id SET DEFAULT nextval('public.oauthaccount_id_seq'::regclass);


--
-- Name: projectv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.projectv2 ALTER COLUMN id SET DEFAULT nextval('public.projectv2_id_seq'::regclass);


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
-- Name: workflowtaskv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtaskv2 ALTER COLUMN id SET DEFAULT nextval('public.workflowtaskv2_id_seq'::regclass);


--
-- Name: workflowv2 id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowv2 ALTER COLUMN id SET DEFAULT nextval('public.workflowv2_id_seq'::regclass);


--
-- Data for Name: accountingrecord; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.accountingrecord (id, user_id, "timestamp", num_tasks, num_new_images) FROM stdin;
\.


--
-- Data for Name: accountingrecordslurm; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.accountingrecordslurm (id, user_id, "timestamp", slurm_job_ids) FROM stdin;
\.


--
-- Data for Name: alembic_version; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.alembic_version (version_num) FROM stdin;
981d588fe248
\.


--
-- Data for Name: datasetv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.datasetv2 (id, name, project_id, history, timestamp_created, zarr_dir, images) FROM stdin;
1	MyDataset	1	[{"status": "done", "workflowtask": {"id": 1, "task": {"id": 1, "name": "Echo Task", "type": "compound", "owner": "admin", "source": "admin:echo-task", "version": null, "input_types": {}, "output_types": {}, "command_parallel": "echo", "command_non_parallel": "echo"}, "order": 0, "task_id": 1, "task_legacy": null, "workflow_id": 1, "type_filters": {}, "is_legacy_task": false, "task_legacy_id": null}, "parallelization": {}}]	2024-04-24 12:54:44.017086+02	/invalid/zarr	[{"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000000", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000000", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}, {"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000001", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000001", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}, {"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000002", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000002", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}, {"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000003", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000003", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}, {"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000004", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000004", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}, {"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000005", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000005", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}, {"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000006", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000006", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}, {"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000007", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000007", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}, {"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000008", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000008", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}, {"types": {"is_3D": true}, "origin": "/invalid/zarr/very/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/origin-000009", "zarr_url": "/invalid/zarr/very/very/long/path/to/mimic/real/path/to/the/zarr/dir/000009", "attributes": {"well": "A99", "plate": "my-beautiful-plate.zarr"}}]
\.


--
-- Data for Name: historyimagecache; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.historyimagecache (zarr_url, dataset_id, workflowtask_id, latest_history_unit_id) FROM stdin;
\.


--
-- Data for Name: historyrun; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.historyrun (id, dataset_id, workflowtask_id, workflowtask_dump, task_group_dump, timestamp_started, status, num_available_images, job_id, task_id) FROM stdin;
\.


--
-- Data for Name: historyunit; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.historyunit (id, history_run_id, logfile, status, zarr_urls) FROM stdin;
\.


--
-- Data for Name: jobv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.jobv2 (id, project_id, workflow_id, dataset_id, user_email, slurm_account, dataset_dump, workflow_dump, project_dump, worker_init, working_dir, working_dir_user, first_task_index, last_task_index, start_timestamp, end_timestamp, status, log, attribute_filters, type_filters, executor_error_log) FROM stdin;
1	1	1	1	vanilla@example.org	\N	{"id": 1, "name": "MyDataset", "zarr_dir": "/invalid/zarr", "project_id": 1, "type_filters": {}, "attribute_filters": {}, "timestamp_created": "2024-04-24T10:54:44.017086+00:00"}	{"id": 1, "name": "MyWorkflow", "project_id": 1, "timestamp_created": "2024-04-24T10:54:44.034782+00:00"}	{"id": 1, "name": "MyProject_uv", "timestamp_created": "2024-04-24T10:54:43.995984+00:00"}	\N	/private/tmp/proj_0000001_wf_0000001_job_0000001_20240424_105444	/private/tmp/proj_0000001_wf_0000001_job_0000001_20240424_105444	0	0	2024-04-24 12:54:44.103821+02	2024-04-24 12:54:44.176501+02	done	2024-04-24 12:54:44,151 - WF1_job1 - INFO - Start execution of workflow "MyWorkflow"; more logs at /private/tmp/proj_0000001_wf_0000001_job_0000001_20240424_105444/workflow.log\n2024-04-24 12:54:44,151 - WF1_job1 - DEBUG - fractal_server.__VERSION__: 2.0.0a11\n2024-04-24 12:54:44,151 - WF1_job1 - DEBUG - FRACTAL_RUNNER_BACKEND: local\n2024-04-24 12:54:44,151 - WF1_job1 - DEBUG - slurm_user: vanilla-slurm\n2024-04-24 12:54:44,153 - WF1_job1 - DEBUG - slurm_account: None\n2024-04-24 12:54:44,153 - WF1_job1 - DEBUG - worker_init: None\n2024-04-24 12:54:44,153 - WF1_job1 - DEBUG - job.id: 1\n2024-04-24 12:54:44,153 - WF1_job1 - DEBUG - job.working_dir: /private/tmp/proj_0000001_wf_0000001_job_0000001_20240424_105444\n2024-04-24 12:54:44,153 - WF1_job1 - DEBUG - job.working_dir_user: /private/tmp/proj_0000001_wf_0000001_job_0000001_20240424_105444\n2024-04-24 12:54:44,153 - WF1_job1 - DEBUG - job.first_task_index: 0\n2024-04-24 12:54:44,153 - WF1_job1 - DEBUG - job.last_task_index: 0\n2024-04-24 12:54:44,153 - WF1_job1 - DEBUG - START workflow "MyWorkflow"\n2024-04-24 12:54:44,154 - WF1_job1 - DEBUG - SUBMIT 0-th task (name="Echo Task")\n2024-04-24 12:54:44,168 - WF1_job1 - DEBUG - END    0-th task (name="Echo Task")\n2024-04-24 12:54:44,169 - WF1_job1 - INFO - End execution of workflow "MyWorkflow"; more logs at /private/tmp/proj_0000001_wf_0000001_job_0000001_20240424_105444/workflow.log\n2024-04-24 12:54:44,169 - WF1_job1 - DEBUG - END workflow "MyWorkflow"\n	{}	{}	\N
\.


--
-- Data for Name: linkusergroup; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.linkusergroup (group_id, user_id, timestamp_created) FROM stdin;
1	1	2000-01-01 01:00:00+01
1	6	2000-01-01 01:00:00+01
1	27	2000-01-01 01:00:00+01
1	28	2000-01-01 01:00:00+01
\.


--
-- Data for Name: linkuserprojectv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.linkuserprojectv2 (project_id, user_id) FROM stdin;
1	28
\.


--
-- Data for Name: oauthaccount; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.oauthaccount (id, user_id, oauth_name, access_token, expires_at, refresh_token, account_id, account_email) FROM stdin;
\.


--
-- Data for Name: projectv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.projectv2 (id, name, timestamp_created) FROM stdin;
1	MyProject_uv	2024-04-24 12:54:43.995984+02
\.


--
-- Data for Name: taskgroupactivityv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.taskgroupactivityv2 (id, user_id, taskgroupv2_id, timestamp_started, pkg_name, version, status, action, log, timestamp_ended) FROM stdin;
1	1	2	2024-11-12 15:08:40.027268+01	admin:ls-task	1.0.0	OK	collect	\N	\N
\.


--
-- Data for Name: taskgroupv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.taskgroupv2 (id, user_id, user_group_id, origin, pkg_name, version, python_version, path, venv_path, archive_path, pip_extras, pinned_package_versions_post, active, timestamp_created, env_info, "venv_size_in_kB", venv_file_number, timestamp_last_used, pixi_version, pinned_package_versions_pre) FROM stdin;
1	1	1	other	admin:echo-task	\N	\N	\N	\N	\N	\N	{}	t	2024-10-29 09:05:04.891366+01	\N	\N	\N	2024-10-29 09:05:04.891366+01	\N	{}
2	1	1	other	admin:ls-task	1.0.0	\N	\N	\N	\N	\N	{}	t	2024-10-29 09:05:04.91603+01	\N	\N	\N	2024-10-29 09:05:04.91603+01	\N	{}
\.


--
-- Data for Name: taskv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.taskv2 (id, name, type, command_non_parallel, command_parallel, source, meta_non_parallel, meta_parallel, version, args_schema_non_parallel, args_schema_parallel, args_schema_version, docs_info, docs_link, input_types, output_types, taskgroupv2_id, category, modality, authors, tags) FROM stdin;
1	Echo Task	compound	echo	echo	admin:echo-task	{}	{}	\N	null	null	\N	\N	\N	{}	{}	1	\N	\N	\N	[]
2	Ls Task	non_parallel	ls	\N	admin:ls-task	{}	{}	\N	null	null	\N	\N	\N	{}	{}	2	\N	\N	\N	[]
\.


--
-- Data for Name: user_oauth; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.user_oauth (id, email, hashed_password, is_active, is_superuser, is_verified, username, user_settings_id) FROM stdin;
1	admin@example.org	$2b$12$qVuxg/SmyTLvtVDUcWoD..3Q9QvScTrUDbSW8IaYX1vZqbwGY0dUq	t	t	f	admin	1
6	user@example.org	$2b$12$qVuxg/SmyTLvtVDUcWoD..3Q9QvScTrUDbSW8IaYX1vZqbwGY0dUq	t	f	f	__REDACTED_OWNER__	2
28	vanilla@example.org	$2b$12$tS4FU1JBa5XuFtqbGKZD/ubUAaTvbtsaqPJkBhLnMm0TgQwiQR8rm	t	f	t	\N	4
27	admin@fractal.xy	$2b$12$ya6S7rcG/S.aaJFoy6DzhOmlREv0lcJ/D1SV8lM1harCCBDlKBSXS	t	t	t	admin_123	3
\.


--
-- Data for Name: user_settings; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.user_settings (id, slurm_accounts, ssh_host, ssh_username, ssh_private_key_path, ssh_tasks_dir, ssh_jobs_dir, slurm_user, project_dir) FROM stdin;
1	[]	\N	\N	\N	\N	\N	\N	\N
2	[]	\N	\N	\N	\N	\N	__REDACTED_SLURM_USER_	\N
3	[]	\N	\N	\N	\N	\N	slurm	/placeholder
4	[]	\N	\N	\N	\N	\N	vanilla-slurm	/placeholder
\.


--
-- Data for Name: usergroup; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.usergroup (id, name, timestamp_created, viewer_paths) FROM stdin;
1	All	2024-09-12 12:52:48.441196+02	[]
\.


--
-- Data for Name: workflowtaskv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.workflowtaskv2 (id, workflow_id, "order", meta_parallel, meta_non_parallel, args_parallel, args_non_parallel, task_type, task_id, type_filters) FROM stdin;
1	1	0	null	null	null	null	compound	1	{}
\.


--
-- Data for Name: workflowv2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.workflowv2 (id, name, project_id, timestamp_created) FROM stdin;
1	MyWorkflow	1	2024-04-24 12:54:44.034782+02
\.


--
-- Name: accountingrecord_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.accountingrecord_id_seq', 1, false);


--
-- Name: accountingrecordslurm_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.accountingrecordslurm_id_seq', 1, false);


--
-- Name: datasetv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.datasetv2_id_seq', 1, true);


--
-- Name: historyrun_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.historyrun_id_seq', 1, false);


--
-- Name: historyunit_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.historyunit_id_seq', 1, false);


--
-- Name: jobv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.jobv2_id_seq', 1, true);


--
-- Name: oauthaccount_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.oauthaccount_id_seq', 1, false);


--
-- Name: projectv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.projectv2_id_seq', 1, true);


--
-- Name: taskgroupactivityv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.taskgroupactivityv2_id_seq', 1, true);


--
-- Name: taskgroupv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.taskgroupv2_id_seq', 2, true);


--
-- Name: taskv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.taskv2_id_seq', 2, true);


--
-- Name: user_oauth_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.user_oauth_id_seq', 28, true);


--
-- Name: user_settings_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.user_settings_id_seq', 4, true);


--
-- Name: usergroup_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.usergroup_id_seq', 1, true);


--
-- Name: workflowtaskv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.workflowtaskv2_id_seq', 1, true);


--
-- Name: workflowv2_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.workflowv2_id_seq', 1, true);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: oauthaccount oauthaccount_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.oauthaccount
    ADD CONSTRAINT oauthaccount_pkey PRIMARY KEY (id);


--
-- Name: accountingrecord pk_accountingrecord; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.accountingrecord
    ADD CONSTRAINT pk_accountingrecord PRIMARY KEY (id);


--
-- Name: accountingrecordslurm pk_accountingrecordslurm; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.accountingrecordslurm
    ADD CONSTRAINT pk_accountingrecordslurm PRIMARY KEY (id);


--
-- Name: datasetv2 pk_datasetv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.datasetv2
    ADD CONSTRAINT pk_datasetv2 PRIMARY KEY (id);


--
-- Name: historyimagecache pk_historyimagecache; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyimagecache
    ADD CONSTRAINT pk_historyimagecache PRIMARY KEY (zarr_url, dataset_id, workflowtask_id);


--
-- Name: historyrun pk_historyrun; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyrun
    ADD CONSTRAINT pk_historyrun PRIMARY KEY (id);


--
-- Name: historyunit pk_historyunit; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyunit
    ADD CONSTRAINT pk_historyunit PRIMARY KEY (id);


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
-- Name: linkuserprojectv2 pk_linkuserprojectv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkuserprojectv2
    ADD CONSTRAINT pk_linkuserprojectv2 PRIMARY KEY (project_id, user_id);


--
-- Name: projectv2 pk_projectv2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.projectv2
    ADD CONSTRAINT pk_projectv2 PRIMARY KEY (id);


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
-- Name: usergroup uq_usergroup_name; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.usergroup
    ADD CONSTRAINT uq_usergroup_name UNIQUE (name);


--
-- Name: user_oauth user_oauth_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_oauth
    ADD CONSTRAINT user_oauth_pkey PRIMARY KEY (id);


--
-- Name: ix_historyimagecache_dataset_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_historyimagecache_dataset_id ON public.historyimagecache USING btree (dataset_id);


--
-- Name: ix_historyimagecache_workflowtask_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_historyimagecache_workflowtask_id ON public.historyimagecache USING btree (workflowtask_id);


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
-- Name: accountingrecord fk_accountingrecord_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.accountingrecord
    ADD CONSTRAINT fk_accountingrecord_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- Name: accountingrecordslurm fk_accountingrecordslurm_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.accountingrecordslurm
    ADD CONSTRAINT fk_accountingrecordslurm_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- Name: datasetv2 fk_datasetv2_project_id_projectv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.datasetv2
    ADD CONSTRAINT fk_datasetv2_project_id_projectv2 FOREIGN KEY (project_id) REFERENCES public.projectv2(id) ON DELETE CASCADE;


--
-- Name: historyimagecache fk_historyimagecache_dataset_id_datasetv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyimagecache
    ADD CONSTRAINT fk_historyimagecache_dataset_id_datasetv2 FOREIGN KEY (dataset_id) REFERENCES public.datasetv2(id) ON DELETE CASCADE;


--
-- Name: historyimagecache fk_historyimagecache_latest_history_unit_id_historyunit; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyimagecache
    ADD CONSTRAINT fk_historyimagecache_latest_history_unit_id_historyunit FOREIGN KEY (latest_history_unit_id) REFERENCES public.historyunit(id) ON DELETE CASCADE;


--
-- Name: historyimagecache fk_historyimagecache_workflowtask_id_workflowtaskv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyimagecache
    ADD CONSTRAINT fk_historyimagecache_workflowtask_id_workflowtaskv2 FOREIGN KEY (workflowtask_id) REFERENCES public.workflowtaskv2(id) ON DELETE CASCADE;


--
-- Name: historyrun fk_historyrun_dataset_id_datasetv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyrun
    ADD CONSTRAINT fk_historyrun_dataset_id_datasetv2 FOREIGN KEY (dataset_id) REFERENCES public.datasetv2(id) ON DELETE CASCADE;


--
-- Name: historyrun fk_historyrun_job_id_jobv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyrun
    ADD CONSTRAINT fk_historyrun_job_id_jobv2 FOREIGN KEY (job_id) REFERENCES public.jobv2(id);


--
-- Name: historyrun fk_historyrun_task_id_taskv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyrun
    ADD CONSTRAINT fk_historyrun_task_id_taskv2 FOREIGN KEY (task_id) REFERENCES public.taskv2(id) ON DELETE SET NULL;


--
-- Name: historyrun fk_historyrun_workflowtask_id_workflowtaskv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyrun
    ADD CONSTRAINT fk_historyrun_workflowtask_id_workflowtaskv2 FOREIGN KEY (workflowtask_id) REFERENCES public.workflowtaskv2(id) ON DELETE SET NULL;


--
-- Name: historyunit fk_historyunit_history_run_id_historyrun; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.historyunit
    ADD CONSTRAINT fk_historyunit_history_run_id_historyrun FOREIGN KEY (history_run_id) REFERENCES public.historyrun(id) ON DELETE CASCADE;


--
-- Name: jobv2 fk_jobv2_dataset_id_datasetv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.jobv2
    ADD CONSTRAINT fk_jobv2_dataset_id_datasetv2 FOREIGN KEY (dataset_id) REFERENCES public.datasetv2(id) ON DELETE SET NULL;


--
-- Name: jobv2 fk_jobv2_project_id_projectv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.jobv2
    ADD CONSTRAINT fk_jobv2_project_id_projectv2 FOREIGN KEY (project_id) REFERENCES public.projectv2(id) ON DELETE SET NULL;


--
-- Name: jobv2 fk_jobv2_workflow_id_workflowv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.jobv2
    ADD CONSTRAINT fk_jobv2_workflow_id_workflowv2 FOREIGN KEY (workflow_id) REFERENCES public.workflowv2(id) ON DELETE SET NULL;


--
-- Name: linkusergroup fk_linkusergroup_group_id_usergroup; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkusergroup
    ADD CONSTRAINT fk_linkusergroup_group_id_usergroup FOREIGN KEY (group_id) REFERENCES public.usergroup(id) ON DELETE CASCADE;


--
-- Name: linkusergroup fk_linkusergroup_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.linkusergroup
    ADD CONSTRAINT fk_linkusergroup_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id) ON DELETE CASCADE;


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
-- Name: taskgroupactivityv2 fk_taskgroupactivityv2_taskgroupv2_id_taskgroupv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupactivityv2
    ADD CONSTRAINT fk_taskgroupactivityv2_taskgroupv2_id_taskgroupv2 FOREIGN KEY (taskgroupv2_id) REFERENCES public.taskgroupv2(id) ON DELETE SET NULL;


--
-- Name: taskgroupactivityv2 fk_taskgroupactivityv2_user_id_user_oauth; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupactivityv2
    ADD CONSTRAINT fk_taskgroupactivityv2_user_id_user_oauth FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- Name: taskgroupv2 fk_taskgroupv2_user_group_id_usergroup; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.taskgroupv2
    ADD CONSTRAINT fk_taskgroupv2_user_group_id_usergroup FOREIGN KEY (user_group_id) REFERENCES public.usergroup(id) ON DELETE SET NULL;


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
-- Name: workflowtaskv2 fk_workflowtaskv2_task_id_taskv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtaskv2
    ADD CONSTRAINT fk_workflowtaskv2_task_id_taskv2 FOREIGN KEY (task_id) REFERENCES public.taskv2(id);


--
-- Name: workflowtaskv2 fk_workflowtaskv2_workflow_id_workflowv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowtaskv2
    ADD CONSTRAINT fk_workflowtaskv2_workflow_id_workflowv2 FOREIGN KEY (workflow_id) REFERENCES public.workflowv2(id) ON DELETE CASCADE;


--
-- Name: workflowv2 fk_workflowv2_project_id_projectv2; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.workflowv2
    ADD CONSTRAINT fk_workflowv2_project_id_projectv2 FOREIGN KEY (project_id) REFERENCES public.projectv2(id) ON DELETE CASCADE;


--
-- Name: oauthaccount oauthaccount_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.oauthaccount
    ADD CONSTRAINT oauthaccount_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.user_oauth(id);


--
-- PostgreSQL database dump complete
--

\unrestrict UFeLsxWLfUOdmFZHAh4GpZLTFKJlONtX9T1MNAVXB9nIfyq7nfiQkLOwJpUyivV
