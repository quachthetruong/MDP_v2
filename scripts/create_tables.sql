create sequence miner_cfg_id_seq
    as integer;

alter sequence miner_cfg_id_seq owner to postgres;

create sequence miner_cfg_id_seq1
    as integer;

alter sequence miner_cfg_id_seq1 owner to postgres;



create table stream_cfg
(
    id               serial
        primary key,
    name             varchar(255)                                                not null
        unique,
    description      text,
    signal_name      text                                                        not null,
    same_table_name  boolean     default true                                    not null,
    timestep_days    integer     default 0                                       not null,
    timestep_minutes integer     default 0                                       not null,
    timestamp_field  varchar(50) default 'indexed_timestamp_'::character varying not null,
    version          varchar(50) default '1'::character varying                  not null,
    symbol_field     varchar(50) default 'symbol_'::character varying            not null,
    storage_backend  varchar(50) default 'DatabaseStorage'::character varying    not null,
    timestep_hours   integer     default 0                                       not null,
    type             integer     default 0                                       not null
);

alter table stream_cfg
    owner to postgres;

create index idx_stream_name
    on stream_cfg (name);

create table miner_stream_relationship
(
    miner_id  integer     not null,
    stream_id integer     not null,
    type      varchar(50) not null,
    primary key (miner_id, stream_id, type)
);

alter table miner_stream_relationship
    owner to postgres;

create table stream_fields
(
    id             serial
        primary key,
    stream_id      integer      not null,
    name           varchar(255) not null,
    type           varchar(50),
    is_nullable    boolean default true,
    is_primary_key boolean default false
);

alter table stream_fields
    owner to postgres;

create unique index stream_id_name
    on stream_fields (stream_id, name);

create table miner_cfg_v2
(
    id               integer,
    name             varchar(255),
    description      text,
    target_symbols   text,
    schedule         varchar(50),
    timestep_days    integer,
    timestep_minutes integer,
    start_date_year  integer,
    start_date_month integer,
    start_date_day   integer,
    start_date_hour  integer,
    timestep_hours   integer,
    input_streams    character varying[],
    input_sources    character varying[],
    output_stream    character varying[]
);

alter table miner_cfg_v2
    owner to postgres;

create table miner_cfg
(
    id               integer     default nextval('miner_cfg_id_seq1'::regclass) not null
        constraint miner_cfg_pkey1
            primary key,
    name             varchar(255)                                               not null
        constraint miner_cfg_name_key1
            unique,
    description      text,
    target_symbols   text,
    schedule         varchar(50) default 'None'::character varying,
    timestep_days    integer     default 0                                      not null,
    timestep_hours   integer     default 0                                      not null,
    timestep_minutes integer     default 0                                      not null,
    start_date_year  integer     default 0                                      not null,
    start_date_month integer     default 0                                      not null,
    start_date_day   integer     default 0                                      not null,
    start_date_hour  integer     default 0                                      not null,
    file_path        text
);

alter table miner_cfg
    owner to postgres;

alter sequence miner_cfg_id_seq1 owned by miner_cfg.id;

create table miner_cfg_v4
(
    id               integer,
    name             varchar(255),
    description      text,
    target_symbols   text,
    schedule         varchar(50),
    timestep_days    integer,
    timestep_hours   integer,
    timestep_minutes integer,
    start_date_year  integer,
    start_date_month integer,
    start_date_day   integer,
    start_date_hour  integer,
    file_path        text,
    input_streams    character varying[],
    input_sources    character varying[],
    output_stream    character varying[]
);

alter table miner_cfg_v4
    owner to postgres;

