-- Удаление таблиц

drop table if exists logs.log_table cascade;

-- Создание таблиц для логов

create table if not exists logs.log_table
(
    id_log_table serial not null,
    log_data text collate pg_catalog."default" not null,
    log_time timestamp without time zone not null,
    constraint logs_pk primary key (id_log_table)
);

