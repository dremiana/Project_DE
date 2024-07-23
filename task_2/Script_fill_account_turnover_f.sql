-- Процедура для расчета витрины оборотов по лицевым счетам

create or replace procedure ds.fill_account_turnover_f (i_OnDate date)
language plpgsql
as $$
declare
    start_time timestamp;
    end_time timestamp;
begin
    start_time := current_timestamp;
	-- Логирование
    insert into logs.log_table (log_data, log_time)
    values ('Начало заполнения DM_ACCOUNT_TURNOVER_F за ' || i_OnDate, start_time);
   -- Удаление существующих записей за дату расчета
    delete from DM.DM_ACCOUNT_TURNOVER_F where on_date = i_OnDate;
   -- Вставка новых данных
    insert into DM.DM_ACCOUNT_TURNOVER_F (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    with transactions_data as (
    	select 
    		p.credit_account_rk as account_rk,
    		p.credit_amount as credit_amount,
    		p.credit_amount * coalesce(er.reduced_cource, 1) as credit_amount_rub,
    		cast(null as int) as debet_amount,
    		cast(null as int) as debet_amount_rub
    	from DS.FT_POSTING_F p
    	join DS.MD_ACCOUNT_D a on p.credit_account_rk = a.account_rk
    	left join DS.MD_EXCHANGE_RATE_D er on er.currency_rk = a.currency_rk 
    		and i_OnDate between er.data_actual_date and er.data_actual_end_date
    	where p.oper_date = i_OnDate 
    		and i_OnDate between a.data_actual_date and a.data_actual_end_date
    		and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('month', i_OnDate) + interval '1 month - 1 day')
    	
    	union all
    	
    	select 
    		p.credit_account_rk as account_rk,
    		cast(null as int) as credit_amount,
    		cast(null as int) as credit_amount_rub,
    		p.debet_amount as debet_amount,
    		p.debet_amount * coalesce(er.reduced_cource, 1) as debet_amount_rub
    	from DS.FT_POSTING_F p
    	join DS.MD_ACCOUNT_D a on p.debet_account_rk = a.account_rk 
    	left join DS.MD_EXCHANGE_RATE_D er on er.currency_rk = a.currency_rk 
    		and i_OnDate between er.data_actual_date and er.data_actual_end_date
    	where p.oper_date = i_OnDate 
    		and i_OnDate between a.data_actual_date and a.data_actual_end_date
    		and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('month', i_OnDate) + interval '1 month - 1 day')
    )
	select 
		i_OnDate as on_date,
		td.account_rk,
		sum(td.credit_amount) as credit_amount,
		sum(td.credit_amount_rub) as credit_amount_rub,
		sum(td.debet_amount) as debet_amount,
		sum(td.debet_amount_rub) as debet_amount_rub
	from transactions_data td
	group by td.account_rk;
	
	-- Логирование
	end_time := current_timestamp;
	insert into logs.log_table (log_data, log_time)
	values ('Окончание заполнения DM_ACCOUNT_TURNOVER_F за ' || i_OnDate, end_time);
	
	exception when others then
	end_time := current_timestamp;
	insert into logs.log_table (log_data, log_time)
	values ('Ошибка: ' || SQLERRM, end_time);

end;
$$


-- Вызов процедуры для расчета витрины оборотов по лицевым счетам для нужного месяца
do $$
declare
    start_date date;
    end_date date;
    day date;
begin
    start_date := date '2018-01-01';
    end_date := (start_date + interval '1 month' - interval '1 day');

    day := start_date;
    while day <= end_date loop
        call ds.fill_account_turnover_f(day);
        day := day + interval '1 day';
    end loop;
end;
$$;


select * 
from DM.DM_ACCOUNT_TURNOVER_F 
order by on_date desc;