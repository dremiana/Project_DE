-- Процедура для заполнения витрины остатков по лицевым счетам

create or replace procedure ds.fill_account_balance_f (i_OnDate date)
language plpgsql
as $$
declare
	start_time timestamp;
	end_time timestamp;
	previous_date date;
begin
	start_time := current_timestamp;
	previous_date := i_OnDate - interval '1 day';
	-- Логирование
    insert into logs.log_table (log_data, log_time)
    values ('Начало заполнения DM_ACCOUNT_BALANCE_F за ' || i_OnDate, start_time);
   -- Удаление существующих записей за дату расчета
    delete from dm.dm_account_balance_f where on_date = i_OnDate;
   -- Вставка новых данных
    insert into dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    select 
    	i_OnDate,
    	a.account_rk,
    	case 
	    	/* для активных счетов (DS.MD_ACCOUNT_D.char_type = ‘А’): берем остаток в валюте счета за предыдущий день (если его нет, то считаем его равным 0), 
	    	 * прибавляем к нему обороты по дебету в валюте счета (DM.DM_ACCOUNT_TURNOVER_F.debet_amount) и вычитаем обороты по кредиту 
	    	 * в валюте счета  (DM.DM_ACCOUNT_TURNOVER_F.credit_amount) за этот день. 
	    	 * для пассивных счетов (DS.MD_ACCOUNT_D.char_type = ‘П’): берем остаток в валюте счета за предыдущий день 
	    	 * (если его нет, то считаем его равным 0), вычитаем из него обороты по дебету в валюте счета и прибавляем 
	    	 * обороты по кредиту в валюте счета  за этот день*/
    		when a.char_type = 'А' then coalesce (prev.balance_out, 0) + coalesce (t.debet_amount, 0) - coalesce (t.credit_amount, 0)
    		when a.char_type = 'П' then coalesce (prev.balance_out, 0) - coalesce (t.debet_amount, 0) + coalesce (t.credit_amount, 0)
    	end as balance_out,
    	case 
    		when a.char_type = 'А' then (coalesce (prev.balance_out, 0) + coalesce (t.debet_amount, 0) - coalesce (t.credit_amount, 0)) * coalesce (er.reduced_cource, 1)
    		when a.char_type = 'П' then (coalesce (prev.balance_out, 0) - coalesce (t.debet_amount, 0) + coalesce (t.credit_amount, 0)) * coalesce (er.reduced_cource, 1)
    	end as balance_out_rub
    	
    from DS.MD_ACCOUNT_D a
    left join DM.DM_ACCOUNT_BALANCE_F prev on a.account_rk = prev.account_rk and previous_date = prev.on_date
    left join DM.DM_ACCOUNT_TURNOVER_F t on a.account_rk = t.account_rk and t.on_date = i_OnDate
    left join DS.MD_EXCHANGE_RATE_D er on a.currency_rk = er.currency_rk and i_OnDate between er.data_actual_date and er.data_actual_end_date 
    where i_OnDate between a.data_actual_date and a.data_actual_end_date;
   
    -- Логирование
	end_time := current_timestamp;
	insert into logs.log_table (log_data, log_time)
	values ('Окончание заполнения DM_ACCOUNT_BALANCE_F за ' || i_OnDate, end_time);
	
	exception when others then
	end_time := current_timestamp;
	insert into logs.log_table (log_data, log_time)
	values ('Ошибка: ' || SQLERRM, end_time);
    
end;
$$



-- Вызов процедуры для заполнения витрины остатков по лицевым счетам для нужного месяца

DO $$
declare
	start_date date;
    end_date date;
	prev_day date;
    day date;
begin
	start_date := date '2018-01-01';
    end_date := (start_date + interval '1 month' - interval '1 day');
	prev_day := start_date - interval '1 day';
	day := start_date;
    -- Заполнение данными за 31.12.2017
    insert into DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, currency_rk, balance_out, balance_out_rub)
    select 
        b.on_date,
        b.account_rk,
        b.currency_rk,
        b.balance_out,
        b.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
	from DS.FT_BALANCE_F b
	left join DS.MD_EXCHANGE_RATE_D er on b.currency_rk = er.currency_rk 
		and prev_day between er.data_actual_date and er.data_actual_end_date
	where b.on_date = prev_day;

    -- Расчет за каждый день января 2018
	while day <= end_date loop
		call ds.fill_account_balance_f(day);
		day := day + interval '1 day';
	end loop;
end $$;



select * 
from DM.DM_ACCOUNT_BALANCE_F 
order by on_date desc;
    	