-- Процедура для расчета 101 формы

create or replace procedure dm.fill_f101_round_f (i_OnDate date)
language plpgsql
as $$
declare 
	start_time timestamp;
	end_time timestamp;
	from_date_var date;
	to_date_var date;
begin
	start_time := current_timestamp;
	from_date_var := date_trunc('month', i_OnDate) - interval '1 month';
	to_date_var := date_trunc('month', i_OnDate) - interval '1 day';
	-- Логирование
	insert into logs.log_table (log_data, log_time)
    values ('Начало заполнения DM.DM_F101_ROUND_F за ' || i_OnDate, start_time);
	-- Удаление существующих записей за дату расчета
	delete from DM.DM_F101_ROUND_F where to_date_var = i_OnDate - interval '1 day';
	-- Вставка новых данных
	insert into DM.DM_F101_ROUND_F (
		from_date, to_date, chapter, ledger_account, characteristic, 
		balance_in_rub, balance_in_val, balance_in_total, 
		turn_deb_rub, turn_deb_val, turn_deb_total, 
		turn_cre_rub, turn_cre_val, turn_cre_total, 
		balance_out_rub, balance_out_val, balance_out_total)
	select 
		from_date_var, --первый день отчетного периода
		to_date_var, --последний день отчетного периода
		la.chapter, --глава из справочника балансовых счетов (DS.MD_LEDGER_ACCOUNT_S)
		substr(a.account_number, 1, 5) as ledger_account, --балансовый счет второго порядка
		a.char_type as characteristic, --характеристика счета
		
		--сумма остатков в рублях
		sum (case
				when a.currency_code in ('810', '643') then b_in.balance_out_rub else 0
			end) as balance_in_rub, --для рублевых счетов
		sum (case 
				when a.currency_code not in ('810', '643') then b_in.balance_out else 0
			end) as balance_in_val, --для всех счетов, кроме рублевых
		sum (b_in.balance_out_rub) as balance_in_total, --для всех счетов
		
		--сумма дебетовых оборотов в рублях
		sum (case
				when a.currency_code in ('810', '643') then coalesce(t.debet_amount_rub, 0) else 0
			end) as turn_deb_rub, --для рублевых счетов
		sum (case
				when a.currency_code not in ('810', '643') then coalesce(t.debet_amount_rub, 0) else 0
			end) as turn_deb_val, --для всех счетов, кроме рублевых
		sum (coalesce(t.debet_amount_rub)) as turn_deb_total, --для всех счетов
		
		--сумма кредитовых оборотов в рублях
		sum (case
				when a.currency_code in ('810', '643') then coalesce(t.credit_amount_rub) else 0
			end) as turn_cre_rub, --для рублевых счетов
		sum (case
				when a.currency_code not in ('810', '643') then coalesce(t.credit_amount_rub) else 0
			end) as turn_cre_val, --для всех счетов, кроме рублевых
		sum (coalesce(t.credit_amount_rub)) as turn_cre_total, --для всех счетов
		
		--сумма остатков в рублях
		sum (case
				when a.currency_code in ('810', '643') then b_out.balance_out_rub else 0
			end) as balance_out_rub, --для рублевых счетов
		sum (case 
				when a.currency_code not in ('810', '643') then b_out.balance_out else 0
			end) as balance_out_val, --для всех счетов, кроме рублевых
		sum (b_out.balance_out_rub) as balance_out_total --для всех счетов
		
	from ds.md_account_d a
	left join ds.md_ledger_account_s la on substr(a.account_number, 1, 5)::int = la.ledger_account 
	left join dm.dm_account_balance_f b_in on a.account_rk = b_in.account_rk and b_in.on_date = from_date_var - interval '1 day'
	left join dm.dm_account_turnover_f t on a.account_rk = t.account_rk and t.on_date between from_date_var and to_date_var
	left join dm_account_balance_f b_out on a.account_rk = b_out.account_rk and b_out.on_date = to_date_var
	where a.data_actual_date <= to_date_var and (a.data_actual_end_date is null or a.data_actual_end_date >= from_date_var)
	group by substr(a.account_number, 1, 5), la.chapter, a.char_type;
	
	-- Логирование
	end_time := current_timestamp;
	insert into logs.log_table (log_data, log_time)
	values ('Окончание заполнения DM.DM_F101_ROUND_F за ' || i_OnDate, end_time);
	
	exception when others then
	end_time := current_timestamp;
	insert into logs.log_table (log_data, log_time)
	values ('Ошибка: ' || SQLERRM, end_time);
	
end;
$$


-- Вызов процедуры для расчета 101 формы
call dm.fill_f101_round_f('2018-02-01'::date);



select * 
from DM.DM_F101_ROUND_F
order by ledger_account;