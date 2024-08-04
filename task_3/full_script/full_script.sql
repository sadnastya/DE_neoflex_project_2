
--П.1. Верным account_in_sum считается account_out_sum предыдущего дня
select ab.account_rk, 
	ab.effective_date,
	coalesce((select account_out_sum 
				from rd.account_balance
				where effective_date=ab.effective_date-INTERVAL '1 day' and account_rk=ab.account_rk),
					ab.account_in_sum) as account_in_sum,
	ab.account_out_sum
from rd.account_balance ab;


--П.2. Верным account_out_sum считается account_in_sum следующего дня
select ab.account_rk, 
	ab.effective_date,
	ab.account_in_sum,
	coalesce((select account_in_sum 
				from rd.account_balance
				where effective_date=ab.effective_date+INTERVAL '1 day' and account_rk=ab.account_rk),
					ab.account_out_sum) as account_out_sum
from rd.account_balance ab;


--Обновление rd.account_balance
with correct_balance as (
    select ab.account_rk,
        ab.effective_date + INTERVAL '1 day' AS next_day,
        ab.account_out_sum
    from
        rd.account_balance ab
)
update rd.account_balance ab
set account_in_sum = COALESCE(cb.account_out_sum, ab.account_in_sum)
from correct_balance cb
where ab.account_rk = cb.account_rk
    and ab.effective_date = cb.next_day;
   
   

   
   
--Процедура для заполнения витрины из таблиц-источников
CREATE OR REPLACE PROCEDURE dm.fill_account_balance_turnover()
as $$
	begin
		truncate table dm.account_balance_turnover;
		INSERT INTO dm.account_balance_turnover(account_rk, currency_name, department_rk, effective_date, account_in_sum, account_out_sum)
		SELECT a.account_rk,
	   		COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
	   		a.department_rk,
	   		ab.effective_date,
	   		ab.account_in_sum,
	   		ab.account_out_sum
		FROM rd.account a
		LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
		LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;	
	end;
$$ language plpgsql;

--Вызов процедуры
call dm.fill_account_balance_turnover();
   

--Запросы для проверки результата выполнения dag:
/*select * from dm.account_balance_turnover order by account_rk, effective_date;

select * from rd.account_balance order by account_rk, effective_date;

select * from dm.dict_currency;*/
   
   
   
   
   
   


				
				
				
				




