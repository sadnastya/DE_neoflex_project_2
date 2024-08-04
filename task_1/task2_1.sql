--Найдем дубликаты
select 
    client_rk, 
    effective_from_date, 
    COUNT(*) as duplicates
from dm.client
group by
    client_rk, 
    effective_from_date
having COUNT(*) > 1;
 
--Удалим дублированные строки
delete from dm.client
where ctid not in (
  select min(ctid)
  from dm.client
  group by client_rk, effective_from_date
);
   
