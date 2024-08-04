with correct_balance as(
    select
        ab.account_rk,
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
