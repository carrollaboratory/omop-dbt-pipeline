
    
    



select payer_plan_period_end_date
from "dbt"."main_omop"."payer_plan_period"
where payer_plan_period_end_date is null


