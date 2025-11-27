select
    s.user_pseudo_id,
    s.session_date,
    f.first_click_source,
    f.first_click_medium,
    f.first_click_campaign,
    l.last_click_source,
    l.last_click_medium,
    l.last_click_campaign
from {{ ref('fct_attribution_first') }} f
join {{ ref('fct_attribution_last') }} l
  using (user_pseudo_id, session_date)
join {{ ref('int_sessions') }} s
  using (user_pseudo_id, session_date)
