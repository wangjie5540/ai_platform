from src.sample.sample_selection_dixiaohu.sample_select import start_sample_selection

dixiao_before_days = 10
dixiao_after_days = 10
right_zc_threshold = 120000
avg_zc_threshold = 120000
event_tag = "login"
sample_table_name = start_sample_selection(
    dixiao_before_days=dixiao_before_days,
    dixiao_after_days=dixiao_after_days,
    right_zc_threshold=right_zc_threshold,
    avg_zc_threshold=avg_zc_threshold,
    event_tag=event_tag,
)


