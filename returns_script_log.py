from returns_script_functions import fill_return_label




import os
print("cwd:", os.getcwd())
print("db_path:", os.path.abspath("options_data.db"))

fill_return_label(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL 10 MINUTE"
)


fill_return_label(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL 1 HOUR"
)


fill_return_label(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    "AND DATE(base.timestamp) < CURRENT_DATE"
)




fill_return_label(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)"
)




fill_return_label(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL 1 DAY"
)


fill_return_label(
    "opt_ret_exp",
    "DATE(f.timestamp) <= base.expiration_date",
    "AND base.expiration_date <= CURRENT_DATE"
)
























