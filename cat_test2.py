if __name__ == "__main__":
    #import pandas as pd
    import modin.pandas as pd
    from collections import OrderedDict
    cols = [
        "loan_id",
        "monthly_reporting_period",
        "servicer",
        "interest_rate",
        "current_actual_upb",
        "loan_age",
        "remaining_months_to_legal_maturity",
        "adj_remaining_months_to_maturity",
        "maturity_date",
        "msa",
        "current_loan_delinquency_status",
        "mod_flag",
        "zero_balance_code",
        "zero_balance_effective_date",
        "last_paid_installment_date",
        "foreclosed_after",
        "disposition_date",
        "foreclosure_costs",
        "prop_preservation_and_repair_costs",
        "asset_recovery_costs",
        "misc_holding_expenses",
        "holding_taxes",
        "net_sale_proceeds",
        "credit_enhancement_proceeds",
        "repurchase_make_whole_proceeds",
        "other_foreclosure_proceeds",
        "non_interest_bearing_upb",
        "principal_forgiveness_upb",
        "repurchase_make_whole_proceeds_flag",
        "foreclosure_principal_write_off_amount",
        "servicing_activity_indicator",
    ]

    dtypes = OrderedDict(
        [
            ("loan_id", "int64"),
            ("monthly_reporting_period", "datetime64"),
            ("servicer", "category"),
            ("interest_rate", "float64"),
            ("current_actual_upb", "float64"),
            ("loan_age", "float64"),
            ("remaining_months_to_legal_maturity", "float64"),
            ("adj_remaining_months_to_maturity", "float64"),
            ("maturity_date", "datetime64"),
            ("msa", "float64"),
            ("current_loan_delinquency_status", "int32"),
            ("mod_flag", "category"),
            ("zero_balance_code", "category"),
            ("zero_balance_effective_date", "datetime64"),
            ("last_paid_installment_date", "datetime64"),
            ("foreclosed_after", "datetime64"),
            ("disposition_date", "datetime64"),
            ("foreclosure_costs", "float64"),
            ("prop_preservation_and_repair_costs", "float64"),
            ("asset_recovery_costs", "float64"),
            ("misc_holding_expenses", "float64"),
            ("holding_taxes", "float64"),
            ("net_sale_proceeds", "float64"),
            ("credit_enhancement_proceeds", "float64"),
            ("repurchase_make_whole_proceeds", "float64"),
            ("other_foreclosure_proceeds", "float64"),
            ("non_interest_bearing_upb", "float64"),
            ("principal_forgiveness_upb", "float64"),
            ("repurchase_make_whole_proceeds_flag", "category"),
            ("foreclosure_principal_write_off_amount", "float64"),
            ("servicing_activity_indicator", "category"),
        ]
    )
    all_but_dates = {
        col: valtype for (col, valtype) in dtypes.items() if valtype != "datetime64"
    }
    dates_only = [
        col for (col, valtype) in dtypes.items() if valtype == "datetime64"
    ]
    df = pd.read_csv("perf3.txt",
                     delimiter="|", names=cols, dtype=all_but_dates, parse_dates=dates_only)
    print(df.dtypes)
    s = df["servicer"]
    print("dtype = ", s.dtype)
    print("dtypes = ", s.dtypes)
    print(s)
