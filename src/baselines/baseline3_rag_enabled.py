from baseline_common import BaselineSpec, run_baseline


if __name__ == "__main__":
    run_baseline(
        __file__,
        BaselineSpec(
            label="Single Agent Baseline 3",
            result_col="Single Agent Baseline 3",
            include_log_summary=True,
            include_manual_rag=True,
        ),
    )
