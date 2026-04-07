from baseline_common import BaselineSpec, run_baseline


if __name__ == "__main__":
    run_baseline(
        __file__,
        BaselineSpec(
            label="Single Agent Baseline 1",
            result_col="Single Agent Baseline 1",
            include_log_summary=False,
            include_manual_rag=False,
        ),
    )
