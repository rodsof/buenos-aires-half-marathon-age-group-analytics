import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from scipy import stats
    from google.cloud import bigquery
    import re
    from plotly.subplots import make_subplots

    return bigquery, go, mo, px, stats


@app.cell
def _(bq_project, bq_dataset, bq_table, mo):
    return mo.vstack(
        [
            mo.md("# Buenos Aires Half Marathon 21K"),
            mo.md(
                f"### BigQuery Source: {bq_project.value}.{bq_dataset.value}.{bq_table.value}"
            ),
        ]
    )


@app.cell
def _(mo):
    bq_project = mo.ui.text(
        value="moonlit-state-486723-r7",
        label="GCP Project ID",
        full_width=True,
    )
    bq_dataset = mo.ui.text(
        value="staging",
        label="Dataset",
    )
    bq_table = mo.ui.text(
        value="half_marathon",
        label="Table",
    )

    return bq_dataset, bq_project, bq_table


@app.cell
def _(bigquery, bq_dataset, bq_project, bq_table, mo):
    _full_table = f"`{bq_project.value}.{bq_dataset.value}.{bq_table.value}`"

    with mo.status.spinner(f"Loading data from {_full_table}…"):
        _client = bigquery.Client(project=bq_project.value)
        _query = f"SELECT * FROM {_full_table}"
        df_raw = _client.query(_query).to_dataframe()

    mo.md(f"**Loaded {len(df_raw):,} rows from BigQuery.**")
    return (df_raw,)


@app.cell
def _(df_raw):
    def _find_col(df, *keywords):
        for kw in keywords:
            matches = [c for c in df.columns if kw in c.lower()]
            if matches:
                return matches[0]
        return None

    TIME_COL = _find_col(df_raw, "chip_time_hours", "seconds", "time", "net", "chip")
    GENDER_COL = _find_col(df_raw, "gender", "sex")
    AGE_COL = _find_col(df_raw, "age_group", "age", "grupo", "group")
    YEAR_COL = _find_col(df_raw, "year", "edition", "año")

    # Use staging chip_time_hours directly if available, otherwise try parsing
    if "chip_time_hours" in df_raw.columns:
        df = df_raw.dropna(subset=["chip_time_hours"]).copy()
        df = df[df["chip_time_hours"] > 0]
    elif TIME_COL:

        def _parse(s):
            try:
                parts = [float(x) for x in str(s).strip().split(":")]
                if len(parts) == 3:
                    return parts[0] * 3600 + parts[1] * 60 + parts[2]
                if len(parts) == 2:
                    return parts[0] * 60 + parts[1]
            except Exception:
                pass
            return None

        df = df_raw.copy()
        df["chip_time_hours"] = df[TIME_COL].apply(_parse)
        df = df.dropna(subset=["chip_time_hours"])
        df = df[df["chip_time_hours"] > 0]
    else:
        df = df_raw.copy()
        df["chip_time_hours"] = None
    return AGE_COL, GENDER_COL, YEAR_COL, df


@app.cell
def _(GENDER_COL, YEAR_COL, df, mo):
    _year_opts = (
        ["All"]
        + sorted(df[YEAR_COL].dropna().astype(int).astype(str).unique().tolist())
        if YEAR_COL
        else ["All"]
    )
    _gender_opts = (
        ["All"] + sorted(df[GENDER_COL].dropna().unique().tolist())
        if GENDER_COL
        else ["All"]
    )

    year_filter = mo.ui.dropdown(
        options=_year_opts, value="All", label="Year / Edition"
    )
    gender_filter = mo.ui.dropdown(options=_gender_opts, value="All", label="Gender")

    mo.hstack([year_filter, gender_filter])
    return gender_filter, year_filter


@app.cell
def _(GENDER_COL, YEAR_COL, df, gender_filter, year_filter):
    df_f = df.copy()

    if YEAR_COL and year_filter.value != "All":
        df_f = df_f[df_f[YEAR_COL].astype(str) == year_filter.value]

    if GENDER_COL and gender_filter.value != "All":
        df_f = df_f[df_f[GENDER_COL] == gender_filter.value]
    return (df_f,)


@app.cell
def _(df_f, mo):
    def _fmt(hours):
        total_seconds = int(float(hours) * 3600)
        h, r = divmod(total_seconds, 3600)
        m, s = divmod(r, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    stats_view = None

    if df_f.empty:
        stats_view = mo.md("> No rows match the current filters.")
    else:
        stats_view = mo.hstack(
            [
                mo.stat(label="Finishers", value=f"{len(df_f):,}"),
                mo.stat(
                    label="Median time", value=_fmt(df_f["chip_time_hours"].median())
                ),
                mo.stat(label="Fastest", value=_fmt(df_f["chip_time_hours"].min())),
                mo.stat(label="Slowest", value=_fmt(df_f["chip_time_hours"].max())),
            ]
        )
    return (stats_view,)


@app.cell
def _(stats_view):
    stats_view
    return


@app.cell
def _(AGE_COL, df_f, go, px):
    if AGE_COL:

        def _age_lower_bound(label):
            s = str(label).strip()
            m = re.search(r"(\d{1,2})\D+(\d{1,2})", s)
            if m:
                return int(m.group(1))
            m = re.search(r"(\d{1,2})\s*\+", s)
            if m:
                return int(m.group(1))
            return 999

        _agg = df_f.groupby(AGE_COL)["chip_time_hours"].median().reset_index()
        _agg["_age_lb"] = _agg[AGE_COL].map(_age_lower_bound)
        _agg = _agg.sort_values(["_age_lb", AGE_COL])
        fig_age = px.bar(
            _agg,
            x=AGE_COL,
            y="chip_time_hours",
            color="chip_time_hours",
            color_continuous_scale="Blues_r",
            labels={AGE_COL: "Age group", "chip_time_hours": "Median finish (hours)"},
            title="Median finish time by age group",
        )
    else:
        fig_age = go.Figure().update_layout(title="Age-group column not found")
    return (fig_age,)


@app.cell
def _(fig_age, mo):
    mo.vstack(
        [
            mo.md(
                "This bar chart compares the median finish time for each age group in the current selection. "
                "Lower bars indicate faster typical performance, which makes it easier to spot which age ranges "
                "tend to finish sooner."
            ),
            fig_age,
        ]
    )
    return fig_age


@app.cell
def _(GENDER_COL, df_f, go, px):
    if GENDER_COL:
        fig_gender = px.box(
            df_f,
            x=GENDER_COL,
            y="chip_time_hours",
            color=GENDER_COL,
            points="outliers",
            labels={GENDER_COL: "Gender", "chip_time_hours": "Finish time (s)"},
            title="Finish-time distribution by gender",
        )
    else:
        fig_gender = go.Figure().update_layout(title="Gender column not found")
    return (fig_gender,)


@app.cell
def _(fig_gender, mo):
    mo.vstack(
        [
            mo.md(
                "This box plot summarizes finish-time variability by gender. The center line marks the median, "
                "the box captures the middle 50% of results, and outliers highlight unusually fast or slow finishers."
            ),
            fig_gender,
        ]
    )
    return


@app.cell
def _(AGE_COL, GENDER_COL, df_f, mo):
    def _age_lower_bound(label):
        s = str(label).strip()
        m = re.search(r"(\d{1,2})\D+(\d{1,2})", s)
        if m:
            return int(m.group(1))
        m = re.search(r"(\d{1,2})\s*\+", s)
        if m:
            return int(m.group(1))
        return None

    def _fmt(hours):
        total_seconds = int(float(hours) * 3600)
        h, r = divmod(total_seconds, 3600)
        m, sec = divmod(r, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"

    if not AGE_COL or not GENDER_COL:
        pairwise_best_view = mo.md(
            "> ⚠️ Age-group or gender column not detected — cannot build pairwise best-per-category comparison."
        )

    _tmp = df_f[[AGE_COL, GENDER_COL, "chip_time_hours"]].dropna().copy()
    if _tmp.empty:
        pairwise_best_view = mo.md("> ⚠️ No rows available after current filters.")

    _tmp["_age_lb"] = _tmp[AGE_COL].map(_age_lower_bound)
    _tmp = _tmp.dropna(subset=["_age_lb"])
    if _tmp.empty:
        pairwise_best_view = mo.md("> ⚠️ No parseable age brackets found.")

    _best = (
        _tmp.groupby([GENDER_COL, AGE_COL], as_index=False)["chip_time_hours"]
        .min()
        .rename(columns={"chip_time_hours": "best_time_hours"})
    )
    _best["_age_lb"] = _best[AGE_COL].map(_age_lower_bound)
    _best = _best.sort_values([GENDER_COL, "_age_lb", AGE_COL])

    _best_header = f"| {GENDER_COL} | Age bracket | Best time |\n|---|---|---|\n"
    _best_rows = []
    for _, row in _best.iterrows():
        _best_rows.append(
            f"| {row[GENDER_COL]} | {row[AGE_COL]} | {_fmt(row['best_time_hours'])} |"
        )

    _pb_pairs = []
    for _pb_gender, _pb_gdf in _best.groupby(GENDER_COL):
        _pb_gdf = _pb_gdf.sort_values(["_age_lb", AGE_COL])
        _pb_recs = _pb_gdf.to_dict("records")

        for i in range(len(_pb_recs)):
            for j in range(i + 1, len(_pb_recs)):
                _pb_best_younger = _pb_recs[i]
                _pb_best_older = _pb_recs[j]
                _pb_delta_minutes = (
                    _pb_best_older["best_time_hours"]
                    - _pb_best_younger["best_time_hours"]
                ) * 60.0

                _pb_pairs.append(
                    {
                        "gender": _pb_gender,
                        "younger": _pb_best_younger[AGE_COL],
                        "older": _pb_best_older[AGE_COL],
                        "delta_minutes": _pb_delta_minutes,
                    }
                )

    if not _pb_pairs:
        pairwise_best_view = mo.md(
            "### Pairwise best-per-age comparison by gender\n\n"
            "Not enough age brackets inside each gender to compute pairwise comparisons."
        )

    _pb_df = pd.DataFrame(_pb_pairs)

    _pb_genders = sorted(_pb_df["gender"].astype(str).unique().tolist())
    _pb_n = len(_pb_genders)

    _pb_fig = make_subplots(
        rows=1,
        cols=_pb_n,
        subplot_titles=[str(g) for g in _pb_genders],
        horizontal_spacing=0.08,
    )

    _pb_global_abs = max(
        abs(_pb_df["delta_minutes"].min()), abs(_pb_df["delta_minutes"].max())
    )
    _pb_global_abs = float(_pb_global_abs) if _pb_global_abs > 0 else 1.0

    for _pb_col, _pb_gender in enumerate(_pb_genders, start=1):
        _pb_sub = _pb_df[_pb_df["gender"].astype(str) == str(_pb_gender)].copy()

        # Keep a stable age order based on lower bound from _best
        _pb_order = (
            _best[_best[GENDER_COL].astype(str) == str(_pb_gender)]
            .sort_values(["_age_lb", AGE_COL])[AGE_COL]
            .astype(str)
            .tolist()
        )

        _pb_pivot = _pb_sub.pivot(
            index="younger", columns="older", values="delta_minutes"
        )
        _pb_y = [a for a in _pb_order if a in _pb_pivot.index]
        _pb_x = [a for a in _pb_order if a in _pb_pivot.columns]
        _pb_z = _pb_pivot.reindex(index=_pb_y, columns=_pb_x).values

        _pb_fig.add_trace(
            go.Heatmap(
                x=_pb_x,
                y=_pb_y,
                z=_pb_z,
                coloraxis="coloraxis",
                hovertemplate=(
                    "Gender: "
                    + str(_pb_gender)
                    + "<br>Younger: %{y}"
                    + "<br>Older: %{x}"
                    + "<br>Delta (older - younger): %{z:.2f} min"
                    + "<extra></extra>"
                ),
            ),
            row=1,
            col=_pb_col,
        )

        _pb_fig.update_xaxes(
            title_text="Older bracket", tickangle=-35, row=1, col=_pb_col
        )
        _pb_fig.update_yaxes(title_text="Younger bracket", row=1, col=_pb_col)

    _pb_fig.update_layout(
        title="Pairwise best-only comparison by gender (minutes)",
        coloraxis=dict(
            colorscale="RdBu",
            cmin=-_pb_global_abs,
            cmax=_pb_global_abs,
            cmid=0,
            colorbar=dict(title="Older - Younger (min)"),
        ),
        height=500,
    )

    pairwise_best_view = mo.vstack(
        [
            mo.md(
                "### Pairwise age-bracket comparison using only category winners\n\n"
                "Color is delta minutes = older best time - younger best time.\n"
                "Positive values mean the younger bracket winner is faster."
            ),
            _pb_fig,
        ]
    )
    return (pairwise_best_view,)


@app.cell
def _(pairwise_best_view):
    pairwise_best_view
    return


@app.cell
def _(mo):
    return mo.md("""
## Hypotheses

- **H₀:**  For each age bracket, its finish-time distribution is not lower than the pooled older brackets.
- **H₁:** For a given age bracket, its finish-time distribution is lower than pooled older brackets.

- **Gender hypothesis:** Performance may differ by gender.
        """)


@app.cell
def _(AGE_COL, df_f, mo, stats):
    hypothesis_view = None

    if not AGE_COL:
        hypothesis_view = mo.md(
            "> ⚠️ Age-group column not detected — cannot run hypothesis test."
        )

    def _age_lower_bound(label):
        s = str(label).strip()
        # Matches: 18-29, 18_29, 18 a 29, 18–29
        m = re.search(r"(\d{1,2})\D+(\d{1,2})", s)
        if m:
            return int(m.group(1))
        # Matches: 60+
        m = re.search(r"(\d{1,2})\s*\+", s)
        if m:
            return int(m.group(1))
        return None

    def _fmt(hours):
        total_seconds = int(float(hours) * 3600)
        h, r = divmod(total_seconds, 3600)
        m, sec = divmod(r, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"

    # Keep only rows with parseable age groups
    _tmp = df_f[[AGE_COL, "chip_time_hours"]].dropna().copy()
    _tmp["age_label"] = _tmp[AGE_COL].astype(str)
    _tmp["age_lb"] = _tmp["age_label"].map(_age_lower_bound)
    _tmp = _tmp.dropna(subset=["age_lb"])

    if _tmp.empty:
        hypothesis_view = mo.md("> ⚠️ No parseable age groups found.")

    # Ordered unique groups by lower bound
    _groups = (
        _tmp[["age_label", "age_lb"]]
        .drop_duplicates()
        .sort_values(["age_lb", "age_label"])["age_label"]
        .tolist()
    )

    _results = []
    for g in _groups:
        g_lb = _age_lower_bound(g)
        older_labels = [
            x
            for x in _groups
            if _age_lower_bound(x) is not None and _age_lower_bound(x) > g_lb
        ]

        if not older_labels:
            continue

        younger = _tmp.loc[_tmp["age_label"] == g, "chip_time_hours"]
        older = _tmp.loc[_tmp["age_label"].isin(older_labels), "chip_time_hours"]

        if len(younger) < 2 or len(older) < 2:
            continue

        u, p = stats.mannwhitneyu(younger, older, alternative="less")
        _results.append(
            {
                "group": g,
                "younger_n": len(younger),
                "older_n": len(older),
                "younger_median": younger.median(),
                "older_median": older.median(),
                "u": u,
                "p": p,
            }
        )

    if not _results:
        hypothesis_view = mo.md("> ⚠️ Not enough data to compare age brackets.")

    alpha = 0.05
    m = len(_results)  # number of tests for Bonferroni
    for r in _results:
        r["p_bonf"] = min(r["p"] * m, 1.0)
        r["reject"] = r["p_bonf"] < alpha

    header = (
        "| Younger group | Younger median | Older median (pooled) | n_younger | n_older | p (raw) | p (Bonf.) | Verdict |\n"
        "|---|---|---|---:|---:|---:|---:|---|\n"
    )

    rows = []
    for r in _results:
        _verdict = (
            "✅ younger is faster" if r["reject"] else "❌ no significant evidence"
        )
        rows.append(
            f"| {r['group']} | {_fmt(r['younger_median'])} | {_fmt(r['older_median'])} | "
            f"{r['younger_n']:,} | {r['older_n']:,} | {r['p']:.4f} | {r['p_bonf']:.4f} | {_verdict} |"
        )

    hypothesis_view = mo.md(
        "### Pairwise age-bracket tests: younger vs all older\n\n"
        f"{header}{chr(10).join(rows)}\n\n"
        "Test: one-tailed Mann-Whitney U (alternative = younger has lower finish time), "
        "with Bonferroni correction for multiple comparisons."
    )
    return (hypothesis_view,)


@app.cell
def _(hypothesis_view):
    hypothesis_view
    return


if __name__ == "__main__":
    app.run()
