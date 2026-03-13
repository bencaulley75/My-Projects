import streamlit as st
import pandas as pd
import plotly.express as px
# from cleaner import auto_clean
from question_generator import generate_questions
from cleaner import (
    auto_clean,
    detect_problem_columns,
    detect_outliers,
    apply_outlier_treatment
)

st.set_page_config(page_title="Automated Data Analysis", layout="wide")

st.title("📊 Automated Data Analysis v1")
st.write("Upload any CSV file and generate automatic cleaning insights and KPIs.")

# -----------------------------------
# File Upload
# -----------------------------------

uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

if uploaded_file is not None:

    df = pd.read_csv(uploaded_file)

    # -----------------------------------
    # Original Dataset Preview
    # -----------------------------------

    st.subheader("📄 Original Dataset Preview")
    st.dataframe(df)
    st.write("Rows:", df.shape[0])
    st.write("Columns:", df.shape[1])

    # -----------------------------------
    # Basic Cleaning
    # -----------------------------------

    df = auto_clean(df)

    st.subheader("🧹 Cleaned Dataset Preview")
    st.dataframe(df)
    st.write("Rows after cleaning:", df.shape[0])
    st.write("Columns after cleaning:", df.shape[1])

    # -----------------------------------
    # Column Selection for Analysis
    # -----------------------------------

    st.subheader("🔍 Select Columns for Analysis")

    all_columns = df.columns.tolist()

    selected_columns = st.multiselect(
        "Choose the columns you want to include in your analysis:",
        options=all_columns,
        default=all_columns
    )

    if len(selected_columns) == 0:
        st.warning("Please select at least one column to continue.")
        st.stop()

    df = df[selected_columns]
    st.success(f"{len(selected_columns)} column(s) selected for analysis.")
    st.dataframe(df.head())
    

    # -----------------------------------
    # Outlier Detection
    # -----------------------------------

    st.subheader("📉 Outlier Detection")

    outliers = detect_outliers(df)

    if len(outliers) > 0:

        for col, info in outliers.items():
            st.write(f"{col}: {info['count']} extreme values detected")

        outlier_action = st.radio(
            "How would you like to handle outliers?",
            ["Keep", "Cap (Recommended)", "Remove Rows"]
        )

        if st.button("Apply Outlier Treatment"):

            if outlier_action == "Cap (Recommended)":
                df = apply_outlier_treatment(df, outliers, method="cap")
                st.success("Outliers capped successfully.")

            elif outlier_action == "Remove Rows":
                df = apply_outlier_treatment(df, outliers, method="remove")
                st.success("Outlier rows removed.")

            st.dataframe(df)

    else:
        st.success("No major outliers detected.")

    # -----------------------------------
    # Smart KPI Detection
    # -----------------------------------

    st.subheader("📌 Smart KPI Detection")

    numeric_cols = df.select_dtypes(include='number').columns

    if len(numeric_cols) > 0:

        variance_scores = df[numeric_cols].var().sort_values(ascending=False)
        top_kpis = variance_scores.head(3).index

        cols = st.columns(len(top_kpis))

        for i, col in enumerate(top_kpis):

            total_value = df[col].sum(skipna=True)
            avg_value = df[col].mean(skipna=True)

            with cols[i]:
                st.metric(
                    label=f"{col} (Total)",
                    value=f"{total_value:,.2f}"
                )
                st.metric(
                    label=f"{col} (Average)",
                    value=f"{avg_value:,.2f}"
                )

    else:
        st.warning("No numeric columns available for KPI generation.")

    # -------------------------
    # Generated Business Questions
    # -------------------------

    # st.subheader("🧠 Generated Business Questions")

    # questions = generate_questions(df)
    # for q in questions:
    #     st.write("-", q)

    # -------------------------
    # Numeric Visualizations
    # -------------------------

    # if len(numeric_cols) > 0:
    #     st.subheader("📈 Numeric Analysis")

    #     selected_numeric = st.selectbox("Select Numeric Column", numeric_cols)

    #     colA, colB = st.columns(2)

    #     with colA:
    #         st.write("Distribution")
    #         fig_hist = px.histogram(df, x=selected_numeric)
    #         st.plotly_chart(fig_hist, use_container_width=True)

    #     with colB:
    #         st.write("Outlier Detection (Boxplot)")
    #         fig_box = px.box(df, y=selected_numeric)
    #         st.plotly_chart(fig_box, use_container_width=True)

    # -------------------------
    # Correlation Heatmap
    # -------------------------

    # if len(numeric_cols) > 1:
    #     st.subheader("🔎 Correlation Heatmap")

    #     corr = df[numeric_cols].corr()

    #     fig_corr = px.imshow(
    #         corr,
    #         text_auto=True,
    #         aspect="auto"
    #     )

    #     st.plotly_chart(fig_corr, use_container_width=True)

    # -------------------------
    # Category vs Numeric Analysis
    # -------------------------

    # cat_cols = df.select_dtypes(include='object').columns

    # if len(cat_cols) > 0 and len(numeric_cols) > 0:
    #     st.subheader("📊 Category Performance Analysis")

    #     cat_selected = st.selectbox("Select Category Column", cat_cols)
    #     num_selected = st.selectbox("Select Numeric Metric", numeric_cols)

    #     grouped = df.groupby(cat_selected)[num_selected].mean().reset_index()

    #     fig_cat_num = px.bar(
    #         grouped,
    #         x=cat_selected,
    #         y=num_selected
    #     )

    #     st.plotly_chart(fig_cat_num, use_container_width=True)

  
    # Categorical Analysis


    # if len(cat_cols) > 0:
    #     st.subheader("📊 Categorical Distribution Analysis")

    #     selected_cat = st.selectbox("Select Categorical Column", cat_cols, key="cat_dist")

    #     value_counts = df[selected_cat].value_counts().reset_index()
    #     value_counts.columns = [selected_cat, "count"]

    #     top_n = st.slider("Show Top N Categories", 5, 30, 10)

    #     top_categories = value_counts.head(top_n)

    #     colC, colD = st.columns(2)

    #     with colC:
    #         st.write("Category Frequency (Bar Chart)")
    #         fig_bar = px.bar(
    #             top_categories,
    #             x=selected_cat,
    #             y="count"
    #         )
    #         st.plotly_chart(fig_bar, use_container_width=True)

    #     with colD:
    #         st.write("Category Proportion (Pie Chart)")
    #         fig_pie = px.pie(
    #             top_categories,
    #             names=selected_cat,
    #             values="count"
    #         )
    #         st.plotly_chart(fig_pie, use_container_width=True)

    # # -------------------------
    # # Two Categorical Comparison
    # # -------------------------

    # if len(cat_cols) > 1:
    #     st.subheader("🔎 Categorical vs Categorical Comparison")

    #     cat1 = st.selectbox("Select First Category", cat_cols, key="cat1")
    #     cat2 = st.selectbox("Select Second Category", cat_cols, key="cat2")

    #     cross_tab = pd.crosstab(df[cat1], df[cat2]).reset_index()

    #     fig_cross = px.bar(
    #         cross_tab,
    #         x=cat1,
    #         y=cross_tab.columns[1:],
    #         barmode="group"
    #     )

    #     st.plotly_chart(fig_cross, use_container_width=True)

    # # -------------------------
    # # Statistical Summary
    # # -------------------------

    # st.subheader("📄 Statistical Summary")
    # st.dataframe(df.describe(include="all"))