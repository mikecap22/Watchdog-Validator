#app.py

#This is the Streamlit Front End

#app.py
#This is the Streamlit Front End

import streamlit as st
import pandas as pd
from watchdog_validator import WatchdogValidator
from sqlalchemy import create_engine
from fpdf import FPDF
from datetime import datetime

# --- SECTION 1: PAGE SETUP ---
st.set_page_config(
    page_title="Watchdog | Data Quality Gate", 
    page_icon="🐕‍🦺",
    layout="wide"
)

# --- SECTION 2: VISUAL STYLING ---
st.markdown("""
    <style>
    .block-container {
        padding-top: 1rem;
        max-width: 90%;
    }
    /* THE TRUE CENTER FIX */
    .brand-wrapper {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        width: 100%;
        text-align: center;
        margin-bottom: 20px;
    }
    .logo-img {
        width: 250px;
    }
    /* Button Styling */
    div.stButton > button:first-child {
        background-color: #0068c9;
        color: white;
        height: 3.5em;
        width: 100%;
        border-radius: 10px;
        border: none;
        font-size: 20px;
        font-weight: bold;
        margin-top: 20px;
        transition: 0.3s;
    }
    div.stButton > button:hover {
        background-color: #ff4b4b;
    }
    [data-testid="stMetricValue"] {
        font-size: 28px;
        color: #1f77b4;
    }
    </style>""", unsafe_allow_html=True)

# --- SECTION 3: HEADER & BRANDING (NEW CENTERED LOGIC) ---
st.markdown(f"""
    <div class="brand-wrapper">
        <img src="data:image/png;base64,{st.image("watchdog_header.png", width=250)}" class="logo-img">
        <h1 style="margin-top: 10px;">Watchdog Validator</h1>
        <p style="font-size: 1.2rem; font-weight: 500; color: #333;">
            The Automated Data Gatekeeper: Validating and Protecting your Pipelines.
        </p>
    </div>
""", unsafe_allow_html=True)


with st.columns([1, 2, 1])[1]: # Centers the expander specifically
    with st.expander("🌐 Industry Applications"):
        st.markdown("""
        * **E-Commerce**: Validate transaction integrity and prevent negative pricing.
        * **Healthcare**: Ensure patient records have non-null IDs.
        * **Finance**: Sanitize CSV; Excel ledgers before importing.
        * **Marketing**: Cleanse lead lists by ensuring unique identifiers.
        """)
    
st.markdown("---")

# --- SECTION 4: PDF REPORT GENERATION ---
def generate_pdf(summary_data, rules):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 16)
    pdf.cell(200, 10, "Watchdog Data Validation Report", ln=True, align="C")
    pdf.set_font("Arial", "", 12)
    pdf.cell(200, 10, f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True, align="C")
    pdf.ln(10)
    
    pdf.set_font("Arial", "B", 14)
    pdf.cell(200, 10, "Executive Summary", ln=True)
    pdf.set_font("Arial", "", 12)
    pdf.cell(100, 10, f"Total Records: {summary_data['total']}")
    pdf.cell(100, 10, f"Clean Records: {summary_data['clean']}", ln=True)
    pdf.cell(100, 10, f"Flagged Records: {summary_data['failed']}")
    pdf.cell(100, 10, f"Pass Rate: {summary_data['rate']}%", ln=True)
    pdf.ln(5)
    
    pdf.set_font("Arial", "B", 14)
    pdf.cell(200, 10, "Rules Applied", ln=True)
    pdf.set_font("Arial", "", 11)
    for rule in rules:
        pdf.cell(200, 8, f"- {rule}", ln=True)
    
    return pdf.output(dest="S").encode("latin-1")

# --- SECTION 5: DATA SOURCE SELECTION ---
st.write("### 📂 1. Source Selection")
data_source = st.radio("Select Data Source", ["CSV", "Excel", "SQL Database"], horizontal=True)

df = None

if data_source == "SQL Database":
    with st.expander("Database Connection & SQL Script", expanded=True):
        db_uri = st.text_input("Connection URI", placeholder="postgresql://user:password@host:port/dbname")
        sql_input_method = st.radio("Input Method:", ["Table Name", "Upload .sql File", "Manual Query"], horizontal=True)
        
        query = ""
        if sql_input_method == "Table Name":
            t_name = st.text_input("Table Name", value="transactions")
            query = f"SELECT * FROM {t_name} LIMIT 5000"
        elif sql_input_method == "Upload .sql File":
            sql_file = st.file_uploader("Upload Script", type=["sql"])
            if sql_file:
                query = sql_file.read().decode("utf-8")
        else:
            query = st.text_area("SQL Editor", value="SELECT * FROM transactions LIMIT 5000")

        if st.button("🔗 Execute & Pull Data"):
            if db_uri and query:
                try:
                    engine = create_engine(db_uri)
                    df = pd.read_sql(query, engine)
                    st.session_state['df'] = df
                    st.success(f"Success! {len(df)} rows pulled.")
                except Exception as e:
                    st.error(f"Connection failed: {e}")
else:
    file_types = ['csv'] if data_source == "CSV" else ['xlsx', 'xls']
    uploaded_file = st.file_uploader(f"Upload {data_source} file", type=file_types)
    if uploaded_file:
        df = pd.read_csv(uploaded_file) if data_source == "CSV" else pd.read_excel(uploaded_file)
        st.session_state['df'] = df

# --- SECTION 6: VALIDATION CONFIGURATION ---
if 'df' in st.session_state and st.session_state['df'] is not None:
    working_df = st.session_state['df']
    st.write("### ⚙️ 2. Configure Validation Rules")
    cols = working_df.columns.tolist()
    
    c1, c2, c3 = st.columns(3)
    with c1: id_col = st.selectbox("Identifier Column", cols, index=0)
    with c2: val_col = st.selectbox("Value/Amount Column", cols, index=1 if len(cols)>1 else 0)
    with c3: user_col = st.selectbox("User/Category Column", cols, index=2 if len(cols)>2 else 0)

    with st.expander("Adjust Quality Rules", expanded=True):
        check_nulls = st.checkbox("Reject Null Values in ID/User", value=True)
        check_negatives = st.checkbox("Reject Negative Amounts", value=True)
        check_uniques = st.checkbox("Ensure IDs are Unique", value=False)
    
    # --- SECTION 7: RUN VALIDATION & DISPLAY RESULTS ---
    if st.button("🚀 RUN VALIDATION ENGINE"):
        try:
            validator = WatchdogValidator(working_df)
            rules_applied = []
            
            if check_nulls:
                validator.add_not_null_validation(id_col)
                validator.add_not_null_validation(user_col)
                rules_applied.extend([f"{id_col} not null", f"{user_col} not null"])
            if check_negatives:
                validator.add_price_validation(val_col, min_value=0)
                rules_applied.append(f"{val_col} >= 0")
            if check_uniques:
                validator.add_unique_validation(id_col)
                rules_applied.append(f"{id_col} unique")
            
            results, df_clean, df_failed = validator.run_validation_all()
            
            pass_rate = round((len(df_clean)/len(working_df))*100, 1)
            st.session_state['stats'] = {
                "total": len(working_df), 
                "clean": len(df_clean), 
                "failed": len(df_failed), 
                "rate": pass_rate
            }
            st.session_state['rules'] = rules_applied

            # UI Feedback
            st.balloons()
            st.write("### 📊 3. Data Health Results")
            st.write(f"**Overall Data Health: {pass_rate}%**")
            st.progress(pass_rate / 100)
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Records", st.session_state['stats']['total'])
            m2.metric("Clean Data", st.session_state['stats']['clean'], delta=f"{pass_rate}% Pass")
            m3.metric("Quarantined", st.session_state['stats']['failed'], delta=f"-{100-pass_rate}% Failed", delta_color="inverse")

            if not df_failed.empty:
                st.warning(f"🚨 {st.session_state['stats']['failed']} records failed quality checks.")
                st.write("#### 📝 Failure Log (Preview)")
                st.dataframe(df_failed.head(10), use_container_width=True)
            
            st.divider()
            st.write("#### 📥 Data Export")
            d1, d2 = st.columns(2)
            
            clean_csv = df_clean.to_csv(index=False).encode('utf-8')
            d1.download_button("Download Clean CSV", clean_csv, "clean_data.csv", "text/csv")
            
            pdf_bytes = generate_pdf(st.session_state['stats'], st.session_state['rules'])
            d2.download_button("📑 Download PDF Report", pdf_bytes, "validation_report.pdf", "application/pdf")

        except Exception as e:
            st.error(f"Error during validation: {e}")
else:
    st.info("Please provide a data source to begin.")

# --- SECTION 8: FOOTER & DISCLAIMER ---
st.markdown("---")
st.markdown("<p style='text-align: center; color: #333; font-weight: 500;'>Built by mikecap22 | watchdog-validator v1.0</p>", unsafe_allow_html=True)

st.markdown("""
    <div style="background-color: rgba(0, 104, 201, 0.05); padding: 15px; border-radius: 10px; border: 1px solid #0068c9;">
        <p style="font-size: 1.1rem; color: #000; margin: 0;">
            <strong>Disclaimer:</strong> This tool is intended for data integrity screening; 
            validation results are based on user-defined rules. Always verify critical financial 
            data manually before final processing.
        </p>
    </div>
""", unsafe_allow_html=True)