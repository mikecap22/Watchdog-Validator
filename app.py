#app.py

#This is the Streamlit Front End

import streamlit as st
import pandas as pd
from watchdog_validator import WatchdogValidator
from io import BytesIO

# Page configuration

#Header
# Using the new native st.logo feature (available in Streamlit 1.35+)
st.logo("watchdog_header.png", size="large")

st.set_page_config(
    page_title="Watchdog Validator",
    page_icon="🐕‍🦺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        padding: 1rem;
        background-color: #d4edda;
        border-left: 5px solid #28a745;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .error-box {
        padding: 1rem;
        background-color: #f8d7da;
        border-left: 5px solid #dc3545;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .info-box {
        padding: 1rem;
        background-color: #d1ecf1;
        border-left: 5px solid #17a2b8;
        border-radius: 5px;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# Header
st.markdown('<p class="main-header">🐕‍🦺 Watchdog Validator</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Automated Data Quality Pipeline with Great Expectations</p>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.header("⚙️ Configuration")
    
    st.markdown("### About")
    st.info("""
    **Watchdog Validator** ensures your data quality by validating it against customizable business rules.
    
    Upload your CSV, configure validation rules, and get clean data separated from problematic records.
    """)
    
    st.markdown("### How It Works")
    st.markdown("""
    1. 📤 Upload your CSV file
    2. ⚙️ Configure validation rules
    3. ✅ Run validation
    4. 📊 Review results
    5. 💾 Download clean/failed data
    """)
    
    st.markdown("---")
    st.markdown("**Created by:** mikecap22")
    st.markdown("[GitHub Repository](https://github.com/mikecap22/watchdog-validator)")

# Main content
tab1, tab2, tab3 = st.tabs(["📤 Upload & Validate", "📊 Results", "ℹ️ About"])

with tab1:
    st.header("Upload Your Dataset")
    
    # Data source selection
    data_source = st.radio(
        "Select Data Source",
        ["Upload File (CSV/Excel)", "SQL Database (Advanced)"],
        horizontal=True
    )
    
    if data_source == "Upload File (CSV/Excel)":
        # File upload
        uploaded_file = st.file_uploader(
            "Choose a CSV or Excel file",
            type=['csv', 'xlsx', 'xls'],
            help="Upload your transaction data in CSV or Excel format"
        )
        
        if uploaded_file is not None:
            try:
                # Determine file type and load accordingly
                file_extension = uploaded_file.name.split('.')[-1].lower()
                
                if file_extension == 'csv':
                    df = pd.read_csv(uploaded_file)
                    st.success(f"✅ CSV file uploaded successfully! **{len(df)}** rows loaded.")
                elif file_extension in ['xlsx', 'xls']:
                    # For Excel files, let user select sheet
                    excel_file = pd.ExcelFile(uploaded_file)
                    sheet_names = excel_file.sheet_names
                    
                    if len(sheet_names) > 1:
                        selected_sheet = st.selectbox("Select Excel Sheet", sheet_names)
                    else:
                        selected_sheet = sheet_names[0]
                    
                    df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
                    st.success(f"✅ Excel file uploaded successfully! Sheet: **{selected_sheet}**, **{len(df)}** rows loaded.")
                else:
                    st.error("Unsupported file format.")
                    df = None
                
                if df is not None:
                    # Display preview
                    st.subheader("📋 Data Preview")
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    # Show column info
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Rows", len(df))
                    with col2:
                        st.metric("Total Columns", len(df.columns))
                    with col3:
                        st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024:.2f} KB")
                    
                    st.markdown("---")
                    
                    # Store df in session state for validation
                    st.session_state['df'] = df
                    st.session_state['data_loaded'] = True
            
            except Exception as e:
                st.error(f"❌ Error reading file: {str(e)}")
                st.exception(e)
        else:
            st.info("👆 Please upload a CSV or Excel file to begin validation.")
            
            # Sample data template
            st.markdown("---")
            st.subheader("📝 Don't have data? Download a sample template:")
            
            sample_data = pd.DataFrame({
                'Transaction ID': [1, 2, 3, 4, 5],
                'Customer ID': ['C001', 'C002', 'C003', None, 'C005'],
                'Price': [29.99, 15.50, -10.00, 99.00, 45.75],
                'Quantity': [2, 1, 3, None, 1],
                'Product': ['Widget', 'Gadget', 'Tool', 'Device', 'Component']
            })
            
            col1, col2 = st.columns(2)
            with col1:
                csv = sample_data.to_csv(index=False)
                st.download_button(
                    label="📥 Download Sample CSV",
                    data=csv,
                    file_name="sample_transactions.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            with col2:
                # Create Excel file in memory
                excel_buffer = BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    sample_data.to_excel(writer, index=False, sheet_name='Transactions')
                excel_buffer.seek(0)
                
                st.download_button(
                    label="📥 Download Sample Excel",
                    data=excel_buffer,
                    file_name="sample_transactions.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
    
    else:  # SQL Database option
        st.subheader("🗄️ Connect to SQL Database")
        
        st.info("Connect to your SQL database and run a query to validate data directly from your database.")
        
        # Database type selection
        db_type = st.selectbox(
            "Database Type",
            ["PostgreSQL", "MySQL", "SQLite", "SQL Server", "Custom Connection String"]
        )
        
        # Connection parameters based on database type
        if db_type == "SQLite":
            db_file = st.text_input("Database File Path", value="database.db")
            connection_string = f"sqlite:///{db_file}"
        elif db_type == "Custom Connection String":
            connection_string = st.text_input(
                "Connection String",
                help="Example: postgresql://user:password@localhost:5432/dbname"
            )
        else:
            col1, col2 = st.columns(2)
            with col1:
                host = st.text_input("Host", value="localhost")
                port_defaults = {"PostgreSQL": 5432, "MySQL": 3306, "SQL Server": 1433}
                port = st.number_input("Port", value=port_defaults.get(db_type, 5432))
            with col2:
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
            
            database = st.text_input("Database Name")
            
            # Build connection string
            if db_type == "PostgreSQL":
                connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            elif db_type == "MySQL":
                connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
            elif db_type == "SQL Server":
                connection_string = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        
        # Query input
        query_type = st.radio("Query Type", ["SQL Query", "Table Name"], horizontal=True)
        
        if query_type == "SQL Query":
            sql_query = st.text_area(
                "SQL Query",
                value="SELECT * FROM transactions LIMIT 1000",
                help="Enter your SQL query to fetch data"
            )
        else:
            table_name = st.text_input("Table Name", value="transactions")
            schema_name = st.text_input("Schema (optional)", value="")
            sql_query = None
        
        # Connect button
        if st.button("🔌 Connect and Load Data", type="primary"):
            if connection_string:
                with st.spinner("Connecting to database..."):
                    try:
                        if query_type == "SQL Query" and sql_query:
                            validator_temp = WatchdogValidator.from_sql(sql_query, connection_string)
                        else:
                            validator_temp = WatchdogValidator.from_sql_table(
                                table_name, 
                                connection_string, 
                                schema=schema_name if schema_name else None
                            )
                        
                        df = validator_temp.df
                        st.success(f"✅ Connected successfully! **{len(df)}** rows loaded from database.")
                        
                        # Display preview
                        st.subheader("📋 Data Preview")
                        st.dataframe(df.head(10), use_container_width=True)
                        
                        # Show column info
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Rows", len(df))
                        with col2:
                            st.metric("Total Columns", len(df.columns))
                        with col3:
                            st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024:.2f} KB")
                        
                        st.markdown("---")
                        
                        # Store df in session state for validation
                        st.session_state['df'] = df
                        st.session_state['data_loaded'] = True
                        
                    except Exception as e:
                        st.error(f"❌ Database connection error: {str(e)}")
                        st.exception(e)
            else:
                st.warning("Please provide connection details.")
    
    # Validation configuration (only show if data is loaded)
    if 'data_loaded' in st.session_state and st.session_state['data_loaded']:
        df = st.session_state['df']
        
        # Validation Rules Configuration
        st.subheader("⚙️ Configure Validation Rules")
        
        st.markdown("**Available Columns:**")
        st.write(", ".join(df.columns.tolist()))
        
        # Create expandable sections for each rule type
        with st.expander("🔢 Numeric Range Validations", expanded=True):
            enable_price = st.checkbox("Validate Price (must be >= 0)", value=True)
            if enable_price:
                price_col = st.selectbox("Select Price Column", df.columns, 
                                        index=df.columns.tolist().index('Price') if 'Price' in df.columns else 0)
                price_min = st.number_input("Minimum Price", value=0.0, step=0.01)
                price_max = st.number_input("Maximum Price (optional, 0 = no limit)", value=0.0, step=0.01)
        
        with st.expander("❌ Null Value Checks", expanded=True):
            enable_quantity = st.checkbox("Validate Quantity (cannot be null)", value=True)
            if enable_quantity:
                quantity_col = st.selectbox("Select Quantity Column", df.columns, 
                                           index=df.columns.tolist().index('Quantity') if 'Quantity' in df.columns else 0)
            
            enable_customer = st.checkbox("Validate Customer ID (cannot be null)", value=True)
            if enable_customer:
                customer_col = st.selectbox("Select Customer ID Column", df.columns, 
                                           index=df.columns.tolist().index('Customer ID') if 'Customer ID' in df.columns else 0)
        
        # Additional custom validation
        with st.expander("➕ Additional Validations (Optional)", expanded=False):
            enable_custom = st.checkbox("Add custom column validation")
            if enable_custom:
                custom_col = st.selectbox("Select Column", df.columns, key="custom_col")
                custom_rule = st.selectbox("Rule Type", ["Not Null", "Unique Values", "In Set"])
                
                if custom_rule == "In Set":
                    custom_values = st.text_input("Allowed values (comma-separated)", "")
        
        st.markdown("---")
        
        # Run Validation Button
        if st.button("🚀 Run Validation", type="primary", use_container_width=True):
            with st.spinner("Running validation checks..."):
                try:
                    # Initialize the Watchdog Validator (backend)
                    validator = WatchdogValidator(df)
                    
                    # Track applied rules for display
                    rules_applied = []
                    
                    # Apply validation rules based on user configuration
                    if enable_price:
                        if price_max > 0:
                            validator.add_price_validation(price_col, min_value=price_min, max_value=price_max)
                            rules_applied.append(f"{price_col} between {price_min} and {price_max}")
                        else:
                            validator.add_price_validation(price_col, min_value=price_min)
                            rules_applied.append(f"{price_col} >= {price_min}")
                    
                    if enable_quantity:
                        validator.add_not_null_validation(quantity_col)
                        rules_applied.append(f"{quantity_col} not null")
                    
                    if enable_customer:
                        validator.add_not_null_validation(customer_col)
                        rules_applied.append(f"{customer_col} not null")
                    
                    if enable_custom and custom_rule == "Not Null":
                        validator.add_not_null_validation(custom_col)
                        rules_applied.append(f"{custom_col} not null")
                    
                    if enable_custom and custom_rule == "Unique Values":
                        validator.add_unique_validation(custom_col)
                        rules_applied.append(f"{custom_col} unique")
                    
                    if enable_custom and custom_rule == "In Set" and custom_values:
                        allowed_values = [v.strip() for v in custom_values.split(',')]
                        validator.add_set_validation(custom_col, allowed_values)
                        rules_applied.append(f"{custom_col} in {allowed_values}")
                    
                    # Run validation (backend handles all the logic)
                    summary = validator.run_validation()
                    
                    # Get clean and failed data
                    df_clean = validator.get_clean_data()
                    df_failed = validator.get_failed_data()
                    
                    # Store results in session state
                    st.session_state['validation_complete'] = True
                    st.session_state['summary'] = summary
                    st.session_state['df_clean'] = df_clean
                    st.session_state['df_failed'] = df_failed
                    st.session_state['rules_applied'] = rules_applied
                    
                    st.success("✅ Validation complete! Check the **Results** tab.")
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"❌ Error during validation: {str(e)}")
                    st.exception(e)


with tab2:
    st.header("Validation Results")
    
    if 'validation_complete' in st.session_state and st.session_state['validation_complete']:
        summary = st.session_state['summary']
        df_clean = st.session_state['df_clean']
        df_failed = st.session_state['df_failed']
        rules_applied = st.session_state['rules_applied']
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Rows", summary['total_rows'])
        with col2:
            st.metric("✅ Clean Rows", summary['clean_rows'], 
                     delta=f"{summary['pass_rate']:.1f}%")
        with col3:
            st.metric("❌ Failed Rows", summary['failed_rows'], 
                     delta=f"-{100 - summary['pass_rate']:.1f}%")
        with col4:
            validation_status = "PASSED" if summary['success'] else "FAILED"
            st.metric("Validation Status", validation_status)
        
        # Validation summary
        if summary['success']:
            st.markdown('<div class="success-box"><strong>🎉 All data passed validation!</strong></div>', 
                       unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="error-box"><strong>⚠️ {summary["failed_rows"]} rows failed validation</strong></div>', 
                       unsafe_allow_html=True)
        
        # Rules applied
        st.subheader("📋 Rules Applied")
        for i, rule in enumerate(rules_applied, 1):
            st.markdown(f"**{i}.** {rule}")
        
        st.markdown("---")
        
        # Clean data section
        st.subheader("✅ Clean Data")
        if not df_clean.empty:
            st.dataframe(df_clean, use_container_width=True)
            
            # Download button for clean data
            clean_csv = df_clean.to_csv(index=False)
            st.download_button(
                label="📥 Download Clean Data",
                data=clean_csv,
                file_name="clean_transactions.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.warning("No clean data found.")
        
        st.markdown("---")
        
        # Failed data section
        st.subheader("❌ Failed Data (Quarantined)")
        if not df_failed.empty:
            st.dataframe(df_failed, use_container_width=True)
            
            # Download button for failed data
            failed_csv = df_failed.to_csv(index=False)
            st.download_button(
                label="📥 Download Failed Data",
                data=failed_csv,
                file_name="failed_transactions.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.success("No failed data - all records passed validation!")
        
        # Detailed validation results
        with st.expander("🔍 Detailed Validation Results"):
            st.json(summary['results'].to_json_dict())
    
    else:
        st.info("👈 Please upload data and run validation in the **Upload & Validate** tab first.")

with tab3:
    st.header("About Watchdog Validator")
    
    st.markdown("""
    ### 🐕‍🦺 What is Watchdog Validator?
    
    Watchdog Validator is an automated data quality pipeline built with **Great Expectations** and **Streamlit**. 
    It helps data professionals ensure their data meets quality standards before it enters production systems.
    
    ### 🏗️ Architecture
    
    **Backend (`watchdog_validator.py`):**
    - Core validation engine
    - Reusable Python class
    - Great Expectations integration
    - Quarantine pattern implementation
    
    **Frontend (`app.py`):**
    - Streamlit web interface
    - Interactive rule configuration
    - Real-time results visualization
    - Data export capabilities
    
    ### 🎯 Key Features
    
    - **Flexible Validation Rules**: Configure numeric ranges, null checks, uniqueness, and custom validations
    - **Quarantine Pattern**: Automatically separates clean data from problematic records
    - **Interactive Interface**: User-friendly web interface for non-technical users
    - **Detailed Reporting**: Get comprehensive insights into data quality issues
    - **Export Capabilities**: Download clean and failed datasets separately
    - **Reusable Backend**: Import the validator module in your own Python scripts
    
    ### 🛠️ Technology Stack
    
    - **Streamlit**: Interactive web interface
    - **Great Expectations**: Data validation framework
    - **Pandas**: Data manipulation and analysis
    - **Python**: Core programming language
    
    ### 📊 Use Cases
    
    - **E-commerce**: Validate transaction data before processing
    - **Finance**: Ensure pricing and monetary values are correct
    - **Marketing**: Verify customer data integrity
    - **Operations**: Check inventory and quantity data
    - **Data Engineering**: Quality gates in ETL pipelines
    
    ### 🚀 Getting Started
    
    1. Upload your CSV file
    2. Select which columns to validate
    3. Configure validation rules
    4. Run validation
    5. Download clean and failed data
    
    ### 💡 Using the Backend Directly
    
    You can also import and use the validator in your own Python scripts:
    
    ```python
    from watchdog_validator import WatchdogValidator
    import pandas as pd
    
    # Load your data
    df = pd.read_csv('your_data.csv')
    
    # Initialize validator
    validator = WatchdogValidator(df)
    
    # Add validation rules
    validator.add_price_validation("Price", min_value=0)
    validator.add_not_null_validation("Customer ID")
    
    # Run validation
    summary = validator.run_validation()
    
    # Get results
    clean_data = validator.get_clean_data()
    failed_data = validator.get_failed_data()
    
    # Save to files
    validator.save_results()
    ```
    
    ### 🤝 Contributing
    
    This is an open-source project. Contributions are welcome!
    
    **GitHub**: [mikecap22/watchdog-validator](https://github.com/mikecap22/watchdog-validator)
    
    ### 📧 Contact
    
    For questions, suggestions, or feedback:
    - Open an issue on GitHub
    - Connect with me: **mikecap22**
    
    ---
    
    ### 📝 License
    
    This project is open source and available under the MIT License.
    
    ---
    
    **Built with ❤️ by mikecap22**
    """)

# Footer
st.markdown("---")
st.markdown(
    "<p style='text-align: center; color: #666;'>Watchdog Validator v1.0 | "
    "Built with Streamlit and Great Expectations | "
    "<a href='https://github.com/mikecap22/watchdog-validator'>GitHub</a></p>",
    unsafe_allow_html=True
)