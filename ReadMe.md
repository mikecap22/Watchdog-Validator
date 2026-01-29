# Watchdog Validator

A professional data governance and integrity suite built with Great Expectations. This project implements an automated data quality gate that separates clean data from problematic records using a robust quarantine pattern.

## 📋 Overview

Watchdog Validator is designed to act as a "Gatekeeper" for data pipelines. It validates incoming datasets against strict business rules; automatically quarantining records that fail quality checks to prevent downstream system corruption.

## ✨ Features

* **Visual Analytics**: Real-time bar charts and metrics showing data health at a glance.
* **Single-Stream UI**: A modern; tab-less Streamlit interface designed for efficiency.
* **Source Agnostic**: Direct support for CSV; Excel; and SQL Databases (PostgreSQL; MySQL; SQLite).
* **Dynamic Column Mapping**: Map any dataset columns to validation logic without touching code.
* **Professional PDF Reporting**: Generate downloadable executive summaries of validation results.
* **Advanced Rule Engine**: 
    * Range checks for financial integrity.
    * Null value detection for operational completeness.
    * Uniqueness constraints for identity management.

## 🛠️ Technologies Used

* **Python 3.11+**
* **Streamlit**: Interactive web interface.
* **Great Expectations**: Enterprise-grade data validation framework.
* **SQLAlchemy**: Database connectivity engine.
* **FPDF**: Automated PDF report generation.

## 📦 Installation

### Prerequisites
* Python 3.11 or higher
* pip package manager

### Setup

1. Clone this repository:
```bash
git clone [https://github.com/mikecap22/watchdog-validator.git](https://github.com/mikecap22/watchdog-validator.git)
cd watchdog-validator

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Run the Streamlit app:
```bash
streamlit run app.py
```

## 🚀 Usage

1. Source Selection: Choose between uploading a file or connecting to a live SQL database.

2. Column Mapping: Select which columns in your data represent IDs; Prices; or Users.

3. Configure Rules: Toggle specific quality checks (e.g.; Reject Negatives; Reject Nulls).

4. Execute: Run the engine to see visual results and balloons on success.

5. Export: Download the "Clean" dataset or the "Executive PDF Report" for stakeholders.


## 🔍 Validation Rules

Rule	    Validation	                    Business Impact		
Range Check	Price/Amount must be >= 0	    Prevents financial loss from negative billing.		
Null Check	Critical IDs cannot be empty	Ensures data joins and CRM tracking remain intact.		
Uniqueness	Transaction IDs must be unique	Prevents duplicate processing and double-counting.		

## 📈 Output

The script provides console output showing:
- Whether data passed the quality gate
- Number of clean rows
- Number of flagged rows

Example output:
```
Project 1: Issues detected; validation failed.
Quarantine Complete:
Clean Rows: 47
Flagged Rows: 3
```

## 🏗️ Project Structure

```
.
├── app.py                   # Streamlit front-end with visual UI logic
├── watchdog_validator.py    # Backend class-based validation engine
├── watchdog_header.png      # Centered branding header image
├── requirements.txt         # Project dependencies
└── README.md                # Project documentation

⚖️ Disclaimer
This tool is intended for data integrity screening. Validation results are based on user-defined rules. Always verify critical financial data manually before final processing.


## 📝 License

This project is open source and available under the [MIT License](LICENSE).

## 👤 Author

Michael Cappello
- GitHub: [@mikecap22](https://github.com/mikecap22)

## 🙏 Acknowledgments

- Built with [Great Expectations](https://greatexpectations.io/)
- Inspired by modern data quality best practices

## 📧 Contact

For questions or feedback, please open an issue on GitHub.

---

⭐ If you find this project useful, please consider giving it a star!