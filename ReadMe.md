# Watchdog Validator

A robust data validation pipeline for e-commerce transactions using Great Expectations. This project implements automated data quality checks and separates clean data from problematic records using a quarantine pattern.

## 📋 Overview

This pipeline validates e-commerce transaction data against defined business rules and automatically separates clean records from those that fail validation. Failed records are quarantined for review and remediation.

## ✨ Features

- **Automated Data Validation**: Validates incoming transaction data against business rules
- **Quarantine Pattern**: Automatically separates clean data from failed records
- **Business Rule Enforcement**:
  - Price must be positive (Finance integrity)
  - Quantity cannot be null (Operational integrity)
  - Customer ID must exist (Marketing/CRM tracking)
- **Detailed Reporting**: Provides summary of clean vs. flagged records

## 🛠️ Technologies Used

- **Python 3.x**
- **pandas**: Data manipulation and analysis
- **Great Expectations**: Data validation framework

## 📦 Installation

### Prerequisites
- Python 3.7 or higher
- pip package manager

### Setup

1. Clone this repository:
```bash
git clone https://github.com/mikecap22/watchdog-validator.git
cd watchdog-validator
```

2. Install required packages:
```bash
pip install pandas great-expectations
```

## 🚀 Usage

1. Place your `ecommerce_transactions.csv` file in the project directory

2. Run the validation pipeline:
```bash
python your_script_name.py
```

3. Check the output:
   - `clean_transactions.csv` - Records that passed all validation rules
   - `failed_transactions.csv` - Records that failed one or more rules

## 📊 Input Data Format

Your CSV file should contain the following columns:
- `Transaction ID`
- `Customer ID`
- `Price`
- `Quantity`
- `Product`

Example:
```csv
Transaction ID,Customer ID,Price,Quantity,Product
1,C001,29.99,2,Widget
2,C002,15.50,1,Gadget
3,C003,99.00,3,Tool
```

## 🔍 Validation Rules

| Rule | Column | Validation | Business Impact |
|------|--------|------------|-----------------|
| 1 | Price | Must be >= 0 | Finance integrity - prevents negative prices |
| 2 | Quantity | Cannot be null | Operational integrity - ensures inventory tracking |
| 3 | Customer ID | Cannot be null | Marketing/CRM - maintains customer relationships |

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
├── your_script_name.py          # Main validation script
├── ecommerce_transactions.csv   # Input data (not tracked in git)
├── clean_transactions.csv       # Output: validated data (not tracked in git)
├── failed_transactions.csv      # Output: quarantined data (not tracked in git)
├── .gitignore                   # Git ignore file
└── README.md                    # This file
```

## 🔧 Customization

To add additional validation rules, add more expectations in the script:

```python
# Example: Add maximum price validation
validator.expect_column_values_to_be_between("Price", min_value=0, max_value=10000)

# Example: Validate product names
validator.expect_column_values_to_be_in_set("Product", ["Widget", "Gadget", "Tool"])
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is open source and available under the [MIT License](LICENSE).

## 👤 Author

**mikecap22**
- GitHub: [@mikecap22](https://github.com/mikecap22)

## 🙏 Acknowledgments

- Built with [Great Expectations](https://greatexpectations.io/)
- Inspired by modern data quality best practices

## 📧 Contact

For questions or feedback, please open an issue on GitHub.

---

⭐ If you find this project useful, please consider giving it a star!