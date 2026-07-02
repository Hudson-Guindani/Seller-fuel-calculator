# Seller Fuel Calculator 📊

<a name="english"></a>
## 🇺🇸 English Version

This Python-based tool optimizes the process of auditing, analyzing routes, and calculating fuel reimbursements for sales representatives (RCA).

### Features
- **ERP/Database Integration:** Queries via SQL/SQLAlchemy on an Oracle DB to fetch visit logs (`C2DESLOC`, `PCCLIENT`, `PCUSUARI`).
- **Intelligent Distance Calculation:** Consumes the Google Maps Directions API to calculate routes based on the geographic coordinates of the visited clients.
- **Formatted Export:** Generates Excel spreadsheets (`.xlsx`) with auto-fitted columns and centered text using `pandas` and `xlsxwriter`.

### Technologies Used
- **Python 3**
- **Pandas**
- **Google Maps Services Python**
- **SQLAlchemy & cx_Oracle**
- **XlsxWriter**

### 📋 Prerequisites & Setup
```bash
pip install googlemaps pandas sqlalchemy cx_oracle xlsxwriter
