# Seller Fuel Calculator 📊

[Click here for the English version](#english) | [Clique aqui para a versão em Português](#portugues)

---

<a name="english"></a>
## 🇺🇸 English Version

This Python-based tool optimizes the process of auditing, analyzing routes, and calculating fuel reimbursements for sales representatives (RCA).

The system connects directly to a corporate Oracle database to extract visit and order histories, utilizes the Google Maps API to calculate precise distances traveled, and exports a detailed, styled report to Excel.

### 🚀 Features

- **ERP/Database Integration:** Queries via SQL/SQLAlchemy on an Oracle DB to fetch visit logs (`C2DESLOC`, `PCCLIENT`, `PCUSUARI`).
- **Intelligent Distance Calculation:** Consumes the Google Maps Directions API to calculate routes based on the geographic coordinates (Latitude/Longitude) of the visited clients.
- **Commercial Routing Logic:** Sorts visits by the shortest distance starting from the salesperson's residence and calculates the return trip at the end of the day.
- **Geographic Exception Handling:** Includes specific business rules for hard-to-reach locations (e.g., geolocation overrides for Ilha Grande/Angra dos Reis).
- **Financial Calculation:** Allows inputting the reimbursement rate per kilometer to generate the exact payout amount.
- **Formatted Export:** Generates Excel spreadsheets (`.xlsx`) with auto-fitted columns and centered text using `pandas` and `xlsxwriter`.

### 🛠️ Technologies Used

- **Python 3**
- **Pandas** (Data manipulation and report structuring)
- **Google Maps Services Python** (Directions API consumption)
- **SQLAlchemy & cx_Oracle** (Oracle DB connection and query execution)
- **Tkinter** (Native file dialog interface to save the Excel file)
- **XlsxWriter** (Spreadsheet styling and formatting)

### 📋 Prerequisites & Setup

Before running the script, install the project dependencies:

```bash
pip install googlemaps pandas sqlalchemy cx_oracle xlsxwriter
