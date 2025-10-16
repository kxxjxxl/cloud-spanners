# cloud-spanners
lightweight Python utility for importing CSV data into a **Google Cloud Spanner** table.



# CSV to Google Cloud Spanner Importer

a lightweight Python utility for importing CSV data into a **Google Cloud Spanner** table.  
It supports both **single-batch** and **chunked** inserts, with optional type definitions via a JSON schema.

---

## Features
- Import large CSV files directly into Cloud Spanner.
- Support for **chunked uploads** using pandas' `chunksize`.
- Optional **data type mapping** using a JSON format file.
- Clean, modular code structure for maintainability.

---

## 📦 Requirements

- Python 3.8+
- Google Cloud SDK installed and authenticated (`gcloud auth application-default login`)
- A Cloud Spanner instance and database already created.

### Python Dependencies
Install required packages:
```bash
pip install pandas google-cloud-spanner
