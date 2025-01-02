# AFL Analytics Project

## Overview
The AFL Analytics Project is a Python-based application that integrates with the AFL API to fetch, process, and store Australian Football League (AFL) data in Google BigQuery. It supports multiple endpoints for data retrieval and update operations using Flask. The app is designed to help users maintain up-to-date datasets for teams, players, matches, and statistics.

---

## Features
1. **API Integration**: Fetches data from the AFL API for teams, players, matches, and statistics.
2. **Google BigQuery Integration**: Automates data storage and updates to BigQuery tables.
3. **Flask API**: Provides endpoints to update individual or all datasets via HTTP requests.
4. **Threaded Operations**: Allows simultaneous updates for multiple datasets for efficient processing.
5. **Error Handling**: Includes retry mechanisms for handling API request failures.

---

## Requirements
### Libraries
- `requests`
- `json`
- `pandas`
- `pandas_gbq`
- `google-cloud-bigquery`
- `google-cloud-secretmanager`
- `flask`

Install required libraries using:
```bash
pip install requests pandas pandas-gbq google-cloud-bigquery google-cloud-secretmanager flask
