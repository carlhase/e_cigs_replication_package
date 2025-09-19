
# Data Folder

This folder is a placeholder. The replication package does **not** include raw data files.

## Why?
- Many datasets used in this project are **restricted** or subject to licensing agreements and therefore cannot be redistributed here.
- Instead, this repository provides all scripts (`.py` and `.do` files) required to clean, prepare, and analyze the data once access is obtained.

## How to Access the Data
This analysis uses different sources of data.
- PDI Technologies scanner data: Researchers can request access to the scanner data from Dewey at https://www.deweydata.io/
- Local Area Unemployment Statistics (LAUS): county-level unemployment data from US Census Bureau used to construct a control variable. The data are freely available at https://www.bls.gov/data/
- FHFA House Price Indexes: quarterly house price indexes at the three-digit zip code level in the United States. Used as a control variable. The data are freely available at https://www.fhfa.gov/hpi
- Quarterly Census of Employment and Wages (QCEW): County-level quarterly average wage data. The data are freely available at https://www.bls.gov/cew/
- Tobacco Compliance Check Outcomes: ddata from the FDA on the universe of tobacco inspections at brick and mortar retailers. The data are freely available at https://timp-ccid.fda.gov/

## Workflow
Once the appropriate raw data files are placed into this `data/raw_data` folder, the scripts in `scripts/` can be run to:
1. Clean and prepare the raw data.
2. Construct analysis datasets.
3. Replicate all tables and figures in the paper.

---
*Note: If you are a prospective employer browsing this repo, the exclusion of raw data is deliberate. The focus here is on the **code** — which demonstrates skills in Python, Stata, and reproducible research workflows.*
