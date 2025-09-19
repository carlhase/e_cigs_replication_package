
# Cleaning and Preparation Scripts

These scripts take raw scanner and auxiliary datasets, process them into usable analysis datasets, and construct variables required for the empirical analysis.

### Execution Order
Run these scripts in the order shown below

1. Run `scripts/cleaning_and_prep/01_read_da_chunk_feather.py` — Reads in chunks of zip archives, collects daily aggregates at store level across datasets in a chunk, and saves store-chunk-level datasets

2. Run `scripts/cleaning_and_prep/02_da_chunks_to_store_level_monthly_ag_feather.py` — For each store, reads and concatinates data across all chunks to get store-level dataframe, then calculates monthly aggregates and saves as monthly store-level panel

3. Run `scripts/cleaning_and_prep/vape_qty_index_1b_fisc.py` — Constructs vape quantity indexes used in main specitifation

4. Run `scripts/cleaning_and_prep/vape_qty_index_1c_fisc.py` — Constructs single-stage vape quantity indexes

5. Run `scripts/cleaning_and_prep/vape_qty_index_1d_fisc.py` — Constructs two-stage indexes but with brand as first stage aggregation instead of product type

6. Run `scripts/cleaning_and_prep/vape_qty_index_1b_fisc_no_authorized.py` — Constructs quantity indexes with authorized brands removed

7. Run `scripts/cleaning_and_prep/vape_price_index_1b_fisc.py` — Constructs price indexes used in Section 8

8. Run `scripts/cleaning_and_prep/vape_qty_count_index_1a.py` — Constructs indexes for monthly vape units sold

9. Run `scripts/cleaning_and_prep/vape_trans_count_index_1a.py` — Constructs indexes for monthly vape transaction counts

10. Run `scripts/cleaning_and_prep/vape_rev_index_1a.py` — Constructs indexes for monthly vape revenue

11. Run `scripts/cleaning_and_prep/vape_qty_per_trans_index_1a.py` — Constructs indexes for monthly vape units sold per transaction

12. Run `scripts/cleaning_and_prep/laus_unemp_rates.py` — Constructs zip code level unemployment rate indexes using data from from LAUS

13. Run `scripts/cleaning_and_prep/qcew_avg_wage.py` — Constructs average wage indexes at zip code level using data from QCEW

14. Run `scripts/cleaning_and_prep/home_price_index.py` — Constructs home price indexes using data from FHFA

15. Run `scripts/cleaning_and_prep/zip_heterogeneity_matrices_2.py` — Extracts OLS event study estimates for each zip code at t+6

16. Run `scripts/cleaning_and_prep/combine_warning_letters.py` — Combines annual datasets of FDA warning letters

17. Run `scripts/cleaning_and_prep/matching_stores_with_fda_inspections_b.py` — Uses fuzzy matching to match FDA tobacco inspections with stores from PDI data

18. Run `scripts/cleaning_and_prep/matching_stores_with_letters_b.py` — Uses fuzzy matching to match FDA e-cigarette warning letters with stores from PDI data

19. Run `scripts/cleaning_and_prep/geographic_inspection_rates.py` — Generates state-level inspection and violation rate variables and scatter plots

20. Run `scripts/cleaning_and_prep/viol_follow_up_rates.py` — State-level follow up inspection rates

21. Run `scripts/cleaning_and_prep/district_court_case_rates.py` — State-level cmp case filing rates in district court

22. Run `scripts/cleaning_and_prep/r_insp_issue_2a_b.py` — Constructs estimation panel for main specification of rival stores regressions

23. Run `scripts/cleaning_and_prep/t_and_s_insp_issue_2a_b.py`— Constructs estimation panel for main specification of violative store regressions




