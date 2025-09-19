# -*- coding: utf-8 -*-
"""
Created on Fri Sep 19 16:29:58 2025

@author: cahase
"""

# Analysis Scripts

These scripts reproduce the paper’s descriptive statistics, main regression results, heterogeneity analyses, robustness checks, and figures.

### Execution Order
Run these scripts after the cleaning and preparation scripts are complete:

1. Run `scripts/analysis/product_market_descriptives_e_cigs_table_a.py` — Generates table 1 panel a

2. Run `scripts/analysis/product_market_descriptives_e_cigs_table_b.py` — Generates table 1 panel b

3. Run `scripts/analysis/pre_treatment_summary_stats.py` — Generates table with pre-treatment balance check

4. Run `scripts/analysis/main_figure.do` — Runs aggregate regressions and generates figure with aggregate treatment effects at violative and rival stores

5. Run `scripts/analysis/r_fiscal_year_iterations_thru112024.do` — Regressions and figures for state-level heterogeneity analyses for rival stores

6. Run `scripts/analysis/r_fiscal_year_iterations_thru112024_opposite_25_pctile.do` — Regressions and figures for state-level heterogeneity analyses for rival stores, 25th vs 75th percentiles

7. Run `scripts/analysis/alt_local_markets_heterogeneity.do` — Regressions and figure for rival heterogeneity analysis using alternate definition of local markets

8. Run `scripts/analysis/r_heterogeneity_robust_thru112024.do` — Robustness checks for rival store heterogeneity. Generates robustness check figures in appendix.

9. Run `scripts/analysis/t_fiscal_year_iterations_thru112024.do` — Regressions and figures for state-level heterogeneity analyses for violative stores

10. Run `scripts/analysis/r_price_effects.do` — Regressions and figures for price effects at rival stores
















