global figs	"G:\My Drive\GSEFM\Research\e_cigarettes\output\"
global regs	"G:\My Drive\GSEFM\Research\e_cigarettes\output\regressions"

global dl_12b (tau0 + tau1 + tau2 + tau3 + tau4 + tau5 + tau6 + tau7 + tau8 + tau9 + tau10 + tau11 + tau12)

global dl_11b (tau0 + tau1 + tau2 + tau3 + tau4 + tau5 + tau6 + tau7 + tau8 + tau9 + tau10 + tau11)

global dl_10b (tau0 + tau1 + tau2 + tau3 + tau4 + tau5 + tau6 + tau7 + tau8 + tau9 + tau10)

global dl_9b (tau0 + tau1 + tau2 + tau3 + tau4 + tau5 + tau6 + tau7 + tau8 + tau9)

 ** t+8
global dl_8b (tau0 + tau1 + tau2 + tau3 + tau4 + tau5 + tau6 + tau7 + tau8)

 ** t+7
global dl_7b (tau0 + tau1 + tau2 + tau3 + tau4 + tau5 + tau6 + tau7)

 ** t+6
global dl_6b (tau0 + tau1 + tau2 + tau3 + tau4 + tau5 + tau6)
 
** t+5
global dl_5b (tau0 + tau1 + tau2 + tau3 + tau4 + tau5)
 
 ** t+4
global dl_4b (tau0 + tau1 + tau2 + tau3 + tau4)

** t+3
global dl_3b (tau0 + tau1 + tau2 + tau3)

** t+2
global dl_2b (tau0 + tau1 + tau2)

** t+1
global dl_1b (tau0 + tau1)

** t
global dl_0b tau0

** reference/empty coefficient
global event_baseb 0

** t-1
global dl_01b -(pre1)

** t-2
global dl_02b -(pre1 + pre2)

** t-3
global dl_03b -(pre1 + pre2 + pre3)

** t-4
global dl_04b -(pre1 + pre2 + pre3 + pre4)

** t-5
global dl_05b -(pre1 + pre2 + pre3 + pre4 + pre5)

*************************************************************
// read in data + generate required variables
*******************************
use "G:\My Drive\GSEFM\Research\e_cigarettes\data\processed_data\LS_Otter\estimation_panels\r_insp_issue_2a_b.dta", clear

encode state, gen(statenr)
label list statenr // view numbers assigned to counties

* Sort the data by retailer license and date
sort store_id date

xtset store_id date

drop if sister == 1

gen treatment_date = date if r_insp == 1

* Replace treatment_date with the first treatment date for each retailer (in case of multiple treatments)
by store_id: egen min_treatment_date = min(treatment_date) 

* Generate the month-since-treatment variable, which calculates the difference between the current date and the treatment date
gen month_since_treatment = (date - min_treatment_date)


drop if state == "nan"
drop if state == "None"

egen p_viol_75= pctile(viol_per_100_insp), p(75)
generate viol_75 = viol_per_100_insp >= p_viol_75

egen p_insp_25 = pctile(insp_per_100k), p(25)
generate insp_25 = (insp_per_100k <= p_insp_25)

egen p_follow_25 = pctile(pct_viol_flwup_insp_12_mo), p(25)
generate follow_25 = (pct_viol_flwup_insp_12_mo <= p_follow_25)

egen p_cmp_25 = pctile(avg_cmp_collected_vs_issued), p(25)
generate cmp_25 = (avg_cmp_collected_vs_issued <= p_cmp_25)

egen p_case_file_25 = pctile(penalty_share_us_civil_cases_17), p(25)
generate case_file_25 = (penalty_share_us_civil_cases_17 <= p_case_file_25)

// drop months after 11/2024
keep if date <= ym(2024, 11)

*************************************************************
/*/ Fiscal year index weights (baseline)
*******************************

quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_t10_thru_11_2024", replace

// Inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if insp_25 == 1  | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_t10_thru_11_2024", replace

// follow up inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_t10_thru_11_2024", replace

// CMP collection ratio
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_t10_thru_11_2024", replace

// CMP case filing rate
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_t10_thru_11_2024", replace
*/

*************************************************************
// Winsorized
*******************************
preserve
sort date
by date: egen p_1 = pctile(l_vape_qty_index_1b), p(.5)
by date: egen p_99 = pctile(l_vape_qty_index_1b), p(99.5)
replace l_vape_qty_index_1b = p_1 if l_vape_qty_index_1b < p_1
replace l_vape_qty_index_1b = p_99 if l_vape_qty_index_1b > p_99


// Violations per 100 inspections
quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 

estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_wins05_t10_thru_11_2024", replace

// Inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if insp_25 == 1  | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_wins05_t10_thru_11_2024", replace

// follow up inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_wins05_t10_thru_11_2024", replace

// CMP collection ratio
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_wins05_t10_thru_11_2024", replace

// CMP case filing rate
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_wins05_t10_thru_11_2024", replace

restore

*************************************************************
// Controls
*******************************
/*use "G:\My Drive\GSEFM\Research\e_cigarettes\data\processed_data\LS_Otter\estimation_panels\r_insp_issue_2a_b.dta", clear

encode state, gen(statenr)
label list statenr // view numbers assigned to counties

* Sort the data by retailer license and date
sort store_id date

xtset store_id date

drop if sister == 1

gen treatment_date = date if r_insp == 1

* Replace treatment_date with the first treatment date for each retailer (in case of multiple treatments)
by store_id: egen min_treatment_date = min(treatment_date) 

* Generate the month-since-treatment variable, which calculates the difference between the current date and the treatment date
gen month_since_treatment = (date - min_treatment_date)


drop if state == "nan"
drop if state == "None"

egen p_viol_75= pctile(viol_per_100_insp), p(75)
generate viol_75 = viol_per_100_insp >= p_viol_75

egen p_insp_25 = pctile(insp_per_100k), p(25)
generate insp_25 = (insp_per_100k <= p_insp_25)

egen p_follow_25 = pctile(pct_viol_flwup_insp_12_mo), p(25)
generate follow_25 = (pct_viol_flwup_insp_12_mo <= p_follow_25)

egen p_cmp_25 = pctile(avg_cmp_collected_vs_issued), p(25)
generate cmp_25 = (avg_cmp_collected_vs_issued <= p_cmp_25)

egen p_case_file_25 = pctile(penalty_share_us_civil_cases_17), p(25)
generate case_file_25 = (penalty_share_us_civil_cases_17 <= p_case_file_25)
*/
global controls d_unemp_rate l_avg_wage_index l_home_price_index

// Violations per 100 inspections
quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample controls($controls)

estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_controls_t10_thru_11_2024", replace

// Inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if insp_25 == 1  | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample controls($controls)
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_controls_t10_thru_11_2024", replace

// follow up inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample controls($controls)
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_controls_t10_thru_11_2024", replace

// CMP collection ratio
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample controls($controls)
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_controls_t10_thru_11_2024", replace

// CMP case filing rate
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample controls($controls)
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_controls_t10_thru_11_2024", replace


*************************************************************
// Calendar year index weights
*******************************

quietly did_imputation l_vaping_products store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 

estimates save "$regs\r_insp_2a_b_bsj_vape_viol_per_insp_75_calweights_t10_thru_11_2024", replace

// Inspection intensity
did_imputation l_vaping_products store_id date min_treatment_date if insp_25 == 1  | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_insp_25_calweights_t10_thru_11_2024", replace

// follow up inspection intensity
did_imputation l_vaping_products store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_followup_25_calweights_t10_thru_11_2024", replace

// CMP collection ratio
did_imputation l_vaping_products store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_cmp_25_calweights_t10_thru_11_2024", replace

// CMP case filing rate
did_imputation l_vaping_products store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_case_25_calweights_t10_thru_11_2024", replace


*************************************************************
// Quantity indexes with authorized products dropped
*******************************

quietly did_imputation l_vape_qty_index_1b_no_auth store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 

estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_nomanuf_t10_thru_11_2024", replace

// Inspection intensity
did_imputation l_vape_qty_index_1b_no_auth store_id date min_treatment_date if insp_25 == 1  | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_nomanuf_t10_thru_11_2024", replace

// follow up inspection intensity
did_imputation l_vape_qty_index_1b_no_auth store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_nomanuf_t10_thru_11_2024", replace

// CMP collection ratio
did_imputation l_vape_qty_index_1b_no_auth store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_nomanuf_t10_thru_11_2024", replace

// CMP case filing rate
did_imputation l_vape_qty_index_1b_no_auth store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_nomanuf_t10_thru_11_2024", replace


*************************************************************
// Compositional balance across event window
*******************************
// NOTE: using the hbalance option requires running two regressions per specificaiton. The first regression throws an error and creates a cannot_impute dummy variable. The second regression excludes observations with cannot_imput == 1
// WILL NEED TO DO THIS BY HAND
// violations per 100 inspections
quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance

quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if (viol_75 == 1 | non_rival == 1) & cannot_impute != 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_hbalance_t10_thru_11_2024", replace

// Inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if insp_25 == 1  | non_rival == 1 , pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if (insp_25 == 1  | non_rival == 1) & cannot_impute != 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_hbalance_t10_thru_11_2024", replace

// follow up inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if (follow_25 == 1 | non_rival == 1) & cannot_impute != 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_hbalance_t10_thru_11_2024", replace

// CMP collection ratio
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if (cmp_25 == 1 | non_rival == 1) & cannot_impute != 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_hbalance_t10_thru_11_2024", replace

// CMP case filing rate
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance

*did_imputation l_vape_qty_index_1b store_id date min_treatment_date if (case_file_25 == 1 | non_rival == 1) & cannot_impute != 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) hbalance
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_hbalance_t10_thru_11_2024", replace


*************************************************************
// Quantity index with first stage aggregation at the brand level
*******************************

quietly did_imputation l_vape_qty_index_1d store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 

estimates save "$regs\r_insp_2a_b_bsj_vape_1d_fisc_viol_per_insp_75_t10_thru_11_2024", replace

// Inspection intensity
did_imputation l_vape_qty_index_1d store_id date min_treatment_date if insp_25 == 1  | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_1d_fisc_insp_25_t10_thru_11_2024", replace

// follow up inspection intensity
did_imputation l_vape_qty_index_1d store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_1d_fisc_followup_25_t10_thru_11_2024", replace

// CMP collection ratio
did_imputation l_vape_qty_index_1d store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_1d_fisc_cmp_25_t10_thru_11_2024", replace

// CMP case filing rate
did_imputation l_vape_qty_index_1d store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_1d_fisc_case_25_t10_thru_11_2024", replace

*************************************************************
// Quantity index with single stage aggregation
*******************************

quietly did_imputation l_qty_index_1c_fisc store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 

estimates save "$regs\r_insp_2a_b_bsj_vape_1c_fisc_viol_per_insp_75_t10_thru_11_2024", replace

// Inspection intensity
did_imputation l_qty_index_1c_fisc store_id date min_treatment_date if insp_25 == 1  | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_1c_fisc_insp_25_t10_thru_11_2024", replace

// follow up inspection intensity
did_imputation l_qty_index_1c_fisc store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_1c_fisc_followup_25_t10_thru_11_2024", replace

// CMP collection ratio
did_imputation l_qty_index_1c_fisc store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_1c_fisc_cmp_25_t10_thru_11_2024", replace

// CMP case filing rate
did_imputation l_qty_index_1c_fisc store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_1c_fisc_case_25_t10_thru_11_2024", replace

*******************************************************
// Cumulative sums
*******************************************************
// baseline
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat baseline_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list baseline_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat baseline_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list baseline_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat baseline_followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list baseline_followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat baseline_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list baseline_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat baseline_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list baseline_case_25

// winsorized
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_wins05_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat wins_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list wins_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_wins05_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat wins_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list wins_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_wins05_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat wins_followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list wins_followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_wins05_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat wins_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list wins_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_wins05_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat wins_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list wins_case_25

// controls
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_controls_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat controls_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list controls_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_controls_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat controls_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list controls_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_controls_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat controls_followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list controls_followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_controls_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat controls_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list controls_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_controls_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat controls_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list controls_case_25

// calendar year weights
estimates use "$regs\r_insp_2a_b_bsj_vape_viol_per_insp_75_calweights_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat calweights_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list calweights_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_insp_25_calweights_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat calweights_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list calweights_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_followup_25_calweights_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat calweights_followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list calweights_followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_cmp_25_calweights_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat calweights_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list calweights_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_case_25_calweights_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat calweights_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list calweights_case_25


// no authorized brands
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_nomanuf_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat nomanuf_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list nomanuf_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_nomanuf_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat nomanuf_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list nomanuf_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_nomanuf_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat nomanuf_followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list nomanuf_followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_nomanuf_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat nomanuf_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list nomanuf_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_nomanuf_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat nomanuf_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list nomanuf_case_25

// compositional balance
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_hbalance_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat hbalance_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list hbalance_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_hbalance_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat hbalance_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list hbalance_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_hbalance_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat hbalance_followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list hbalance_followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_hbalance_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat hbalance_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list hbalance_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_hbalance_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat hbalance_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list hbalance_case_25

// index with first stage brand aggregation
estimates use "$regs\r_insp_2a_b_bsj_vape_1d_fisc_viol_per_insp_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1d_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1d_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_1d_fisc_insp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1d_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1d_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_1d_fisc_followup_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1d_followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1d_followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_1d_fisc_cmp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1d_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1d_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_1d_fisc_case_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1d_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1d_case_25

// single stage index
estimates use "$regs\r_insp_2a_b_bsj_vape_1c_fisc_viol_per_insp_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1c_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1c_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_1c_fisc_insp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1c_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1c_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_1c_fisc_followup_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1c_followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1c_followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_1c_fisc_cmp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1c_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1c_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_1c_fisc_case_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat v1c_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list v1c_case_25


// qty count indexes
estimates use "$regs\r_insp_2a_b_bsj_vape_qty_count_viol_per_insp_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat qty_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list qty_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_qty_count_insp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat qty_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list qty_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_qty_count_followup_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat qty_followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list qty_followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_qty_count_cmp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat qty_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list qty_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_qty_count_case_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat qty_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list qty_case_25


**************************
// t+8
mat comp_t8 = ///
	baseline_viol_75[1..9, 14], controls_viol_75[1..9, 14], wins_viol_75[1..9, 14], calweights_viol_75[1..9, 14], nomanuf_viol_75[1..9, 14], hbalance_viol_75[1..9, 14], v1d_viol_75[1..9, 14], v1c_viol_75[1..9, 14], ///
	baseline_insp_25[1..9, 14], controls_insp_25[1..9, 14], wins_insp_25[1..9, 14], calweights_insp_25[1..9, 14], nomanuf_insp_25[1..9, 14], hbalance_insp_25[1..9, 14], v1d_insp_25[1..9, 14], v1c_insp_25[1..9, 14], ///
	baseline_followup_25[1..9, 14], controls_followup_25[1..9, 14], wins_followup_25[1..9, 14], calweights_followup_25[1..9, 14], nomanuf_followup_25[1..9, 14], hbalance_followup_25[1..9, 14], v1d_followup_25[1..9, 14], v1c_followup_25[1..9, 14], ///
	baseline_cmp_25[1..9, 14], controls_cmp_25[1..9, 14], wins_cmp_25[1..9, 14], calweights_cmp_25[1..9, 14], nomanuf_cmp_25[1..9, 14], hbalance_cmp_25[1..9, 14], v1d_cmp_25[1..9, 14], v1c_cmp_25[1..9, 14], ///
	baseline_case_25[1..9, 14], controls_case_25[1..9, 14], wins_case_25[1..9, 14], calweights_case_25[1..9, 14], nomanuf_case_25[1..9, 14], hbalance_case_25[1..9, 14], v1d_case_25[1..9, 14], v1c_case_25[1..9, 14]


	
matrix colnames comp_t8 = ///
	1 2 3 4 5 6 7 8 9 10 ///
	11 12 13 14 15 16 17 18 19 20 ///
	21 22 23 24 25 26 27 28 29 30 ///
	31 32 33 34 35 36 37 38 39 40 ///
	

// 1. Prepare a dataset with group labels and positions:
clear
input xpos ypos str20 group_label
4.5 0.35 "Violations"
12.5 0.35 "Inspections"
20.5 0.35 "Follow-up"
28.5 0.35 "CMP collect"
36.5 0.35 "CMP cases"
end

// 2. Save this temp dataset
tempfile grouplabels
save `grouplabels'

coefplot ///
    (matrix(comp_t8), keep(1 2 3 4 5 6 7 8) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t8), keep(9 10 11 12 13 14 15 16) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t8), keep(17 18 19 20 21 22 23 24) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t8), keep(25 26 27 28 29 30 31 32 ) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t8), keep(33 34 35 36 37 38 39 40) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    , vertical ci((5 6)) recast(bar) barwidth(0.3) fcolor(*.3) ///
    citop ///
    graphregion(color(white)) bgcolor(white) ///
    legend(off) ///
xlabel(1 `"Baseline"' 2 `"Controls"' 3 `"Wins"' 4 `"Alt. weights"' 5 `"No auth prods"' 6 `"Balance"' 7 `"Brand"' 8 `"Single stage"'  ///
		9 `"Baseline"' 10 `"Controls"' 11 `"Wins"' 12 `"Alt. weights"' 13 `"No auth prods"' 14 `"Balance"' 15 `"Brand"' 16 `"Single stage"' ///
		17 `"Baseline"' 18 `"Controls"' 19 `"Wins"' 20 `"Alt. weights"' 21 `"No auth prods"' 22 `"Balance"' 23 `"Brand"' 24`"Single stage"'  ///
		25 `"Baseline"' 26 `"Controls"' 27 `"Wins"' 28 `"Alt. weights"' 29 `"No auth prods"' 30 `"Balance"' 31 `"Brand"' 32`"Single stage"'  ///
		33 `"Baseline"' 34 `"Controls"' 35 `"Wins"' 36 `"Alt. weights"' 37 `"No auth prods"' 38 `"Balance"' 39 `"Brand"' 40`"Single stage"'  ///
    , angle(90) labsize(small) ) ///
	xtick(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40, tlength(*.5)) ///
    xline(8.5 16.5 24.5 32.5, lpattern(dash) lcolor(gs12)) ///
    addplot(scatter ypos xpos, mlabel(group_label) mlabpos(12) mlabcolor(black) mlabsize(small) msymbol(none))
graph export "${figs}\r_insp_2a_b_vape_fisc_further_robust_checks_thru_11_2024.png", width(2400) height(1800) replace


**************************
// pre-treatment sums
mat comp_t05 = ///
	baseline_viol_75[1..9, 1], controls_viol_75[1..9, 1], wins_viol_75[1..9, 1], calweights_viol_75[1..9, 1], nomanuf_viol_75[1..9, 1], hbalance_viol_75[1..9, 1], v1d_viol_75[1..9, 1], v1c_viol_75[1..9, 1], ///
	baseline_insp_25[1..9, 1], controls_insp_25[1..9, 1], wins_insp_25[1..9, 1], calweights_insp_25[1..9, 1], nomanuf_insp_25[1..9, 1], hbalance_insp_25[1..9, 1], v1d_insp_25[1..9, 1], v1c_insp_25[1..9, 1],  ///
	baseline_followup_25[1..9, 1], controls_followup_25[1..9, 1], wins_followup_25[1..9, 1], calweights_followup_25[1..9, 1], nomanuf_followup_25[1..9, 1], hbalance_followup_25[1..9, 1], v1d_followup_25[1..9, 1], v1c_followup_25[1..9, 1],  ///
	baseline_cmp_25[1..9, 1], controls_cmp_25[1..9, 1], wins_cmp_25[1..9, 1], calweights_cmp_25[1..9, 1], nomanuf_cmp_25[1..9, 1], hbalance_cmp_25[1..9, 1], v1d_cmp_25[1..9, 1], v1c_cmp_25[1..9, 1],  ///
	baseline_case_25[1..9, 1], controls_case_25[1..9, 1], wins_case_25[1..9, 1], calweights_case_25[1..9, 1], nomanuf_case_25[1..9, 1], hbalance_case_25[1..9, 1], v1d_case_25[1..9, 1], v1c_case_25[1..9, 1]



matrix colnames comp_t05 = ///
	1 2 3 4 5 6 7 8 9 10 ///
	11 12 13 14 15 16 17 18 19 20 ///
	21 22 23 24 25 26 27 28 29 30 ///
	31 32 33 34 35 36 37 38 39 40 ///
	

// 1. Prepare a dataset with group labels and positions:
clear
input xpos ypos str20 group_label
4.5 0.35 "Violations"
12.5 0.35 "Inspections"
20.5 0.35 "Follow-up"
28.5 0.35 "CMP collect"
36.5 0.35 "CMP cases"
end

// 2. Save this temp dataset
tempfile grouplabels
save `grouplabels'

coefplot ///
    (matrix(comp_t05), keep(1 2 3 4 5 6 7 8) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t05), keep(9 10 11 12 13 14 15 16) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t05), keep(17 18 19 20 21 22 23 24) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t05), keep(25 26 27 28 29 30 31 32 ) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t05), keep(33 34 35 36 37 38 39 40) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    , vertical ci((5 6)) recast(bar) barwidth(0.3) fcolor(*.3) ///
    citop ///
    graphregion(color(white)) bgcolor(white) ///
    legend(off) ///
xlabel(1 `"Baseline"' 2 `"Controls"' 3 `"Wins"' 4 `"Alt. weights"' 5 `"No auth prods"' 6 `"Balance"' 7 `"Brand"' 8 `"Single stage"'  ///
		9 `"Baseline"' 10 `"Controls"' 11 `"Wins"' 12 `"Alt. weights"' 13 `"No auth prods"' 14 `"Balance"' 15 `"Brand"' 16 `"Single stage"' ///
		17 `"Baseline"' 18 `"Controls"' 19 `"Wins"' 20 `"Alt. weights"' 21 `"No auth prods"' 22 `"Balance"' 23 `"Brand"' 24`"Single stage"'  ///
		25 `"Baseline"' 26 `"Controls"' 27 `"Wins"' 28 `"Alt. weights"' 29 `"No auth prods"' 30 `"Balance"' 31 `"Brand"' 32`"Single stage"'  ///
		33 `"Baseline"' 34 `"Controls"' 35 `"Wins"' 36 `"Alt. weights"' 37 `"No auth prods"' 38 `"Balance"' 39 `"Brand"' 40`"Single stage"'  ///
    , angle(90) labsize(small) ) ///
	xtick(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40, tlength(*.5)) ///
    xline(8.5 16.5 24.5 32.5, lpattern(dash) lcolor(gs12)) ///
    addplot(scatter ypos xpos, mlabel(group_label) mlabpos(12) mlabcolor(black) mlabsize(small) msymbol(none))
graph export "${figs}\r_insp_2a_b_vape_fisc_further_robust_checks_pre_treatment_thru_11_2024.png", width(2400) height(1800) replace































/*
mat comp_t6 = nowins_viol_75[1..9, 14], wins_viol_75[1..9, 14], ///
	nowins_insp_25[1..9, 14], wins_insp_25[1..9, 14], ///
	nowins_followup_25[1..9, 14], wins_followup_25[1..9, 14], ///
	nowins_cmp_25[1..9, 14], wins_cmp_25[1..9, 14], ///
	nowins_case_25[1..9, 14], wins_case_25[1..9, 14] ///

matrix colnames comp_t6 = 1 2 3 4 5 6 7 8 9 10

// 1. Prepare a dataset with group labels and positions:
clear
input xpos ypos str20 group_label
1.5 0.35 "Violations"
3.5 0.35 "Inspections"
5.5 0.35 "Follow-up"
7.5 0.35 "CMP collect"
9.5 0.35 "CMP cases"
end

// 2. Save this temp dataset
tempfile grouplabels
save `grouplabels'

coefplot ///
    (matrix(comp_t6), keep(1 2) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t6), keep(3 4) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t6), keep(5 6) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t6), keep(7 8) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    (matrix(comp_t6), keep(9 10) lcolor(gs4) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
    , vertical ci((5 6)) recast(bar) barwidth(0.3) fcolor(*.3) ///
    citop ///
    graphregion(color(white)) bgcolor(white) ///
    legend(off) ///
xlabel(1 `"No wins"' 2 `"1% Wins"' ///
	3 `"No wins"' 4 `"1% Wins"' ///
	5 `"No wins"' 6 `"1% Wins"' ///
	7 `"No wins"' 8 `"1% Wins"' ///
	9 `"No wins"' 10 `"1% Wins"' ///
    , angle(0) labsize(small) alternate) ///
	xtick(1 2 3 4 5 6 7 8 9 10, tlength(*.5)) ///
    xline(2.5 4.5 6.5 8.5, lpattern(dash) lcolor(gs12)) ///
    addplot(scatter ypos xpos, mlabel(group_label) mlabpos(12) mlabcolor(black) mlabsize(medium) msymbol(none))
graph export "${figs}\r_wins05_heterogeneity_fisc.png", replace
*/

