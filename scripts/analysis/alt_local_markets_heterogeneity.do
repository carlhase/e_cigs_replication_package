global regs "G:\My Drive\GSEFM\Research\e_cigarettes\output\regressions"
global figs	"G:\My Drive\GSEFM\Research\e_cigarettes\output\"


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


********************************************************************************
********************************************************************************
// Rivals
********************************************************************************
********************************************************************************

*************************************************************
*	3-digit zip code level treatment
*************************************************************

use "G:\My Drive\GSEFM\Research\e_cigarettes\data\processed_data\LS_Otter\estimation_panels\r_insp_issue_2a_b_3zip.dta", clear

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

egen p_insp_75 = pctile(insp_per_100k), p(25)
generate insp_75 = (insp_per_100k <= p_insp_75)

egen p_follow_25 = pctile(pct_viol_flwup_insp_12_mo), p(25)
generate follow_25 = (pct_viol_flwup_insp_12_mo <= p_follow_25)

egen p_cmp_25 = pctile(avg_cmp_collected_vs_issued), p(25)
generate cmp_25 = (avg_cmp_collected_vs_issued <= p_cmp_25)

egen p_case_file_25 = pctile(penalty_share_us_civil_cases_17), p(25)
generate case_file_25 = (penalty_share_us_civil_cases_17 <= p_case_file_25)

// Violations per 100 inspections
quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(three_zip) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_t10", replace

// Inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if insp_75 == 1  | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(three_zip) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_insp_25_t10", replace

// Follow up inspection intensity
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(three_zip) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_followup_25_t10", replace


// CMP collection ratio
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(three_zip) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_cmp_25_t10", replace

// CMP case filing rate
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(three_zip) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_case_25_t10", replace



*************************************************************
*	State x time FE
*************************

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

// Violations per 100 inspections
egen p_viol_75= pctile(viol_per_100_insp), p(75)
generate viol_75 = viol_per_100_insp >= p_viol_75

egen p_insp_75 = pctile(insp_per_100k), p(25)
generate insp_75 = (insp_per_100k <= p_insp_75)

egen p_follow_25 = pctile(pct_viol_flwup_insp_12_mo), p(25)
generate follow_25 = (pct_viol_flwup_insp_12_mo <= p_follow_25)

egen p_cmp_25 = pctile(avg_cmp_collected_vs_issued), p(25)
generate cmp_25 = (avg_cmp_collected_vs_issued <= p_cmp_25)

egen p_case_file_25 = pctile(penalty_share_us_civil_cases_17), p(25)
generate case_file_25 = (penalty_share_us_civil_cases_17 <= p_case_file_25)

// Violations per inspection
quietly did_imputation l_vaping_products_fisc store_id date min_treatment_date if viol_75 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_t10_statetfe", replace

// Inspection intensity
did_imputation l_vaping_products_fisc store_id date min_treatment_date if insp_75 == 1  | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_t10_statetfe", replace

// Follow up inspection intensity
did_imputation l_vaping_products_fisc store_id date min_treatment_date if follow_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_t10_statetfe", replace


// CMP collection ratio
did_imputation l_vaping_products_fisc store_id date min_treatment_date if cmp_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_t10_statetfe", replace

// CMP case filing rate
did_imputation l_vaping_products_fisc store_id date min_treatment_date if case_file_25 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_t10"_statetfe, replace


*************************************************************
*	zip vs 3zip cumulative sums
*************************************************************
// zip code (baseline)
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip_insp_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip_insp_75

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip_follow_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip_follow_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip_case_25


// 3 zip
estimates use "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip3_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip3_viol_75

estimates use "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_insp_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip3_insp_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip3_insp_75

estimates use "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_followup_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip3_follow_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip3_follow_25

estimates use "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_cmp_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip3_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip3_cmp_25

estimates use "$regs\r_3zip_insp_2a_b_bsj_vape_fisc_case_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat zip3_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list zip3_case_25


*************************************************************
*	Plot figure: zip code vs 3 digit zip code
*************************************************************

// zip code vs 3 digit zip code
mat comp_t6 = zip_viol_75[1..9, 14], zip3_viol_75[1..9, 14], zip_insp_75[1..9, 14], ///
zip3_insp_75[1..9, 14], zip_follow_25[1..9, 14], zip3_follow_25[1..9, 14], ///
zip_cmp_25[1..9, 14], zip3_cmp_25[1..9, 14], zip_case_25[1..9, 14], ///
zip3_case_25[1..9, 14]

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
xlabel(1 `"ZIP"' 2 `"3-digit ZIP"' 3 `"ZIP"' 4 `"3-digit ZIP"' ///
           5 `"ZIP"' 6 `"3-digit ZIP"' 7 `"ZIP"' 8 `"3-digit ZIP"' ///
           9 `"ZIP"' 10 `"3-digit ZIP"', angle(0) labsize(small)) ///
    xtick(1 2 3 4 5 6 7 8 9 10, tlength(*.5)) ///
    xline(2.5 4.5 6.5 8.5, lpattern(dash) lcolor(gs12)) ///
    addplot(scatter ypos xpos, mlabel(group_label) mlabpos(12) mlabcolor(black) mlabsize(medium) msymbol(none))
graph export "${figs}\r_zip_vs_3zip_heterogeneity_fisc.png", replace



*************************************************************
*	Cumulative sums: Baseline vs state x time FE
*************************************************************

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_viol_75

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_follow_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_follow_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_case_25


*************************************************************
*	Plot figure: Baseline vs state x time FE
*************************************************************

// zip code vs 3 digit zip code
mat comp_t6 = zip_viol_75[1..9, 14], stfe_viol_75[1..9, 14], zip_insp_75[1..9, 14], ///
stfe_insp_75[1..9, 14], zip_follow_25[1..9, 14], stfe_follow_25[1..9, 14], ///
zip_cmp_25[1..9, 14], stfe_cmp_25[1..9, 14], zip_case_25[1..9, 14], ///
stfe_case_25[1..9, 14]

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
xlabel(1 `"Baseline"' 2 `"State-time FE"' 3 `"Baseline"' 4 `"State-time FE"' ///
           5 `"Baseline"' 6 `"State-time FE"' 7 `"Baseline"' 8 `"State-time FE"' ///
           9 `"Baseline"' 10 `"State-time FE"', angle(0) labsize(small) alternate) ///
    xtick(1 2 3 4 5 6 7 8 9 10, tlength(*.5)) ///
    xline(2.5 4.5 6.5 8.5, lpattern(dash) lcolor(gs12)) ///
    addplot(scatter ypos xpos, mlabel(group_label) mlabpos(12) mlabcolor(black) mlabsize(medium) msymbol(none))
graph export "${figs}\r_baseline_vs_statetimefe_heterogeneity_fisc.png", replace



********************************************************************************
********************************************************************************
// Violative stores
********************************************************************************
********************************************************************************

use "G:\My Drive\GSEFM\Research\e_cigarettes\data\processed_data\LS_Otter\estimation_panels\t_and_s_insp_issue_2a_b.dta", clear

encode state, gen(statenr)
label list statenr // view numbers assigned to counties

* Sort the data by retailer license and date
sort store_id date

xtset store_id date

drop if sister == 1

gen treatment_date = date if t_insp == 1

* Replace treatment_date with the first treatment date for each retailer (in case of multiple treatments)
by store_id: egen min_treatment_date = min(treatment_date) 

* Generate the month-since-treatment variable, which calculates the difference between the current date and the treatment date
gen month_since_treatment = (date - min_treatment_date)

drop if state == "nan"
drop if state == "None"


*********************************************
* Baseline specifications
*************

// Violations per 100 inspections

egen p_viol_75 = pctile(viol_per_100_insp), p(75)
generate viol_75 = viol_per_100_insp >= p_viol_75

quietly did_imputation l_vaping_products_fisc store_id date min_treatment_date if viol_75 == 1 | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat base_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list base_viol_75

// Inspection intensity
egen p_insp_25 = pctile(insp_per_100k), p(25)
generate insp_25 = (insp_per_100k <= p_insp_25)

did_imputation l_vaping_products_fisc store_id date min_treatment_date if insp_25 == 1  | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat base_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list base_insp_25

// Follow up inspection intensity
egen p_follow_25 = pctile(pct_viol_flwup_insp_12_mo), p(25)
generate follow_25 = (pct_viol_flwup_insp_12_mo <= p_follow_25)

did_imputation l_vaping_products_fisc store_id date min_treatment_date if follow_25 == 1 | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat base_follow_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list base_follow_25

// CMP collection ratio
egen p_cmp_25 = pctile(avg_cmp_collected_vs_issued), p(25)
generate cmp_25 = (avg_cmp_collected_vs_issued <= p_cmp_25)

did_imputation l_vaping_products_fisc store_id date min_treatment_date if cmp_25 == 1 | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat base_cmp_20 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list base_cmp_20

// CMP case rates
egen p_case_file_25 = pctile(penalty_share_us_civil_cases_17), p(25)
generate case_file_25 = (penalty_share_us_civil_cases_17 <= p_case_file_25)

did_imputation l_vaping_products_fisc store_id date min_treatment_date if case_file_25 == 1 | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat base_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list base_case_25


*************************************************************
*	State x time FE
*************************
// Violations per 100 inspections

quietly did_imputation l_vaping_products_fisc store_id date min_treatment_date if viol_75 == 1 | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_viol_75

// Inspection intensity
did_imputation l_vaping_products_fisc store_id date min_treatment_date if insp_25 == 1  | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_insp_25

// Follow up inspection intensity

did_imputation l_vaping_products_fisc store_id date min_treatment_date if follow_25 == 1 | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_follow_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_follow_25


// CMP collection ratio

did_imputation l_vaping_products_fisc store_id date min_treatment_date if cmp_25 == 1 | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_cmp_25

// CMP case filing rate

did_imputation l_vaping_products_fisc store_id date min_treatment_date if case_file_25 == 1 | control == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample fe(statenr#date)
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat stfe_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list stfe_case_25


*************************************************************
*	Plot figure: Baseline vs state x time FE
*************************************************************

// zip code vs 3 digit zip code
mat comp_t6 = base_viol_75[1..9, 14], stfe_viol_75[1..9, 14], base_insp_25[1..9, 14], ///
stfe_insp_25[1..9, 14], base_follow_25[1..9, 14], stfe_follow_25[1..9, 14], ///
base_cmp_20[1..9, 14], stfe_cmp_25[1..9, 14], base_case_25[1..9, 14], ///
stfe_case_25[1..9, 14]

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
xlabel(1 `"Baseline"' 2 `"State-time FE"' 3 `"Baseline"' 4 `"State-time FE"' ///
           5 `"Baseline"' 6 `"State-time FE"' 7 `"Baseline"' 8 `"State-time FE"' ///
           9 `"Baseline"' 10 `"State-time FE"', angle(0) labsize(small) alternate) ///
    xtick(1 2 3 4 5 6 7 8 9 10, tlength(*.5)) ///
    xline(2.5 4.5 6.5 8.5, lpattern(dash) lcolor(gs12)) ///
    addplot(scatter ypos xpos, mlabel(group_label) mlabpos(12) mlabcolor(black) mlabsize(medium) msymbol(none))
graph export "${figs}\t_baseline_vs_statetimefe_heterogeneity_fisc.png", replace

