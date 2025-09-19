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

gen pos2 = 0
replace pos2 = 1 if effect >= .2  & effect != .

*************************************************************
// Regressions
*******************************

*******************************
// Violations per inspection
************
gen pos0_viol = 0
replace pos0_viol = 1 if viol_75 == 1 & effect > 0 & effect !=.

gen pos2_viol = 0
replace pos2_viol = 1 if viol_75 == 1 & effect >= .2 & effect !=.

// pos0_high_viol
quietly did_imputation l_vape_price_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (pos0_viol == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_price_1b_pos0_viol_75_t10", replace

// pos2_high_viol
quietly did_imputation l_vape_price_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (pos2_viol == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_price_1b_pos2_viol_75_t10", replace

*******************************
// CMP collection rates
************
gen pos0_cmp = 0
replace pos0_cmp = 1 if cmp_25 == 1 & effect > 0 & effect !=.

gen pos2_cmp = 0
replace pos2_cmp = 1 if cmp_25 == 1 & effect >= .2 & effect !=.

// pos0_high_viol
quietly did_imputation l_vape_price_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (pos0_cmp == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_price_1b_pos0_cmp_25_t10", replace

// pos2_high_viol
quietly did_imputation l_vape_price_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (pos2_cmp == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_price_1b_pos2_cmp_25_t10", replace

*******************************
// CMP case filing rates
************
gen pos0_case = 0
replace pos0_case = 1 if case_file_25 == 1 & effect > 0 & effect !=.

gen pos2_case = 0
replace pos2_case = 1 if case_file_25 == 1 & effect >= .2 & effect !=.

// pos0_high_viol
quietly did_imputation l_vape_price_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (pos0_case == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_price_1b_pos0_case_25_t10", replace

// pos2_high_viol
quietly did_imputation l_vape_price_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (pos2_case == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_price_1b_pos2_case_25_t10", replace


*************************************************************
// Figures
*******************************

// pos0_viol
estimates use "$regs\r_insp_2a_b_bsj_vape_price_1b_pos0_viol_75_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat price_high0_viol = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list price_high0_viol

    coefplot ///
	(matrix(price_high0_viol),  label(Price) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
graph export "${figs}\r_insp_2a_b_bsj_vape_price_1b_pos0_viol_75_t10.png", width(2400) height(1800) replace

// pos2_viol
estimates use "$regs\r_insp_2a_b_bsj_vape_price_1b_pos2_viol_75_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat price_high2_viol = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list price_high2_viol

    coefplot ///
	(matrix(price_high2_viol),  label(Price) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
graph export "${figs}\r_insp_2a_b_bsj_vape_price_1b_pos2_viol_75_t10.png", width(2400) height(1800) replace

// pos0_cmp
estimates use "$regs\r_insp_2a_b_bsj_vape_price_1b_pos0_cmp_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat price_pos0_cmp = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list price_pos0_cmp

    coefplot ///
	(matrix(price_pos0_cmp),  label(Price) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
graph export "${figs}\r_insp_2a_b_bsj_vape_price_1b_pos0_cmp_25_t10.png", width(2400) height(1800) replace

// pos2_cmp
estimates use "$regs\r_insp_2a_b_bsj_vape_price_1b_pos2_cmp_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat price_pos2_cmp = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list price_pos2_cmp

    coefplot ///
	(matrix(price_pos2_cmp),  label(Price) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
graph export "${figs}\r_insp_2a_b_bsj_vape_price_1b_pos2_cmp_25_t10.png", width(2400) height(1800) replace


// pos0_case
estimates use "$regs\r_insp_2a_b_bsj_vape_price_1b_pos0_case_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat price_pos0_case= r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list price_pos0_case

    coefplot ///
	(matrix(price_pos0_case),  label(Price) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
graph export "${figs}\r_insp_2a_b_bsj_vape_price_1b_pos0_case_25_t10.png", width(2400) height(1800) replace

// pos2_case
estimates use "$regs\r_insp_2a_b_bsj_vape_price_1b_pos2_case_25_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat price_pos2_case = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list price_pos2_case

    coefplot ///
	(matrix(price_pos2_case),  label(Price) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
graph export "${figs}\r_insp_2a_b_bsj_vape_price_1b_pos2_case_25_t10.png", width(2400) height(1800) replace


// high2
estimates use "$regs\r_insp_2a_b_bsj_vape_price_1b_high2_t10"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat price_high2 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list price_high2

    coefplot ///
	(matrix(price_high2),  label(Price) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
graph export "${figs}\r_insp_2a_b_bsj_vape_price_1b_high2_t10.png", width(2400) height(1800) replace

// high violation states
estimates use "$regs\r_insp_2a_b_bsj_vape_price_fisc_viol_per_insp_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat baseline_viol_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list baseline_viol_75
    
	coefplot ///
	(matrix(price_high2_viol),  label(Non-compliant rivals) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
	(matrix(baseline_viol_75),  label(All rivals) lcolor(gs6) lpattern(dash) msymbol(Dh) mcolor(gs6) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
graph export "${figs}\r_insp_2a_b_bsj_vape_price_1b_viol_per_insp_75_compl_vs_noncompl_t10_thru_11_2024.png", width(2400) height(1800) replace


// pos2
quietly did_imputation l_vape_price_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (pos2 == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_price_1b_pos2_t10", replace
