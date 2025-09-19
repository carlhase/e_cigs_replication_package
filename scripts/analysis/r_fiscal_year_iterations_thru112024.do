
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

*************************************************************
// Past violation rates
*******************************

quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (viol_75 == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_t10_thru_11_2024", replace

// 75th pctile vs all other
quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (viol_75 != 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_bottom_75_t10_thru_11_2024", replace

/*quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (viol_25 == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_bottom_25_t10_thru_11_2024", replace
*/
*************************************************************
// Inspection intensity
*******************************

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (insp_25 == 1  | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_t10_thru_11_2024", replace

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (insp_25 != 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_insp_top_75_t10_thru_11_2024", replace

/*did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (insp_75 == 1  | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_insp_top_25_t10_thru_11_2024", replace
*/

	
*************************************************************
// follow up inspections 
*******************************

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (follow_25 == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_t10_thru_11_2024", replace

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (follow_25 != 1 | non_rival == 1) , pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_followup_top_75_t10_thru_11_2024", replace

/*did_imputation l_vape_qty_index_1b store_id date min_treatment_date if follow_80 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_followup_top_25_t10", replace
*/

*************************************************************
// CMP collected vs issued - low ratio states vs high ratio states
*******************************

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (cmp_25 == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_t10_thru_11_2024", replace

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (cmp_25 != 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_top_75_t10_thru_11_2024", replace

/*did_imputation l_vape_qty_index_1b store_id date min_treatment_date if cmp_80 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_top_25_t10", replace
*/
*************************************************************
// civil case rates
*******************************

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (case_file_25 == 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_t10_thru_11_2024", replace

did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11) & (case_file_25 != 1 | non_rival == 1), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_case_top_75_t10_thru_11_2024", replace

/*
did_imputation l_vape_qty_index_1b store_id date min_treatment_date if case_file_80 == 1 | non_rival == 1, pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_fisc_case_top_25_t10", replace
*/







********************************************************************************
// Figures
********************************************************************************
// Violations per inspection
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat viol_per_insp_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list viol_per_insp_75

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_viol_per_insp_bottom_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat viol_per_insp_bottom75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list viol_per_insp_bottom75

    coefplot ///
	(matrix(viol_per_insp_75),  label(High violation states) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
	(matrix(viol_per_insp_bottom75), label(All other states) lcolor(gs4) msymbol(Dh) mcolor(gs6) ciopts(recast(rcap) lwidth(thin) lcolor(gs6%85))), ///
	vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
	graph export "${figs}r_insp_2a_b_bsj_vape_viol_per_insp_75_vs_other_defaultfe_t10_fisc_thru_11_2024.png", replace


// Inspection intensity
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_insp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat insp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list insp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_insp_top_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat insp_top_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list insp_top_75
    
	coefplot ///
	(matrix(insp_25),  label(Low inspection states) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
	(matrix(insp_top_75), label(All other states) lcolor(gs4) msymbol(Dh) mcolor(gs6) ciopts(recast(rcap) lwidth(thin) lcolor(gs6%85))), ///
	vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
	graph export "${figs}r_insp_2a_b_bsj_vape_insp_per_100k_25_vs_other_defaultfe_t10_fisc_thru_11_2024.png", replace


// Follow up inspection intensity
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_followup_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat followup_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list followup_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_followup_top_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat followup_top_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list followup_top_75

    coefplot ///
	(matrix(followup_25),  label(Low follow-up states) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
	(matrix(followup_top_75), label(All other states) lcolor(gs4) msymbol(Dh) mcolor(gs6) ciopts(recast(rcap) lwidth(thin) lcolor(gs6%85))), ///
	vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.4(.2).6, angle(horizontal))
	graph export "${figs}r_insp_2a_b_bsj_vape_followup_insp_25_vs_other_defaultfe_t10_fisc_thru_11_2024.png", replace


// CMP collection rates
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat cmp_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list cmp_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_cmp_top_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat cmp_top_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list cmp_top_75

    coefplot ///
	(matrix(cmp_25),  label(Low ratio states) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
	(matrix(cmp_top_75), label(All other states) lcolor(gs4) msymbol(Dh) mcolor(gs6) ciopts(recast(rcap) lwidth(thin) lcolor(gs6%85))), ///
	vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.15(.05).5, angle(horizontal))
	graph export "${figs}r_insp_2a_b_bsj_vape_cmp_collections_ratio_25_vs_other_defaultfe_t10_fisc_thru_11_2024.png", replace


// Civil case rates
estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_case_25_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat civil_case_25 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list civil_case_25

estimates use "$regs\r_insp_2a_b_bsj_vape_fisc_case_top_75_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat civil_case_top_75 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list civil_case_top_75

    coefplot ///
	(matrix(civil_case_25),  label(Low filing states) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
	(matrix(civil_case_top_75), label(All other states) lcolor(gs4) msymbol(Dh) mcolor(gs6) ciopts(recast(rcap) lwidth(thin) lcolor(gs6%85))), ///
	vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
    graphregion(color(white)) bgcolor(white) //ylabel(-.4(.2).6, angle(horizontal))
	graph export "${figs}r_insp_2a_b_bsj_vape_penalty_filing_2017_25_vs_other_defaultfe_t10_fisc_thru_11_2024.png", replace





