global regs	"G:\My Drive\GSEFM\Research\e_cigarettes\output\regressions"

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
*	Regressions
********************************************************************************

***************************
// Violative stores
use "G:\My Drive\GSEFM\Research\e_cigarettes\data\processed_data\LS_Otter\estimation_panels\t_and_s_insp_issue_2a_b.dta", clear

encode state, gen(statenr)
label list statenr // view numbers assigned to counties

* Sort the data by retailer license and date
sort store_id date

isid store_id date

drop if sister == 1

xtset store_id date


// treatment at inspection
gen treatment_date = date if t_insp == 1

* Replace treatment_date with the maximum treatment date for each retailer (in case of multiple treatments)
by store_id: egen min_treatment_date = min(treatment_date) // why not min(treatment_date) ?

* Generate the month-since-treatment variable, which calculates the difference between the current date and the treatment date
gen month_since_treatment = (date - min_treatment_date)


drop if state == "nan"
drop if state == "None"

quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11), pretrend(5) horiz(0/10) minn(0) cluster(store_id) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\t_insp_2a_b_bsj_vape_qty_1b_fisc_t10_thru_11_2024", replace

preserve
sort date
by date: egen p_1 = pctile(l_vape_qty_index_1b), p(.5)
by date: egen p_99 = pctile(l_vape_qty_index_1b), p(99.5)
replace l_vape_qty_index_1b = p_1 if l_vape_qty_index_1b < p_1
replace l_vape_qty_index_1b = p_99 if l_vape_qty_index_1b > p_99

quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11), pretrend(5) horiz(0/10) minn(0) cluster(store_id) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\t_insp_2a_b_bsj_vape_qty_1b_fisc_t10_wins05_thru_11_2024", replace
restore


***************************
// Rival stores
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

quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11), pretrend(5) horiz(0/10) minn(0) cluster(zip_code) avgeffectsby(month_since_treatment) autosample 
estimates save "$regs\r_insp_2a_b_bsj_vape_qty_1b_fisc_t10_thru_11_2024", replace

preserve
sort date
by date: egen p_1 = pctile(l_vape_qty_index_1b), p(.5)
by date: egen p_99 = pctile(l_vape_qty_index_1b), p(99.5)
replace l_vape_qty_index_1b = p_1 if l_vape_qty_index_1b < p_1
replace l_vape_qty_index_1b = p_99 if l_vape_qty_index_1b > p_99

quietly did_imputation l_vape_qty_index_1b store_id date min_treatment_date if date <= ym(2024, 11), pretrend(5) horiz(0/10) minn(0) cluster(store_id) avgeffectsby(month_since_treatment) autosample 

estimates save "$regs\r_insp_2a_b_bsj_vape_qty_1b_fisc_t10_wins05_thru_11_2024", replace
restore

********************************************************************************
*	Cumulative sums
********************************************************************************

// Violative stores
estimates use "$regs\t_insp_2a_b_bsj_vape_qty_1b_fisc_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat t_nowins = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list t_nowins

estimates use "$regs\t_insp_2a_b_bsj_vape_qty_1b_fisc_t10_wins05_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat t_wins05 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list t_wins05

// Rival stores
estimates use "$regs\r_insp_2a_b_bsj_vape_qty_1b_fisc_t10_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat r_nowins = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list r_nowins

estimates use "$regs\r_insp_2a_b_bsj_vape_qty_1b_fisc_t10_wins05_thru_11_2024"
xlincom (t05 = $dl_04b) (t04 = $dl_03b) (t03 = $dl_02b) (t02 = $dl_01b) (t01 = $event_baseb) (t = $dl_0b) (t1 = $dl_1b) (t2 = $dl_2b) (t3 = $dl_3b) (t4 = $dl_4b) (t5 = $dl_5b) (t6 = $dl_6b) (t7 = $dl_7b) (t8 = $dl_8b) (t9 = $dl_9b) (t10 = $dl_10b) , level(90) post
mat r = r(table)
mat r_wins05 = r[1..9, 1..16]  // Store coefficients, SEs, CIs
mat list r_wins05

********************************************************************************
*	Plot figures
********************************************************************************

// violative stores
coefplot ///
		(matrix(t_nowins), label(Baseline) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
		(matrix(t_wins05), label(Winsorized) lcolor(gs6) msymbol(Dh) mcolor(gs6) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
	graphregion(color(white)) bgcolor(white) offset //ylabel(-.5(.1).1)
	graph export "${figs}t_insp_2a_b_bsj_vape_fisc_t_10_wins_vs_nowins_thru_11_2024.png", replace

// rivals
coefplot ///
		(matrix(r_nowins), label(Baseline) lcolor(gs4) msymbol(T) mcolor(gs4) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))) ///
		(matrix(r_wins05), label(Winsorized) lcolor(gs6) msymbol(Dh) mcolor(gs6) ciopts(recast(rcap) lwidth(thin) lcolor(gs4%85))), ///
vertical ci((5 6)) recast(connected) /// 
    yline(0, lwidth(vthin) lcolor(blue)) rename(t06 = t-6  t05 = t-5 t04 = t-4 t03 = t-3 ///
    t02 = t-2 t01 = t-1 t1 = t+1 t2 = t+2 t3 = t+3 t4 = t+4 t5 = t+5 t6 = t+6 t7 = t+7 t8 = t+8 ///
	t9 = t+9 t10 = t+10) xline(5.5, lwidth(thin)) /// 
	graphregion(color(white)) bgcolor(white) offset //ylabel(-.5(.1).1)
	graph export "${figs}r_insp_2a_b_bsj_vape_fisc_t_10_wins_vs_nowins_thru_11_2024.png", replace



