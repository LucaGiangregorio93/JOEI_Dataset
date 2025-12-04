clear 
set more off, perm 


global path "/Users/lucagiangregorio/OneDrive - upf.edu/JRC/Italia/storico_stata"
global output "/Users/lucagiangregorio/OneDrive - upf.edu/JRC/Italia/output files" 
global graphs "/Users/lucagiangregorio/OneDrive - upf.edu/jrc/Italia/distributions" 


** Import and merge the files for the Italian dataset ** 

* Check the duplicates in all files we need 
cd "$path" 
fs *.dta
foreach file in `r(files)' { 
	use `file', clear 
	duplicates report 
	duplicates drop 
	save `file', replace 
} 


/* To be able to merge all the possible files, we first need to combine those who are at the hh level, as 
FAMI, CONS and RICF */ 
use "$path/fami.dta", clear 
merge 1:1 anno nquest using "$path/ricf.dta" , nogen 
/* Perfect match */ 

merge 1:1 anno nquest using "$path/cons.dta", nogen 
/* Perfect match */ 

save "$path/hh_file.dta", replace 

* Append the properties with the selected variables  * 
use "$path/immp.dta", clear 
append using "$path/imma.dta" 
keep anno nquest resid tipoimm qpro ubic1 supab usoimm usoimmn valabit affeff affimp propriet proprien tipouso affpag affpagi  
gen owner=. 
replace owner=1 if tipouso==. 
replace owner=0 if tipouso==1 | tipouso==2 
save "$path/properties.dta", replace 

/* To have a file of individuals within household, we cannot join the properties file with its details. We can take the 
sum of values of the values of the property and rents. But the other variables cannot be used joined with hh-individual file */ 

preserve 
collapse (sum) valabit affeff affimp affpag affpagi, by(anno nquest) 
save "$path/property_hh.dta", replace 
restore 


* Adjust the labour files to be merged 
use "$path/ldip.dta", clear 
gen employee=1 
rename oretot oretot_emp
keep if attivp==1 
keep anno nquest nord attivp partean partime contratt dimaz oretot_emp orestra employee
duplicates drop nquest anno nord, force
save "$path/ldip2.dta", replace 


use "$path/linb.dta" , clear
rename numadd numadd_linb
keep if attivp==1 
gen self_employed=1
rename oretot oretot_linb
duplicates drop nquest anno nord, force
save "$path/linb2.dta", replace

use "$path/linc.dta" , clear
rename numadd numadd_linc
keep if attivp==1 
gen employers=1 
rename oretot oretot_linc
duplicates drop nquest anno nord, force
save "$path/linc2.dta", replace


use "$path/lind.dta" , clear
rename numadd numadd_lind
rename ind1 nord
gen fam_business=1 
duplicates drop nquest anno nord, force
save "$path/lind2.dta", replace


** Start to merge the comp (socio-demographic) with the income flows data ** 
use "$path/comp.dta", clear 
merge 1:1 nord nquest anno using "$path/rper.dta" 
rename _merge merge1 
/* The non-matched, 170,916, are those who don't have a labour income: 
tab perl if _merge==1 is 100% of 0! 
*/ 

** Now we merge with the hh-file ** 
merge m:1 anno nquest using "$path/hh_file.dta", nogen 
/* Perfect match with the hh-file */ 


** Add the transfers file ** 
merge m:1 anno nquest nord using "$path/tras.dta"
rename _merge merge2 
/* All the observation of the transfer file - 13,214 - are matched. The non-matched are those hh 
who don't have any transfer */ 


* Add the labour files 
merge 1:1 anno nquest nord using "$path/ldip2.dta" 
rename _merge mergedip 

/* tab nonoc if mergedip!=3 
90% of those not matched are not employed */ 

merge 1:1 anno nquest nord using "$path/linb2.dta" 
rename _merge mergelinb 

merge 1:1 anno nquest nord using "$path/linc2.dta" 
rename _merge mergelinc 

merge 1:1 anno nquest nord using "$path/lind2.dta" 
rename _merge mergelind 

/* 
tab perl if merge3==1 gives 98.04% of not receiving income. 1.96 having income.
tab nonoc if _merge==1 & anno>=1991, m 
97.82% are inactives after 1991  
*/ 


** Add the sum values of properties  ** 
merge m:1 anno nquest using "$path/property_hh.dta" 

/* There are 58 observations not matched from the master. 43 of them referring to 1977, 8 tp 1978, 7 in 1979 and 1 in 1984. */ 


** Adding weights ** 
merge m:1 anno nquest using "$path/peso.dta", nogen 

/* As for the deflators, we have base 2010. Acknowledging that, we can keep only the matched years and use the 
corresponding deflators */ 

merge m:1 anno using "$path/defl.dta", keep(match) nogen

*Check eventually duplicates 
duplicates report anno nquest nord 
duplicates report 

/* Correctly no duplicates */ 

save "$output/completed_italy_lab.dta", replace 



******* DEFINE THE NEEDED INCOME FLOWS VARIABLES *******
use "$output/completed_italy_lab.dta", clear 

gen netlabincome_employee=yl 

/* The depreciation changes in 2004 i.e. values of ym3 will be net of depreciation. Therefore, we need to discount 
the ym3 and ym1 by the depreciation (ym2) */ 

replace ym2=0 if ym2==.
replace ym3=ym3-ym2
replace ym1=ym1-ym2

* The self-employed income is only for those having ym1>0 but numadd<=1 i.e. not having any employee (1 here is assumed to be the individual enterpreneuer/self-employed)
* Set the number of employees all together 
gen num_employees_it=numadd_linb 
replace num_employees_it=numadd_linc if numadd_linc!=. 
replace num_employees_it=numadd_lind if numadd_lind!=. 

gen netlabincome_selfemp=ym1 if num_employees_it<=1 & num_employees_it!=. 
replace netlabincome_selfemp=0 if netlabincome_selfemp==.


gen net_profits=ym3 
replace net_profits=ym1 if (ym3==0 | ym3==.) &  num_employees_it>1 & num_employees_it!=. 


gen financial_income=ycf 
/* if we want to detail the financial, in Italian case we have: 
- interests on accounts 
- interests on treasury bills
- interests on other financial activities 
- negative interests 
*/ 
gen rental_income = yca1 
gen imputed_rents = yca2 

gen transfers_income=yt 

******* DEFINE THE NEEDED WEALTH STOCK VARIABLES *******
gen net_wealth=w 

gen real_wealth= ar 
/* keep its detailed components? */ 

gen financial_wealth=af 

gen overall_debts =pf 

******* LABELLING AND HOMOGENIZE THE POSSIBLE SOCIO-ECONOMIC VARIABLES *******
*Gender & age 
rename anno year 
recode sesso (1=1 "Male") (2=0 "Female"), gen(gender) 

gen age=eta

rename anasc birth_year

* HH size, marital status & type of family
gen hh_size=ncomp 

gen marital_status=staciv 
label define marital_status 1 "Married" 2 "Single" 3 "Separated/divorced" 4 "Widowed" 
label values marital_status marital_status 

recode tipofam (1 2 = 1 "Single" ) (5=2 "Single-parents") (3 = 3 "Couples w/out child") (4=4 "Couples with child") (6=5 "Other"), gen(hh_type) 


* Education 
gen isced97=studio
label define isced97 1 "Isced 0" 2 "Isced 1" 3 "Isced 2" 4 "Isced 3" 5 "Isced 5" 6 "Isced 6" 
label values isced97 isced97 

* Social background
gen father_edu=stupcf 
replace father_edu=. if stupcf==7 
label values father_edu isced97 

gen mother_edu=stumcf 
replace mother_edu=. if stumcf==7 
label values mother_edu isced97 

* Labour market variables
gen occupation_it=qualp10 
label define occupation_it 1 "Blue-collar worker" 2 "Employee/teachers" 3 "Junior managers" 4 "Managers" 5 "Liberal professional" 6 "Individual enterpreneuer" 7 "Autonomous" 8 "Family business" 9 "Business partner" 10 "Non-employed"  
label values occupation_it occupation_it


gen contract_type_it=contratt 
label define contract_type 1 "Permanent" 2 "Temporary" 3 "Agency contract" 
label values contract_type_it contract_type 

rename partime ptime 
label define partime 1 "Yes" 0 "No" 
label values ptime partime 

gen firm_size_it = dimaz 
label define firm_size_it 1 "<= 4" 2 "5-19" 3 "20-49" 4 "50-99" 5 "100-499" 6 ">=500" 7 "Public employee" 
labe values firm_size_it firm_size_it

rename orestra extra_wk_hours 

* Sector labelling 
recode settp11 (8=9) (9=8), gen(nace1_it) 
replace nace1_it=. if settp11==11 
label define nace1_it 1 "A - agriculture" 2 "D - Manufacturing" 3 "F - constructions" 4 "G+H - retail, restaurants & hotels" 5 "I - transports & comm." 6 "J - Financial intermediation" 7 "K - real estate, renting & business activities " 8 "L - public administration  " 9 "P - private hh services" 10 "Q - Extraterritorial organizations " 
label values nace1_it nace1_it 

* Create the emp status based on nonoc and lab status 
gen emp_status=. 
replace emp_status=1 if nonoc==0 & employee==1 
replace emp_status=2 if nonoc==0 & self_employed==1 | employers==1 | fam_business==1 
replace emp_status=3 if nonoc==4 
replace emp_status=4 if nonoc==5 
replace emp_status=5 if nonoc==6 
replace emp_status=6 if inlist(nonoc, 1, 2, 3, 7) 
label define emp_status 1 "Employee" 2 "Self-employed" 3 "Retired" 4 "Unemployed" 5 "Students" 6 "Other inactives"  
label values emp_status emp_status

* Type of self employment 
rename profn type_selfemp_it 
label define type_selfemp_it 1 "Liberal professional" 2 "Single entrepreneur" 3 "Autonomous" 4 "Firm partner" 
label values type_selfemp_it type_selfemp_it 

* Set the working hours all together 
gen wk_hours=oretot_emp
replace wk_hours=oretot_linb if oretot_linb!=. 
replace wk_hours=oretot_linc if oretot_linc!=. 


* Tenant status 
gen tenant_status=godab
label define tenant_status 1 "Owner" 2 "Tenant" 3 "Usufructuary/free rent" 4 "rent to buy" 
label values tenant_status tenant_status 


*Geographical area 
label define area5 1 "North-west" 2 "North-east" 3 "Centre" 4 "South" 5 "Islands" 
label values area5 area5 

label define acom5 1 "<= 4999" 2 "5000-19999" 3 "20000-49999" 4 "50000-199999" 5 ">= 200000" 
label values acom5 acom5 

**** COLLAPSE AT THE HH LEVEL **** 
/* The variable cfdic defines whether the individual respondent is the household-head. However, we cannot collapse the set 
by the hh head to obtain values at the household level, as we have socio-demographic charactersitcs that cannot be collased as sum. Therefore,
we firstly create the total values of income within the hh - as the wealth values are already at hh level - and then keep only the hh==1. */ 

* Computing the total of individual flows at the hh level 
bysort nquest year : egen hh_netlabincome_employee=total(netlabincome_employee) 
bysort nquest year : egen hh_netlabincome_selfemp=total(netlabincome_selfemp) 
bysort nquest year : egen hh_net_profits=total(net_profits) 
bysort nquest year : egen hh_financial_income=total(financial_income)
bysort nquest year: egen hh_rental_income=total(rental_income) 
bysort nquest year: egen hh_imputed_rents=total(imputed_rents) 
bysort nquest year: egen hh_transfers_income=total(transfers_income) 

* Keep only the householder 
keep if cfdic==1 

* Use the Eurostat deflator 
gen deflator=0.779 if year==2002 
replace deflator=0.819 if year==2004
replace deflator=0.856 if year==2006
replace deflator=0.904 if year==2008
replace deflator=0.926 if year==2010
replace deflator=0.984 if year==2012
replace deflator=0.999 if year==2014
replace deflator=0.999 if year==2016


save "$output/completed_hh_italy_lab.dta", replace 


******* DESCRIPTIVES *******
use "$output/completed_hh_italy_lab.dta", clear
 
* 1. Applying the Eurostat deflator  
foreach v of var hh_netlabincome_employee hh_netlabincome_selfemp hh_net_profits hh_rental_income hh_financial_income hh_imputed_rents net_wealth real_wealth financial_wealth { 
	gen defl_`v'=`v'/deflator	
	gen log_`v'=ln(defl_`v')
}  

/* 
levelsof year, local(a) 
foreach v of var defl_* { 
	foreach yearin `a' { 
		sum `v' if year==`year' & year>=1991 [aw=pesopop], det 
		}
} 
*/ 

* Computing the share of hh with values greater than 0 for all income and wealth components 
bysort year: egen tot_hh=count(nquest) 
foreach v of var hh_netlabincome_employee hh_netlabincome_selfemp hh_net_profits hh_rental_income hh_financial_income hh_imputed_rents net_wealth real_wealth financial_wealth { 
	bysort year: egen `v'_no0=count(`v') if `v'>0 
	gen pp_`v'= (`v'_no0/tot_hh)*100
	
} 



* Overall sum looking at mean sd min max
preserve 
rename defl_hh_netlabincome_employee employee_wage
rename defl_hh_netlabincome_selfemp selfemp_income
estpost tabstat employee_wage selfemp_income defl_hh_net_profits defl_net_wealth if year>=1991 [aw=pesopop] , by(year) stat(mean sd min max) not 
esttab using summaries.csv, cells("employee_wage selfemp_income defl_hh_net_profits defl_net_wealth") 

* Doing it with values >0 
foreach v of var employee_wage selfemp_income defl_hh_net_profits defl_net_wealth { 
	estpost tabstat `v' if `v'>0 & year>=1991 [aw=pesopop], by(year) stat(mean sd min max) not 
	esttab using summaries_no0.csv, cells("mean sd min max") append
} 

restore 
 

 
* 2. Starts with plotting the distribution at different points in time 
* Employee income
twoway kdensity log_hh_netlabincome_employee if year==1991 [aw=pesopop], lpattern(dash) || kdensity log_hh_netlabincome_employee if year==2002 [aw=pesopop] || kdensity log_hh_netlabincome_employee if year==2010 [aw=pesopop] || kdensity log_hh_netlabincome_employee if year==2016 [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("(log) employee income") 
graph export "$graphs/employee_income.png", replace 

pshare log_hh_netlabincome_employee if year==1991 [pw=pesopop], nq(10)  
pshare log_hh_netlabincome_employee if year==2016 [pw=pesopop], nq(10)  

/* The bottom 10 decreased its employee income share between 1991 and 2016, while the bottom 10 increased */ 


* Self-employed income 
twoway kdensity log_hh_netlabincome_selfemp if year==1991 [aw=pesopop], lpattern(dash) || kdensity log_hh_netlabincome_selfemp if year==2002 [aw=pesopop] || kdensity log_hh_netlabincome_selfemp if year==2010 [aw=pesopop] || kdensity log_hh_netlabincome_selfemp if year==2016 [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("(log) self-employed income") 
graph export "$graphs/selfemployed_income.png", replace 


* Profits 
twoway kdensity log_hh_net_profits if year==1991  [aw=pesopop], lpattern(dash) || kdensity log_hh_net_profits if year==2002 [aw=pesopop] || kdensity log_hh_net_profits if year==2010 [aw=pesopop] || kdensity log_hh_net_profits if year==2016  [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("(log) Profits") 
graph export "$graphs/profits.png", replace 

* Profits without log-transformation (as negative values will be excluded)  
twoway kdensity defl_hh_net_profits if year==1991  [aw=pesopop], lpattern(dash) || kdensity defl_hh_net_profits if year==2002 [aw=pesopop] || kdensity defl_hh_net_profits if year==2010 [aw=pesopop] || kdensity defl_hh_net_profits if year==2016  [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("Profits") 

/* Basically since 2004 we don't have negative profits. So the negative before 2004 are due to the adjustments of the depreciation. */ 

* Financial income - as it has a lot of negative values concentrated at the bottom we don't use the log-transformation
twoway kdensity defl_hh_financial_income if year==1991  [aw=pesopop], lpattern(dash) || kdensity defl_hh_financial_income if year==2002 [aw=pesopop] || kdensity defl_hh_financial_income if year==2010 [aw=pesopop] || kdensity defl_hh_financial_income if year==2016 [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("Financial income") 

/* sum defl_hh_financial_income if year==1991 [aw=pesopop], det 
sum defl_hh_financial_income if year==2002 [aw=pesopop], det 
sum defl_hh_financial_income if year==2010 [aw=pesopop], det 
sum defl_hh_financial_income if year==2016 [aw=pesopop], det 

The mean has decreasing trend over time and start to be negative in 2010 
*/ 
* Indeed, using the log-transformation
twoway kdensity log_hh_financial_income if year==1991  [aw=pesopop], lpattern(dash) || kdensity log_hh_financial_income if year==2002 [aw=pesopop] || kdensity log_hh_financial_income if year==2010 [aw=pesopop] || kdensity log_hh_financial_income if year==2016 [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("Financial income") 
graph export "$graphs/financial_income.png", replace 


* Rental income
twoway kdensity log_hh_rental_income if year==1991 [aw=pesopop], lpattern(dash) || kdensity log_hh_rental_income if year==2002 [aw=pesopop] || kdensity log_hh_rental_income if year==2010 [aw=pesopop] || kdensity log_hh_rental_income if year==2016 [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("(log) Rental income") 
graph export "$graphs/rental_income.png", replace 

* Imputed rents 
twoway kdensity log_hh_imputed_rents if year==1991 [aw=pesopop], lpattern(dash) || kdensity log_hh_imputed_rents if year==2002 [aw=pesopop] || kdensity log_hh_imputed_rents if year==2010 [aw=pesopop] || kdensity log_hh_imputed_rents if year==2016 [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("(log) Imputed rents") 
graph export "$graphs/imputed_rents.png", replace 


* Net wealth 
pshare net_wealth if year==1991 [pw=pesopop], nq(10) 
pshare net_wealth if year==2016 [pw=pesopop], nq(10) 

* Wealth accumulates strongly at the top: the top 90th is the only one gaining in wealth shares 

twoway kdensity defl_net_wealth if year==1991 [aw=pesopop], lpattern(dash) || kdensity defl_net_wealth if year==2002 [aw=pesopop] || kdensity defl_net_wealth if year==2010 [aw=pesopop] || kdensity defl_net_wealth if year==2016 [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("Net wealth")

* Excluding the 0 wealth using the log-transformation
twoway kdensity log_net_wealth if year==1991 [aw=pesopop], lpattern(dash) || kdensity log_net_wealth if year==2002 [aw=pesopop] || kdensity log_net_wealth if year==2010 [aw=pesopop] || kdensity log_net_wealth if year==2016 [aw=pesopop] /// 
, legend(label(1 "1991") label(2 "2002") label(3 "2010") label(4 "2016")) ytitle("Density") xtitle("Net wealth")
graph export "$graphs/net_wealth.png", replace 


** Plotting the means over time ** 
preserve 
collapse hh_netlabincome_employee hh_netlabincome_selfemp hh_net_profits hh_rental_income hh_financial_income hh_imputed_rents net_wealth real_wealth financial_wealth [pw=pesopop], by(anno) 
keep if anno>=1991
twoway line hh_netlabincome_employee year, lpattern(dash) || line hh_netlabincome_selfemp year|| line hh_net_profits year /// 
, legend(label(1 "Employee income") label(2 "Self-employed income") label(3 "Profits")) ytitle("Mean values")
restore 

/* Profits main problem: of course the mean is so small because of 0 values. 
Furthermore, as mentioned, up to 2002 there are more than 6,000 records with negative profits. 
This is due to the adjustment with depreciation. */ 


 
**** Rename the income and wealth variables to be equal across countries **** 
rename hh_netlabincome_employee empl_income_it 
rename hh_netlabincome_selfemp selfemp_income_it
rename hh_net_profits profit_it 
rename hh_financial_income financial_income_it
rename hh_rental_income rental_income_it
rename hh_imputed_rents imputed_rents_it
 
rename nquest hh_id

rename defl defl_italy 
keep hh_id year gender age birth_year hh_size hh_type marital_status father_edu mother_edu isced97 area5 acom5 tenant_status emp_status occupation_it nace1_it firm_size_it type_selfemp_it contract_type_it ptime wk_hours extra_wk_hours num_employees_it /// 
empl_income_it selfemp_income_it financial_income_it imputed_rents_it rental_income_it profit_it net_wealth real_wealth financial_wealth overall_debts pesopop deflator defl_italy  

gen country="Italy"  
keep if year>=1991 
save "$output/ready_to_append_it.dta", replace  
 
