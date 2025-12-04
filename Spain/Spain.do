clear 
set more off, perm 

global path "/Users/lucagiangregorio/OneDrive - upf.edu/JRC/Spagna/" 
global output "$path/output" 
global append "$output/append" 
global final_append "$output/final_appended" 
global graphs "/Users/lucagiangregorio/OneDrive - upf.edu/jrc/Spagna/distributions"
 
*** 2002 *** 

foreach c in imp1 imp2 imp3 imp4 imp5 { 
	
/* The socio-demographic variables need a reshape from wide to long in order to have individual rows within hh, as the 
questions are asked to each single individual in the hh. However, the wealth information are at the hh level! The different _x 
are now at the asset perspective, not individual one. Therefore, the wealth variables can be simply taken as given, and then
take the total as the rowsum of the asset */ 

use h_number facine3 p1 p1_1_* p1_2b_* p1_3_* p1_4_* p1_5_* p1_52_* p1_13 p1_14_* renthog mrenthog using "$path/2002/imp1/other_sections_2002_imp1.dta", clear 
save temp1_`c'.dta, replace 

* Reshape wide to long 
reshape long p1_1_@ p1_2b_@ p1_3_@ p1_4_@ p1_5_@ p1_52_@, i(h_number) j(ind_id) 

* Drop the additional rows, as not all hh have 9 members 
bysort h_number : drop if _n>p1 

* Save the socio-demographic 
save "$output/2002/sdem_`c'", replace 

** LABOUR MARKET VARIABLES ** 
use h_number p6_1c* p6_3_* p6_4_* p6_8_* p6_9_* p6_10_* p6_11_* p6_12_* p6_13_* p6_17_* p6_20_* p6_27* p6_29_* p6_30_* p6_31* p6_33* p6_34_* p6_35_* p6_36_* /// 
p6_37_* p6_77_* p6_78_* p6_79_* using "$path/2002/`c'/section6_2002_`c'.dta", clear 


* Take the hh size 
merge 1:m h_number using temp1_`c'.dta, keepus(p1) nogen 

* Adjust the labur market status and sources of income 
forvalues i=1/9 { 
	gen p6_1_`i'=1 if p6_1c1_`i'==1 
	replace p6_1_`i'=2 if p6_1c2_`i'==1 
	replace p6_1_`i'=3 if p6_1c3_`i'==1 
	replace p6_1_`i'=4 if p6_1c4_`i'==1 
	replace p6_1_`i'=5 if p6_1c5_`i'==1 
	replace p6_1_`i'=6 if p6_1c6_`i'==1 
	replace p6_1_`i'=7 if p6_1c7_`i'==1 
	replace p6_1_`i'=8 if p6_1c8_`i'==1 
	
	label define p6_1_`i' 1 "Employee" 2 "Self-employed" 3 "Unemployed" 4 "Retired" 5 "Disabled" 6 "Student" 7 "Housekeeper" 8 "Other inactives"
	label values p6_1_`i' p6_1_`i' 

} 


* Change the orders for the variables that has the hh_member in the wrong place * 
ds p6_10_* p6_11_* p6_12_* p6_13_* p6_17_* p6_20_* p6_33_* p6_34_* p6_35_* p6_36_* p6_37_* 
rename (*_*_*_*) (*_*_*[4]_*[3]) 

* Reshape all the variables from wide to long * 
/* reshape long p6_1_ p6_3_ p6_4_ p6_8_ p6_9_ p6_11_1_ p6_11_2_ p6_11_3_ p6_12_1_ p6_12_2_ p6_12_3_ p6_13_1_ p6_13_2_ p6_13_3_ ///
p6_17_1_ p6_17_2_ p6_17_3_ p6_20_1_ p6_20_2_ p6_20_3_ p6_33_1_ p6_33_2_ p6_33_3_ p6_34_1_ p6_34_2_ p6_34_3_ p6_36_1_ ///
p6_36_2_ p6_36_3_ p6_37_1_ p6_37_2_ p6_37_3_ p6_27_ p6_29_ p6_30_ p6_31_ p6_77_ p6_78_ p6_79_ , i(h_number) j(ind_id) 
*/ 

foreach v of var p* {
    local stub = substr("`v'", 1, strrpos("`v'", "_"))
    local stubs `stubs' `stub'
}

local stubs : list uniq stubs
reshape long `stubs', i(h_number) j(ind_id)

bysort h_number : drop if _n>p1 

* Reshape the variables for the job numer * 
rensfix _

/* In case we want to reshape the jobs as well: 
reshape long p6_11_ p6_12_ p6_13_ p6_17_ p6_20_ p6_33_ p6_34_ p6_36_ p6_37_ , i(h_number ind_id) j(job_num)
However, there are only 37 observations with more than 1 occupation. Therefore, I leave the corresponding variables separated. 

Furthermore, 
egen row_tot=rowtotal(p6_1c1 p6_1c2 p6_1c3 p6_1c4 p6_1c5 p6_1c6 p6_1c7 p6_1c8) 
there are 136 observations that have non-consistent dummy definition for the labour status. 
count if row_tot>1 --> 136. Thinking of dropping them for inconsistency 
*/ 


save "$output/2002/lab_status_`c'", replace 


** WEALTH VARIABLES AND INCOME FLOWS FROM WEALTH ** 
use h_number p2_1 p2_2 p2_3 p2_4 p2_5 p2_7 p2_8 p2_8a p2_22 p2_23 p2_31 p2_32 p2_33 p2_35a_* p2_37_* p2_38_* p2_39_* p2_42_* p2_42s* /// 
p2_43_* p2_39_4 p2_50_* p2_51_* p2_64 p2_72 p2_75 p2_76 p2_79 p2_81 p2_82 p2_84 p2_88 p2_9_* p2_11_* p2_12_* p2_16_* /// 
p2_17_* p2_18_* p2_54_* p2_55_* p2_59_* p2_60_* p2_61_* p2_55_4 p2_61_4 p3_1 p3_2_* p3_5_* p3_6_* p3_9_* p3_10_* ///
p3_11_* p3_19 p3_20 p4_1 p4_2 p4_3 p4_4 p4_5 p4_7_* p4_8* p4_10 p4_12 p4_15 p4_17 p4_18 p4_21 p4_24 p4_27 p4_28 p4_28a p4_31_* ///
p4_33 p4_35 p4_38 p4_39 p5_1 p5_6_* p5_6a_* p5_7_* p5_9 p5_10 p5_13_* p5_14_* p5_16_* p5_17a_* p5_17b_* p4_40 p4_16 p2_24 p4_25 p4_36 using "$path/2002/`c'/other_sections_2002_`c'.dta", clear 


* Accounts and deposits usable for payments 
gen np4_5=(p4_5==1 & p4_7_3>0 & p4_7_3~=.)

* Unlisted shares 
gen np4_18=(p4_18==1 & p4_24>0 & p4_24~=.)

* House purchase savings account and other accounts not for saving
gen cuentas=(p4_3==1|p4_4==1)

* To calculate the percentage of households that own unit-linked or mixed life insurance we generate a new variable
gen seguro=(p5_13_1==2| p5_13_2==2| p5_13_3==2| p5_13_4==2| p5_13_5==2| p5_13_6==2| p5_13_1==3| p5_13_2==3| p5_13_3==3| p5_13_4==3| p5_13_5==3| p5_13_6==3)

*** Define now the values *** 
* 1. REAL ASSETS
*To obtain the value of the main residence we use the variable p2_5

*To obtain the value of the other real estate properties we generate a new variable
gen otraspr=0
replace otraspr=otraspr+p2_39_1*(p2_37_1/100) if (p2_33>=1 & p2_33~=. & p2_39_1>=0 & p2_39_1~=. & p2_37_1>0 & p2_37_1~=.)
replace otraspr=otraspr+p2_39_2 *(p2_37_2/100) if (p2_33>=2 & p2_33~=. & p2_39_2>=0 & p2_39_2~=. & p2_37_2>0 & p2_37_2~=.)
replace otraspr=otraspr+p2_39_3* (p2_37_3/100) if (p2_33>=3 & p2_33~=. & p2_39_3>=0 & p2_39_3~=. & p2_37_3>0 & p2_37_3~=.)
replace otraspr=otraspr+p2_39_4 if (p2_33>3 & p2_33~=. & p2_39_4>=0 & p2_39_4~=.) 

*To obtain the value of the jewellery, works of art and antiques we use the variable p2_84


* 2. FINANCIAL ASSETS
*To obtain the balance of the accounts and deposits usable for payments we use the variable p4_7_3

*To obtain the value of the listed shares we use the variable p4_15

*To obtain the value of the unlisted shares and other equity we use the variable p4_24

*To obtain the value of the fixed-income securities we use the variable p4_35

/* To obtain the total value of mutual funds we use the variable allf calculated as (i) the addition of the values of each mutual fund that the household owns (p4_31_i; i=1,…,10) 
if the number of these funds is 10 or less, and (ii) the household mutual funds’ total value if this one owns more than 10 (p4_28a) */ 
egen allf=rowtotal(p4_31_1 p4_31_2 p4_31_3 p4_31_4 p4_31_5 p4_31_6 p4_31_7 p4_31_8 p4_31_9 p4_31_10) 
replace allf=p4_28a if p4_28>10 


*To obtain the balance of the accounts and deposits not usable for payments we generate a new variable
gen salcuentas=0
replace salcuentas = salcuentas +p4_7_1 if (p4_3==1 & p4_7_1>=0 & p4_7_1~=. )
replace salcuentas = salcuentas + p4_7_2 if (p4_4==1 & p4_7_2>=0 & p4_7_2~=.)

*To obtain the current value of the pension schemes we generate a new variable
gen valor=0
replace valor = valor +p5_7_1 if (p5_1==1 & p5_7_1>=0 & p5_7_1~=. )
replace valor = valor + p5_7_2 if (p5_1==1 & p5_7_2>=0 & p5_7_2~=.)
replace valor = valor + p5_7_3 if (p5_1==1 & p5_7_3>=0 & p5_7_3~=.)
replace valor = valor + p5_7_4 if (p5_1==1 & p5_7_4>=0 & p5_7_4~=.)
replace valor = valor + p5_7_5 if (p5_1==1 & p5_7_5>=0 & p5_7_5~=.)
replace valor = valor + p5_7_6 if (p5_1==1 & p5_7_6>=0 & p5_7_6~=.)
replace valor = valor + p5_7_7 if (p5_1==1 & p5_7_7>=0 & p5_7_7~=.)
replace valor = valor + p5_7_8 if (p5_1==1 & p5_7_8>=0 & p5_7_8~=.)

*To obtain the value of the unit-linked or mixed life insurance we generate a new variable
gen valseg=0
replace valseg = valseg +p5_14_1 if ((p5_13_1==2| p5_13_1==3) & p5_14_1>0 & p5_14_1~=.)
replace valseg = valseg +p5_14_2 if ((p5_13_2==2| p5_13_2==3) & p5_14_2>0 & p5_14_2~=.)
replace valseg = valseg +p5_14_3 if ((p5_13_3==2| p5_13_3==3) & p5_14_3>0 & p5_14_3~=.)
replace valseg = valseg +p5_14_4 if ((p5_13_4==2| p5_13_4==3) & p5_14_4>0 & p5_14_4~=.)
replace valseg = valseg +p5_14_5 if ((p5_13_5==2| p5_13_5==3) & p5_14_5>0 & p5_14_5~=.)
replace valseg = valseg +p5_14_6 if ((p5_13_6==2| p5_13_6==3) & p5_14_6>0 & p5_14_6~=.)

* 3. DEBTS 
*To obtain the value of the outstanding debts from loans used to purchase their main residence, we generate a new variable
gen dvivpral=0
replace dvivpral= dvivpral +p2_12_1 if  (p2_8a>=1 & p2_8a~=. & p2_12_1>0 & p2_12_1~=.)
replace dvivpral= dvivpral + p2_12_2 if (p2_8a>=2 & p2_8a~=. &  p2_12_2>0 & p2_12_2~=.)
replace dvivpral= dvivpral +p2_12_3  if (p2_8a>=3 & p2_8a~=. & p2_12_3>0 & p2_12_3~=.)
replace dvivpral= dvivpral +p2_12_4  if (p2_8a>3 & p2_8a~=. & p2_12_4>0 & p2_12_4~=.)

*To obtain the value of the outstanding debts from loans used to purchase other real estate properties different from the main residence, we generate four new variables
*1st real estate property;
gen dprop1=0
replace dprop1= dprop1+p2_55_1_1 if (p2_51_1>=1 & p2_51_1~=.  &  p2_55_1_1>0 & p2_55_1_1~=.)
replace dprop1= dprop1+ p2_55_1_2 if (p2_51_1>=2 & p2_51_1~=.  & p2_55_1_2>0 & p2_55_1_2~=.)
replace dprop1= dprop1+p2_55_1_3  if (p2_51_1>=3 & p2_51_1~=.  & p2_55_1_3>0 & p2_55_1_3~=.)

*2nd real estate property 
gen dprop2=0
replace dprop2= dprop2+p2_55_2_1 if (p2_51_2>=1 & p2_51_2~=. & p2_55_2_1>0 & p2_55_2_1~=.)
replace dprop2= dprop2+ p2_55_2_2 if (p2_51_2>=2 & p2_51_2~=. & p2_55_2_2>0 & p2_55_2_2~=.)
replace dprop2= dprop2+ p2_55_2_3  if (p2_51_2>=3 & p2_51_2~=. & p2_55_2_3>0 & p2_55_2_3~=.)

*3rd real estate property
gen dprop3=0
replace dprop3= dprop3+p2_55_3_1 if (p2_51_3>=1 & p2_51_3~=. & p2_55_3_1>0 & p2_55_3_1~=.)
replace dprop3= dprop3+ p2_55_3_2 if (p2_51_3>=2 & p2_51_3~=. & p2_55_3_2>0 & p2_55_3_2~=.)
replace dprop3= dprop3+ p2_55_3_3  if (p2_51_3>=3 & p2_51_3~=. & p2_55_3_3>0 & p2_55_3_3~=.)

*For the rest of real estate properties more than 3 
gen dprop4=0
replace dprop4= dprop4+p2_55_4 if (p2_55_4>0 & p2_55_4~=.)

*Considering all real estate but main residence 
gen deuoprop= dprop1+ dprop2+ dprop3+ dprop4


*To obtain the value of the outstanding debts from loans with mortgage guarantee used for the purchase of the main residence, we generate a new variable
gen deuhipv =0
replace deuhipv= deuhipv + p2_12_1 if (p2_8a>=1 & p2_8a~=. & p2_9_1==1 & p2_12_1>0 & p2_12_1~=.)
replace deuhipv = deuhipv + p2_12_2 if (p2_8a>=2 & p2_8a~=. & p2_9_2==1 &  p2_12_2>0 & p2_12_2~=.)
replace deuhipv= deuhipv +p2_12_3  if (p2_8a>=3 & p2_8a~=. & p2_9_3==1 &  p2_12_3>0 & p2_12_3~=.)
replace deuhipv= deuhipv +p2_12_4  if (p2_8a>3 & p2_8a~=. & p2_8a~=. & p2_9_4==1 &  p2_12_4>0 & p2_12_4~=.)

*To obtain the value of the outstanding debts from mortgages and other loans with real guarantee we generate a new variable;
gen phipo=0
replace phipo = phipo +p3_6_1 if ((p3_2_1==1|p3_2_1==2) & p3_6_1>0 & p3_6_1~=.)
replace phipo = phipo +p3_6_2 if ((p3_2_2==1| p3_2_2==2) & p3_6_2>0 & p3_6_2~=.)
replace phipo = phipo +p3_6_3 if ((p3_2_3==1| p3_2_3==2)  & p3_6_3>0 & p3_6_3~=.)
replace phipo = phipo +p3_6_4 if ((p3_2_4==1| p3_2_4==2) & p3_6_4>0 & p3_6_4~=.)

*To obtain the value of the outstanding debts from personal loans we generate a new variable;
gen pperso=0
replace pperso = pperso +p3_6_1 if (p3_2_1==3 & p3_6_1>0 & p3_6_1~=.)
replace pperso = pperso +p3_6_2 if (p3_2_2==3 & p3_6_2>0 & p3_6_2~=.)
replace pperso = pperso +p3_6_3 if (p3_2_3==3 & p3_6_3>0 & p3_6_3~=.)
replace pperso = pperso +p3_6_4 if (p3_2_4==3 & p3_6_4>0 & p3_6_4~=.)

*To obtain the value of the other outstanding debts we generate a new variable;
gen potrasd =0
replace potrasd = potrasd +p3_6_1 if ((p3_2_1==4| p3_2_1==5| p3_2_1==6| p3_2_1==7| p3_2_1==8|p3_2_1==9|p3_2_1==97) & p3_6_1>0 & p3_6_1~=.)
replace potrasd = potrasd +p3_6_2 if ((p3_2_2==4| p3_2_2==5|  p3_2_2==6| p3_2_2==7| p3_2_2==8|p3_2_2==9|p3_2_2==97) & p3_6_2>0 & p3_6_2~=.)
replace potrasd = potrasd +p3_6_3 if ((p3_2_3==4| p3_2_3==5| p3_2_3==6| p3_2_3==7| p3_2_3==8|p3_2_3==9|p3_2_3==97) & p3_6_3>0 & p3_6_3~=.)
replace potrasd = potrasd +p3_6_4 if ((p3_2_4==4| p3_2_4==5| p3_2_4==6| p3_2_4==7| p3_2_4==8|p3_2_4==9|p3_2_4==97) & p3_6_4>0 & p3_6_4~=.)


*To obtain the total value of the all outstanding debt we generate a new variable
gen vdeuda= dvivpral + deuoprop+ phipo+ pperso+ potrasd


save "$output/2002/wealth_`c'", replace 


** INCOME FLOWS **
use h_number p6_14_* p6_16_* p6_28a_* p6_28b_* p6_28c_* p6_28d_* p6_28f_* p6_37_* p6_39* p6_40_* p6_43_* p6_44_* p6_381a_* p6_381b_* p6_47_* p6_48a_* p6_49_* p6_50_* /// 
p6_382_* p6_3821_* p6_3822_* p6_3824_* p6_383_* p6_3831_* p6_3832_* p6_39_* p6_391_* p6_392* p6_52 p6_54 p6_56 p6_58 p6_60 p6_60c p6_60d p6_60e* p6_60f using "$path/2002/`c'/section6_2002_`c'.dta", clear 

drop p6_60es2 p6_60es3 p6_60es4 

* Take the hh size 
merge 1:m h_number using temp1_`c'.dta, keepus(p1) nogen 

*Adjust the p6_392 
forvalues i=1/9 { 
	forvalues j=1/3 { 
		gen p6_392_`i'_`j'=1 if p6_392c1_`i'_`j'==1 
		replace p6_392_`i'_`j'=2 if p6_392c2_`i'_`j'==1 
		replace p6_392_`i'_`j'=3 if p6_392c3_`i'_`j'==1 
		replace p6_392_`i'_`j'=4 if p6_392c4_`i'_`j'==1 
		replace p6_392_`i'_`j'=5 if p6_392c5_`i'_`j'==1 
		replace p6_392_`i'_`j'=6 if p6_392c6_`i'_`j'==1 
		
		label define p6_392_`i'_`j' 1 "Main residence" 2 "1st property" 3 "2nd property" 4 "3rd property" 5 "4th property" 6 "Other properties", modify 
		label values p6_392_`i'_`j' p6_392_`i'_`j'
	} 
}


* Change the orders for the variables that has the hh_member in the wrong place * 
ds p6_14_* p6_16_* p6_37_* p6_39_* p6_40_* p6_43_* p6_44_* p6_49_* p6_50_* p6_391_* p6_392_* p6_381a_* p6_381b_* p6_382_* p6_3821_* p6_3824_* p6_383_* p6_3831_* 
rename (*_*_*_*) (*_*_*[4]_*[3]) 


* Reshape the selected variables from wide to long * 
ds p6_52 p6_54 p6_56 p6_58 p6_60* h_number p1, not 
local lisvr `r(varlist)'
disp "`lisvr'"
foreach h of local lisvr { 
	local stab = substr("`h'", 1, strrpos("`h'", "_"))
    local stabs `stabs' `stab'
} 
local stabs : list uniq stabs
disp "`stabs'" 
reshape long `stabs', i(h_number) j(ind_id)
bysort h_number : drop if _n>p1 
rensfix _

* Create the sums for labour incomes across different existing jobs * 
egen tot_employee=rowtotal(p6_14_1 p6_14_2 p6_14_3 p6_16_1 p6_16_2 p6_16_3) 
/* We need to ask how to aggregate the self-employed income */ 
egen tot_pension=rowtotal(p6_49_1 p6_49_2 p6_49_3 p6_49_4) 


save "$output/2002/income_`c'", replace 

use "$output/2002/sdem_`c'", clear 
merge 1:1 h_number ind_id using "$output/2002/lab_status_`c'", nogen 
merge 1:1 h_number ind_id using "$output/2002/income_`c'", nogen 
merge m:1 h_number using "$output/2002/wealth_`c'", nogen 


* Now, adjust the business value, how much is owed to the hh and create the total gross and net wealth 

*To obtain how much is owed to the household, we use the variables p6_44 and p4_38 and generate a new variable
gen odeuhog=0
replace odeuhog = odeuhog + p4_38 if (p4_38>0 & p4_38~=.) 
replace odeuhog = odeuhog + p6_44_1 if (p6_44_1>0 & p6_44_1~=.)
replace odeuhog = odeuhog + p6_44_2 if (p6_44_2>0 & p6_44_2~=.)
replace odeuhog = odeuhog + p6_44_3 if (p6_44_3>0 & p6_44_3~=.)


*Adjust the business value to net out the value of real estates already mentioned 
forvalues j=1/3 { 
	gen new_p6_39_`j'= p6_39_`j'
	replace new_p6_39_`j' = p6_39_`j' - p2_5 if p6_391_`j'==1 & p6_392_`j'==1 
	replace new_p6_39_`j' = p6_39_`j' - (p2_39_1*(p2_37_1/100)) if p6_391_`j'==1 & p6_392_`j'==2
	replace new_p6_39_`j' = p6_39_`j' - (p2_39_2*(p2_37_2/100)) if p6_391_`j'==1 & p6_392_`j'==3
	replace new_p6_39_`j' = p6_39_`j' - (p2_39_3*(p2_37_3/100)) if p6_391_`j'==1 & p6_392_`j'==4
	replace new_p6_39_`j' = p6_39_`j' - p2_39_4 if p6_391_`j'==1 & p6_392_`j'==5
	*We take the original p6_39 value if netting out the property value we get a negative market value 
	replace new_p6_39_`j' = p6_39_`j' if new_p6_39_`j'<0
	
	* Sum the p6_40_j with the new p6_39 
	gen valhog_`j'=p6_40_`j' + new_p6_39_`j'
} 

* Take the median of the valog_j 
egen valhog_tot= rowtotal(valhog_1 valhog_2 valhog_3) 
replace valhog_tot=. if valhog_tot==0 
bysort h_number (ind_id) : egen median_valhog=median(valhog_tot) 
replace median_valhog=0 if median_valhog==. 

** Total wealth ** 
* Real assets 
gen actreales=0
replace actreales=actreales+p2_5 if (p2_5>0 & p2_5~=.)
replace actreales=actreales+otraspr
replace actreales=actreales+p2_84 if (p2_84>0 & p2_84~=.)
replace actreales=actreales+median_valhog

* Financial assets 
gen actfinanc=0
replace actfinanc=actfinanc+p4_7_3 if (p4_7_3>0 & p4_7_3~=.)
replace actfinanc=actfinanc+p4_15 if (p4_15>0 & p4_15~=.)
replace actfinanc=actfinanc+p4_24 if (p4_24>0 & p4_24~=.)
replace actfinanc=actfinanc+p4_35 if (p4_35>0 & p4_35~=.)
replace actfinanc=actfinanc+allf if (allf>0 & allf~=.)
replace actfinanc=actfinanc+salcuentas
replace actfinanc=actfinanc+valor
replace actfinanc=actfinanc+valseg
replace actfinanc=actfinanc+odeuhog

* Gross wealth
gen riquezabr=0
replace riquezabr=riquezabr+actreales+actfinanc

*Net wealth=gross wealth - debts 
gen riquezanet=riquezabr-vdeuda

* Rename the household number 
rename h_number h_id 

rensfix _
gen anno=2002 

save "$append/`c'/final_2002_`c'.dta", replace 

}	



**********************************************************************************************************************************

*** 2005 **** 
foreach c in imp1 imp2 imp3 imp4 imp5 { 
	
/* The socio-demographic variables need a reshape from wide to long in order to have individual rows within hh, as the 
questions are asked to each single individual in the hh. However, the wealth information are at the hh level! The different _x 
are now at the asset perspective, not individual one. Therefore, the wealth variables can be simply taken as given, and then
take the total as the rowsum of the asset */ 

use h_* facine3 pesopan_* hogar* pan_* p1 p1_1_* p1_2b_* p1_3_* p1_4_* p1_5_* p1_52_* p1_13 p1_14_* renthog mrenthog using "$path/2005/`c'/other_sections_2005_`c'.dta", clear 
save temp1_`c'.dta, replace 

* Reshape wide to long 
reshape long pan_@ p1_1_@ p1_2b_@ p1_3_@ p1_4_@ p1_5_@ p1_52_@, i(h_2005) j(ind_id) 

* Drop the additional rows, as not all hh have 9 members 
bysort h_2005 : drop if _n>p1 

* Save the socio-demographic 
save "$output/2005/sdem_`c'", replace 

** LABOUR MARKET VARIABLES ** 
use h_* p6_1c* p6_3_* p6_4_* p6_8_* p6_9_* p6_10_* p6_11_* p6_12_* p6_13_* p6_17_* p6_20_* p6_27* p6_29_* p6_30_* p6_31* p6_33* p6_34_* p6_35_* p6_36_* /// 
p6_37_* p6_77_* p6_78_* p6_79_* using "$path/2005/`c'/section6_2005_`c'.dta", clear 


* Take the hh size 
merge 1:m h_2005 using temp1_`c'.dta, keepus(p1) nogen 

* Adjust the labur market status and sources of income 
forvalues i=1/9 { 
	gen p6_1_`i'=1 if p6_1c1_`i'==1 
	replace p6_1_`i'=2 if p6_1c2_`i'==1 
	replace p6_1_`i'=3 if p6_1c3_`i'==1 
	replace p6_1_`i'=4 if p6_1c4_`i'==1 
	replace p6_1_`i'=5 if p6_1c5_`i'==1 
	replace p6_1_`i'=6 if p6_1c6_`i'==1 
	replace p6_1_`i'=7 if p6_1c7_`i'==1 
	replace p6_1_`i'=8 if p6_1c8_`i'==1 
	
	
	label define p6_1_`i' 1 "Employee" 2 "Self-employed" 3 "Unemployed" 4 "Retired" 5 "Disabled" 6 "Student" 7 "Housekeeper" 8 "Other inactives"
	label values p6_1_`i' p6_1_`i' 
	
	/* Have a check on whether is necessary some adjustments! */ 

} 


* Change the orders for the variables that has the hh_member in the wrong place * 
ds p6_10_* p6_11_* p6_12_* p6_13_* p6_17_* p6_20_* p6_33_* p6_34_* p6_36_* p6_37_* 
rename (*_*_*_*) (*_*_*[4]_*[3]) 

* Reshape all the variables from wide to long * 
/* reshape long p6_1_ p6_3_ p6_4_ p6_8_ p6_9_ p6_11_1_ p6_11_2_ p6_11_3_ p6_12_1_ p6_12_2_ p6_12_3_ p6_13_1_ p6_13_2_ p6_13_3_ ///
p6_17_1_ p6_17_2_ p6_17_3_ p6_20_1_ p6_20_2_ p6_20_3_ p6_33_1_ p6_33_2_ p6_33_3_ p6_34_1_ p6_34_2_ p6_34_3_ p6_36_1_ ///
p6_36_2_ p6_36_3_ p6_37_1_ p6_37_2_ p6_37_3_ p6_27_ p6_29_ p6_30_ p6_31_ p6_77_ p6_78_ p6_79_ , i(h_number) j(ind_id) 
*/ 

foreach v of var p* {
    local stub = substr("`v'", 1, strrpos("`v'", "_"))
    local stubs `stubs' `stub'
}

local stubs : list uniq stubs
reshape long `stubs', i(h_2005) j(ind_id)

bysort h_2005 : drop if _n>p1 

* Reshape the variables for the job numer * 
rensfix _
drop p6_1c*  

/* In case we want to reshape the jobs as well: 
reshape long p6_11_ p6_12_ p6_13_ p6_17_ p6_20_ p6_33_ p6_34_ p6_36_ p6_37_ , i(h_number ind_id) j(job_num)
However, there are only 37 observations with more than 1 occupation. Therefore, I leave the corresponding variables separated. 

Furthermore, 
egen row_tot=rowtotal(p6_1c1 p6_1c2 p6_1c3 p6_1c4 p6_1c5 p6_1c6 p6_1c7 p6_1c8) 
there are 136 observations that have non-consistent dummy definition for the labour status. 
count if row_tot>1 --> 136. Thinking of dropping them for inconsistency 
*/ 


save "$output/2005/lab_status_`c'", replace 


** WEALTH VARIABLES AND INCOME FLOWS FROM WEALTH ** 
use h_* p2_1 p2_2 p2_3 p2_4 p2_5 p2_7 p2_8 p2_8a p2_22 p2_23 p2_31 p2_32 p2_33 p2_35a_* p2_37_* p2_38_* p2_39_* p2_42_* p2_42s* /// 
p2_43_* p2_39_4 p2_50_* p2_51_* p2_64 p2_72 p2_75 p2_76 p2_79 p2_81 p2_82 p2_84 p2_88 p2_9_* p2_11_* p2_12_* p2_16_* /// 
p2_17_* p2_18_* p2_54_* p2_55_* p2_59_* p2_60_* p2_61_* p2_55_4 p2_61_4 p3_1 p3_2_* p3_5_* p3_6_* p3_9_* p3_10_* ///
p3_11_* p3_19 p3_20 p4_1 p4_2 p4_3 p4_5 p4_7_* p4_8* p4_10 p4_12 p4_15 p4_17 p4_18 p4_21 p4_24 p4_27 p4_28 p4_28a p4_31_* ///
p4_33 p4_35 p4_38 p4_39 p5_1 p5_6_* p5_6a_* p5_7_* p5_9 p5_10 p5_13_* p5_14_* p5_16_* p5_17a_* p5_17b_* p4_40 p4_16 p2_24 p4_25 p4_36 using "$path/2005/`c'/other_sections_2005_`c'.dta", clear 


* Accounts and deposits usable for payments 
gen np4_5=(p4_5==1 & p4_7_3>0 & p4_7_3~=.)

* Unlisted shares 
gen np4_18=(p4_18==1 & p4_24>0 & p4_24~=.)

* House purchase savings account and other accounts not for saving
gen cuentas=(p4_3==1|p4_4==1)

* To calculate the percentage of households that own unit-linked or mixed life insurance we generate a new variable
gen seguro=(p5_13_1==2| p5_13_2==2| p5_13_3==2| p5_13_4==2| p5_13_5==2| p5_13_6==2| p5_13_1==3| p5_13_2==3| p5_13_3==3| p5_13_4==3| p5_13_5==3| p5_13_6==3)

*** Define now the values *** 
* 1. REAL ASSETS
*To obtain the value of the main residence we use the variable p2_5

*To obtain the value of the other real estate properties we generate a new variable
gen otraspr=0
replace otraspr=otraspr+p2_39_1*(p2_37_1/100) if (p2_33>=1 & p2_33~=. & p2_39_1>=0 & p2_39_1~=. & p2_37_1>0 & p2_37_1~=.)
replace otraspr=otraspr+p2_39_2 *(p2_37_2/100) if (p2_33>=2 & p2_33~=. & p2_39_2>=0 & p2_39_2~=. & p2_37_2>0 & p2_37_2~=.)
replace otraspr=otraspr+p2_39_3* (p2_37_3/100) if (p2_33>=3 & p2_33~=. & p2_39_3>=0 & p2_39_3~=. & p2_37_3>0 & p2_37_3~=.)
replace otraspr=otraspr+p2_39_4 if (p2_33>3 & p2_33~=. & p2_39_4>=0 & p2_39_4~=.) 

*To obtain the value of the jewellery, works of art and antiques we use the variable p2_84


* 2. FINANCIAL ASSETS
*To obtain the balance of the accounts and deposits usable for payments we use the variable p4_7_3

*To obtain the value of the listed shares we use the variable p4_15

*To obtain the value of the unlisted shares and other equity we use the variable p4_24

*To obtain the value of the fixed-income securities we use the variable p4_35

/* To obtain the total value of mutual funds we use the variable allf calculated as (i) the addition of the values of each mutual fund that the household owns (p4_31_i; i=1,…,10) 
if the number of these funds is 10 or less, and (ii) the household mutual funds’ total value if this one owns more than 10 (p4_28a) */ 
egen allf=rowtotal(p4_31_1 p4_31_2 p4_31_3 p4_31_4 p4_31_5 p4_31_6 p4_31_7 p4_31_8 p4_31_9 p4_31_10) 
replace allf=p4_28a if p4_28>10 


*To obtain the balance of the accounts and deposits not usable for payments we generate a new variable
gen salcuentas=0
replace salcuentas = salcuentas +p4_7_1 if (p4_3==1 & p4_7_1>=0 & p4_7_1~=. )
replace salcuentas = salcuentas + p4_7_2 if (p4_4==1 & p4_7_2>=0 & p4_7_2~=.)

*To obtain the current value of the pension schemes we generate a new variable
gen valor=0
replace valor = valor +p5_7_1 if (p5_1==1 & p5_7_1>=0 & p5_7_1~=. )
replace valor = valor + p5_7_2 if (p5_1==1 & p5_7_2>=0 & p5_7_2~=.)
replace valor = valor + p5_7_3 if (p5_1==1 & p5_7_3>=0 & p5_7_3~=.)
replace valor = valor + p5_7_4 if (p5_1==1 & p5_7_4>=0 & p5_7_4~=.)
replace valor = valor + p5_7_5 if (p5_1==1 & p5_7_5>=0 & p5_7_5~=.)
replace valor = valor + p5_7_6 if (p5_1==1 & p5_7_6>=0 & p5_7_6~=.)
replace valor = valor + p5_7_7 if (p5_1==1 & p5_7_7>=0 & p5_7_7~=.)
replace valor = valor + p5_7_8 if (p5_1==1 & p5_7_8>=0 & p5_7_8~=.)

*To obtain the value of the unit-linked or mixed life insurance we generate a new variable
gen valseg=0
replace valseg = valseg +p5_14_1 if ((p5_13_1==2| p5_13_1==3) & p5_14_1>0 & p5_14_1~=.)
replace valseg = valseg +p5_14_2 if ((p5_13_2==2| p5_13_2==3) & p5_14_2>0 & p5_14_2~=.)
replace valseg = valseg +p5_14_3 if ((p5_13_3==2| p5_13_3==3) & p5_14_3>0 & p5_14_3~=.)
replace valseg = valseg +p5_14_4 if ((p5_13_4==2| p5_13_4==3) & p5_14_4>0 & p5_14_4~=.)
replace valseg = valseg +p5_14_5 if ((p5_13_5==2| p5_13_5==3) & p5_14_5>0 & p5_14_5~=.)
replace valseg = valseg +p5_14_6 if ((p5_13_6==2| p5_13_6==3) & p5_14_6>0 & p5_14_6~=.)

* 3. DEBTS 
*To obtain the value of the outstanding debts from loans used to purchase their main residence, we generate a new variable
gen dvivpral=0
replace dvivpral= dvivpral +p2_12_1 if  (p2_8a>=1 & p2_8a~=. & p2_12_1>0 & p2_12_1~=.)
replace dvivpral= dvivpral + p2_12_2 if (p2_8a>=2 & p2_8a~=. &  p2_12_2>0 & p2_12_2~=.)
replace dvivpral= dvivpral +p2_12_3  if (p2_8a>=3 & p2_8a~=. & p2_12_3>0 & p2_12_3~=.)
replace dvivpral= dvivpral +p2_12_4  if (p2_8a>3 & p2_8a~=. & p2_12_4>0 & p2_12_4~=.)

*To obtain the value of the outstanding debts from loans used to purchase other real estate properties different from the main residence, we generate four new variables
*1st real estate property;
gen dprop1=0
replace dprop1= dprop1+p2_55_1_1 if (p2_51_1>=1 & p2_51_1~=.  &  p2_55_1_1>0 & p2_55_1_1~=.)
replace dprop1= dprop1+ p2_55_1_2 if (p2_51_1>=2 & p2_51_1~=.  & p2_55_1_2>0 & p2_55_1_2~=.)
replace dprop1= dprop1+p2_55_1_3  if (p2_51_1>=3 & p2_51_1~=.  & p2_55_1_3>0 & p2_55_1_3~=.)

*2nd real estate property 
gen dprop2=0
replace dprop2= dprop2+p2_55_2_1 if (p2_51_2>=1 & p2_51_2~=. & p2_55_2_1>0 & p2_55_2_1~=.)
replace dprop2= dprop2+ p2_55_2_2 if (p2_51_2>=2 & p2_51_2~=. & p2_55_2_2>0 & p2_55_2_2~=.)
replace dprop2= dprop2+ p2_55_2_3  if (p2_51_2>=3 & p2_51_2~=. & p2_55_2_3>0 & p2_55_2_3~=.)

*3rd real estate property
gen dprop3=0
replace dprop3= dprop3+p2_55_3_1 if (p2_51_3>=1 & p2_51_3~=. & p2_55_3_1>0 & p2_55_3_1~=.)
replace dprop3= dprop3+ p2_55_3_2 if (p2_51_3>=2 & p2_51_3~=. & p2_55_3_2>0 & p2_55_3_2~=.)
replace dprop3= dprop3+ p2_55_3_3  if (p2_51_3>=3 & p2_51_3~=. & p2_55_3_3>0 & p2_55_3_3~=.)

*For the rest of real estate properties more than 3 
gen dprop4=0
replace dprop4= dprop4+p2_55_4 if (p2_55_4>0 & p2_55_4~=.)

*Considering all real estate but main residence 
gen deuoprop= dprop1+ dprop2+ dprop3+ dprop4


*To obtain the value of the outstanding debts from loans with mortgage guarantee used for the purchase of the main residence, we generate a new variable
gen deuhipv =0
replace deuhipv= deuhipv + p2_12_1 if (p2_8a>=1 & p2_8a~=. & p2_9_1==1 & p2_12_1>0 & p2_12_1~=.)
replace deuhipv = deuhipv + p2_12_2 if (p2_8a>=2 & p2_8a~=. & p2_9_2==1 &  p2_12_2>0 & p2_12_2~=.)
replace deuhipv= deuhipv +p2_12_3  if (p2_8a>=3 & p2_8a~=. & p2_9_3==1 &  p2_12_3>0 & p2_12_3~=.)
replace deuhipv= deuhipv +p2_12_4  if (p2_8a>3 & p2_8a~=. & p2_8a~=. & p2_9_4==1 &  p2_12_4>0 & p2_12_4~=.)

*To obtain the value of the outstanding debts from mortgages and other loans with real guarantee we generate a new variable;
gen phipo=0
replace phipo = phipo +p3_6_1 if ((p3_2_1==1|p3_2_1==2) & p3_6_1>0 & p3_6_1~=.)
replace phipo = phipo +p3_6_2 if ((p3_2_2==1| p3_2_2==2) & p3_6_2>0 & p3_6_2~=.)
replace phipo = phipo +p3_6_3 if ((p3_2_3==1| p3_2_3==2)  & p3_6_3>0 & p3_6_3~=.)
replace phipo = phipo +p3_6_4 if ((p3_2_4==1| p3_2_4==2) & p3_6_4>0 & p3_6_4~=.)
replace phipo = phipo +p3_6_5 if ((p3_2_5==1| p3_2_5==2) & p3_6_5>0 & p3_6_5~=.)
replace phipo = phipo +p3_6_6 if ((p3_2_6==1| p3_2_6==2) & p3_6_6>0 & p3_6_6~=.)
replace phipo = phipo +p3_6_7 if ((p3_2_7==1| p3_2_7==2) & p3_6_7>0 & p3_6_7~=.)
replace phipo = phipo +p3_6_8 if ((p3_2_8==1| p3_2_8==2) & p3_6_8>0 & p3_6_8~=.)

*To obtain the value of the outstanding debts from personal loans we generate a new variable;
gen pperso=0
replace pperso = pperso +p3_6_1 if (p3_2_1==3 & p3_6_1>0 & p3_6_1~=.)
replace pperso = pperso +p3_6_2 if (p3_2_2==3 & p3_6_2>0 & p3_6_2~=.)
replace pperso = pperso +p3_6_3 if (p3_2_3==3 & p3_6_3>0 & p3_6_3~=.)
replace pperso = pperso +p3_6_4 if (p3_2_4==3 & p3_6_4>0 & p3_6_4~=.)

*To obtain the value of the other outstanding debts we generate a new variable;
gen potrasd =0
replace potrasd = potrasd +p3_6_1 if ((p3_2_1==4| p3_2_1==5| p3_2_1==6| p3_2_1==7| p3_2_1==8|p3_2_1==9|p3_2_1==97) & p3_6_1>0 & p3_6_1~=.)
replace potrasd = potrasd +p3_6_2 if ((p3_2_2==4| p3_2_2==5|  p3_2_2==6| p3_2_2==7| p3_2_2==8|p3_2_2==9|p3_2_2==97) & p3_6_2>0 & p3_6_2~=.)
replace potrasd = potrasd +p3_6_3 if ((p3_2_3==4| p3_2_3==5| p3_2_3==6| p3_2_3==7| p3_2_3==8|p3_2_3==9|p3_2_3==97) & p3_6_3>0 & p3_6_3~=.)
replace potrasd = potrasd +p3_6_4 if ((p3_2_4==4| p3_2_4==5| p3_2_4==6| p3_2_4==7| p3_2_4==8|p3_2_4==9|p3_2_4==97) & p3_6_4>0 & p3_6_4~=.)


*To obtain the total value of the all outstanding debt we generate a new variable
gen vdeuda= dvivpral + deuoprop+ phipo+ pperso+ potrasd

save "$output/2005/wealth_`c'", replace 

** INCOME FLOWS **
use h_* p6_14_* p6_16_* p6_28a_* p6_28b_* p6_28c_* p6_28d_* p6_28f_* p6_37_* p6_39* p6_40_* p6_43_* p6_44_* p6_381a_* p6_381b_* p6_47_* p6_48a_* p6_49_* p6_50_* p6_382_* p6_3821_* ///
p6_3822_* p6_3824_* p6_383_* p6_3831_* p6_3832_* p6_39_* p6_391_* p6_392* p6_52 p6_54 p6_56 p6_58 p6_60 p6_60c p6_60d p6_60e* p6_60f using "$path/2005/`c'/section6_2005_`c'.dta", clear 

drop p6_60es2 p6_60es3 p6_60es4 

* Take the hh size 
merge 1:m h_2005 using temp1_`c'.dta, keepus(p1) nogen 


*Adjust the p6_392 
forvalues i=1/9 { 
	forvalues j=1/3 { 
		gen p6_392_`i'_`j'=1 if p6_392c1_`i'_`j'==1 
		replace p6_392_`i'_`j'=2 if p6_392c2_`i'_`j'==1 
		replace p6_392_`i'_`j'=3 if p6_392c3_`i'_`j'==1 
		replace p6_392_`i'_`j'=4 if p6_392c4_`i'_`j'==1 
		replace p6_392_`i'_`j'=5 if p6_392c5_`i'_`j'==1 
		
		label define p6_392_`i'_`j' 1 "Main residence" 2 "1st property" 3 "2nd property" 4 "3rd property" 5 "Other properties", modify 
		label values p6_392_`i'_`j' p6_392_`i'_`j'
	} 
}


* Change the orders for the variables that has the hh_member in the wrong place * 
ds p6_14_* p6_16_* p6_37_* p6_39_* p6_40_* p6_43_* p6_44_* p6_49_* p6_50_* p6_391_* p6_392_* p6_381a_* p6_381b_* p6_382_* p6_3821_* p6_3824_* p6_383_* p6_3831_* 
rename (*_*_*_*) (*_*_*[4]_*[3]) 


* Reshape the selected variables from wide to long * 
ds p6_52 p6_54 p6_56 p6_58 p6_60* h_* p1, not 
local lisvr `r(varlist)'
disp "`lisvr'"
foreach h of local lisvr { 
	local stab = substr("`h'", 1, strrpos("`h'", "_"))
    local stabs `stabs' `stab'
} 
local stabs : list uniq stabs
disp "`stabs'" 
reshape long `stabs', i(h_2005) j(ind_id)
bysort h_2005 : drop if _n>p1 
rensfix _

* Create the sums for labour incomes across different existing jobs * 
egen tot_employee=rowtotal(p6_14_1 p6_14_2 p6_14_3 p6_16_1 p6_16_2 p6_16_3) 
/* We need to ask how to aggregate the self-employed income */ 
egen tot_pension=rowtotal(p6_49_1 p6_49_2 p6_49_3 p6_49_4) 

save "$output/2005/income_`c'", replace 

use "$output/2005/sdem_`c'", clear 
merge 1:1 h_2005 ind_id using "$output/2005/lab_status_`c'", nogen 
merge 1:1 h_2005 ind_id using "$output/2005/income_`c'", nogen 
merge m:1 h_2005 using "$output/2005/wealth_`c'", nogen 


* Now, adjust the business value, how much is owed to the hh and create the total gross and net wealth 

*To obtain how much is owed to the household, we use the variables p6_44 and p4_38 and generate a new variable
gen odeuhog=0
replace odeuhog = odeuhog + p4_38 if (p4_38>0 & p4_38~=.) 
replace odeuhog = odeuhog + p6_44_1 if (p6_44_1>0 & p6_44_1~=.)
replace odeuhog = odeuhog + p6_44_2 if (p6_44_2>0 & p6_44_2~=.)
replace odeuhog = odeuhog + p6_44_3 if (p6_44_3>0 & p6_44_3~=.)


*Adjust the business value to net out the value of real estates already mentioned 
forvalues j=1/3 { 
	gen new_p6_39_`j'= p6_39_`j'
	replace new_p6_39_`j' = p6_39_`j' - p2_5 if p6_391_`j'==1 & p6_392_`j'==1 
	replace new_p6_39_`j' = p6_39_`j' - (p2_39_1*(p2_37_1/100)) if p6_391_`j'==1 & p6_392_`j'==2
	replace new_p6_39_`j' = p6_39_`j' - (p2_39_2*(p2_37_2/100)) if p6_391_`j'==1 & p6_392_`j'==3
	replace new_p6_39_`j' = p6_39_`j' - (p2_39_3*(p2_37_3/100)) if p6_391_`j'==1 & p6_392_`j'==4
	replace new_p6_39_`j' = p6_39_`j' - p2_39_4 if p6_391_`j'==1 & p6_392_`j'==5
	*We take the original p6_39 value if netting out the property value we get a negative market value 
	replace new_p6_39_`j' = p6_39_`j' if new_p6_39_`j'<0
	
	* Sum the p6_40_j with the new p6_39 
	gen valhog_`j'=p6_40_`j' + new_p6_39_`j'
} 

* Take the median of the valog_j 
egen valhog_tot= rowtotal(valhog_1 valhog_2 valhog_3) 
replace valhog_tot=. if valhog_tot==0 
bysort h_number (ind_id) : egen median_valhog=median(valhog_tot) 
replace median_valhog=0 if median_valhog==. 

** Total wealth ** 
* Real assets 
gen actreales=0
replace actreales=actreales+p2_5 if (p2_5>0 & p2_5~=.)
replace actreales=actreales+otraspr
replace actreales=actreales+p2_84 if (p2_84>0 & p2_84~=.)
replace actreales=actreales+median_valhog

* Financial assets 
gen actfinanc=0
replace actfinanc=actfinanc+p4_7_3 if (p4_7_3>0 & p4_7_3~=.)
replace actfinanc=actfinanc+p4_15 if (p4_15>0 & p4_15~=.)
replace actfinanc=actfinanc+p4_24 if (p4_24>0 & p4_24~=.)
replace actfinanc=actfinanc+p4_35 if (p4_35>0 & p4_35~=.)
replace actfinanc=actfinanc+allf if (allf>0 & allf~=.)
replace actfinanc=actfinanc+salcuentas
replace actfinanc=actfinanc+valor
replace actfinanc=actfinanc+valseg
replace actfinanc=actfinanc+odeuhog

* Gross wealth
gen riquezabr=0
replace riquezabr=riquezabr+actreales+actfinanc

*Net wealth=gross wealth - debts 
gen riquezanet=riquezabr-vdeuda

* Rename the household number 
drop h_number 
rename h_2005 h_id 

rensfix _

gen anno=2005

save "$append/`c'/final_2005_`c'.dta", replace 

}	



**********************************************************************************************************************************

*** FOR THE 2008 WE STILL NEED TO KEEP IT SEPARATE *** 

foreach c in imp1 imp2 imp3 imp4 imp5 { 
/* The socio-demographic variables need a reshape from wide to long in order to have individual rows within hh, as the 
questions are asked to each single individual in the hh. However, the wealth information are at the hh level! The different _x 
are now at the asset perspective, not individual one. Therefore, the wealth variables can be simply taken as given, and then
take the total as the rowsum of the asset. However, since 2008 there are changes in the sections for self-employment, that 
are now in the section 4. So, we extract them now, without reshaping, but we need to generate the rowtotal!!  */ 

use h_* facine3  pesopan_* hogar* pan_* p1 p1_1_* p1_2b_* p1_3_* p1_4_* p1_5_* p1_52_* p1_13 p1_14* renthog mrenthog using "$path/2008/`c'/other_sections_2008_`c'.dta", clear 
save temp1_`c'.dta, replace 

* Reshape wide to long 
reshape long pan_@ p1_1_@ p1_2b_@ p1_3_@ p1_4_@ p1_5_@ p1_52_@, i(h_2008) j(ind_id) 

* Drop the additional rows, as not all hh have 9 members 
bysort h_2008 : drop if _n>p1 

* Save the socio-demographic 
save "$output/2008/sdem_`c'", replace 

*** LABOUR MARKET VARIABLES ** 
use h_* p6_1c* p6_3_* p6_4_* p6_8_* p6_9_* p6_10_* p6_11_* p6_12_* p6_13_* p6_17_* p6_20_* p6_27* p6_29_* p6_30_* p6_31* p6_33* p6_37_* /// 
p6_77_* p6_78_* p6_79_* p6_103_* using "$path/2008/`c'/section6_2008_`c'.dta", clear 


* Take the hh size 
merge 1:m h_2008 using temp1_`c'.dta, keepus(p1) nogen 

* Adjust the labur market status and sources of income 
forvalues i=1/9 { 
	gen p6_1_`i'=1 if p6_1c1_`i'==1 
	replace p6_1_`i'=2 if p6_1c2_`i'==1 
	replace p6_1_`i'=3 if p6_1c3_`i'==1 
	replace p6_1_`i'=4 if p6_1c4_`i'==1 
	replace p6_1_`i'=5 if p6_1c5_`i'==1 
	replace p6_1_`i'=6 if p6_1c6_`i'==1 
	replace p6_1_`i'=7 if p6_1c7_`i'==1 
	replace p6_1_`i'=8 if p6_1c8_`i'==1 
		
	label define p6_1_`i' 1 "Employee" 2 "Self-employed" 3 "Unemployed" 4 "Retired" 5 "Disabled" 6 "Student" 7 "Housekeeper" 8 "Other inactives"
	label values p6_1_`i' p6_1_`i' 

} 


* Change the orders for the variables that has the hh_member in the wrong place * 
ds p6_10_* p6_11_* p6_12_* p6_13_* p6_17_* p6_20_* p6_33_* p6_37_* p6_103_*
rename (*_*_*_*) (*_*_*[4]_*[3]) 


* Reshape all the variables from wide to long * 
/* reshape long p6_1_ p6_3_ p6_4_ p6_8_ p6_9_ p6_11_1_ p6_11_2_ p6_11_3_ p6_12_1_ p6_12_2_ p6_12_3_ p6_13_1_ p6_13_2_ p6_13_3_ ///
p6_17_1_ p6_17_2_ p6_17_3_ p6_20_1_ p6_20_2_ p6_20_3_ p6_33_1_ p6_33_2_ p6_33_3_ p6_34_1_ p6_34_2_ p6_34_3_ p6_36_1_ ///
p6_36_2_ p6_36_3_ p6_37_1_ p6_37_2_ p6_37_3_ p6_27_ p6_29_ p6_30_ p6_31_ p6_77_ p6_78_ p6_79_ , i(h_number) j(ind_id) 
*/ 

foreach v of var p* {
    local stub = substr("`v'", 1, strrpos("`v'", "_"))
    local stubs `stubs' `stub'
}

local stubs : list uniq stubs
reshape long `stubs', i(h_2008) j(ind_id)

bysort h_2008 : drop if _n>p1 

* Reshape the variables for the job numer * 
rensfix _
drop p6_1c* 

/* In case we want to reshape the jobs as well: 
reshape long p6_11_ p6_12_ p6_13_ p6_17_ p6_20_ p6_33_ p6_34_ p6_36_ p6_37_ , i(h_number ind_id) j(job_num)
However, there are only 37 observations with more than 1 occupation. Therefore, I leave the corresponding variables separated. 

Furthermore, 
egen row_tot=rowtotal(p6_1c1 p6_1c2 p6_1c3 p6_1c4 p6_1c5 p6_1c6 p6_1c7 p6_1c8) 
there are 136 observations that have non-consistent dummy definition for the labour status. 
count if row_tot>1 --> 136. Thinking of dropping them for inconsistency 
*/ 

save "$output/2008/lab_status_`c'", replace 

** WEALTH VARIABLES AND INCOME FLOWS FROM WEALTH ** 
use h_* p2_1 p2_1b p2_1c p2_2 p2_3 p2_4 p2_5 p2_7 p2_8 p2_8a p2_22 p2_23 p2_31 p2_32 p2_33 p2_35a_* p2_37_* p2_38_* p2_39_* p2_42_* p2_42s* /// 
p2_43_* p2_39_4 p2_50_* p2_51_* p2_64 p2_72 p2_75 p2_76 p2_79 p2_81 p2_82 p2_84 p2_88 p2_9_* p2_11_* p2_12_* p2_16_* /// 
p2_17_* p2_18_* p2_54_* p2_55_* p2_59_* p2_60_* p2_61_* p2_55_4 p2_61_4 p3_1 p3_2_* p3_5_* p3_6_* p3_9_* p3_10_* ///
p3_11_* p3_19 p3_20 p4_1 p4_2 p4_3 p4_4 p4_5 p4_7_* p4_8* p4_10 p4_12 p4_15 p4_17 p4_18 p4_21 p4_24 p4_27 p4_28 p4_28a p4_31_* ///
p4_33 p4_35 p4_38 p4_39 p5_1 p5_1a p5_6_* p5_6a_* p5_7_* p5_9 p5_10 p5_11a p5_13_* p5_14_* p5_16_* p5_17a_* p5_17b_* p4_40 p4_41 p4_42 p4_43 p4_16 p2_24 p4_25 p4_36 p4_109_* p4_105_* p4_101 p4_102 ///
p4_104_* p4_111_* p4_112* p4_116_* p8_5a using "$path/2008/`c'/other_sections_2008_`c'.dta", clear 


*To calculate the percentage of households that own their main residence we generate a new variable;
gen np2_1=(p2_1==2 & p2_5>0 & p2_5~=.)

*To calculate the percentage of households that own other real estate properties we generate a new variable
gen np2_32=((p2_32==1 & p2_33>=1 & p2_33~=. & p2_39_1>0 & p2_39_1~=.)| ///
            (p2_32==1 & p2_33>=2 & p2_33~=. & p2_39_2>0 & p2_39_2~=.)| ///
            (p2_32==1 & p2_33>=3 & p2_33~=. & p2_39_3>0 & p2_39_3~=.)| ///
            (p2_32==1 & p2_33>3  & p2_33~=. & p2_39_4>0 & p2_39_4~=.))


*To calculate the percentage of households that own jewellery, works of art and antiques we generate a new variable
gen np2_82=(p2_82==1 & p2_84>0 & p2_84~=.)

*To calculate the percentage of households with some business we use the variables p4_101 and p4_111; 
gen haveneg =(p4_101==1)
gen valhog =0
replace valhog =valhog + p4_111_1 if p4_101==1 & p4_111_1>0 & p4_111_1~=.
replace valhog =valhog + p4_111_2 if p4_101==1 & p4_111_2>0 & p4_111_2~=.
replace valhog =valhog + p4_111_3 if p4_101==1 & p4_111_3>0 & p4_111_3~=.
replace valhog =valhog + p4_111_4 if p4_101==1 & p4_111_4>0 & p4_111_4~=.
replace valhog =valhog + p4_111_5 if p4_101==1 & p4_111_5>0 & p4_111_5~=.
replace valhog =valhog + p4_111_6 if p4_101==1 & p4_111_6>0 & p4_111_6~=.

gen havenegval =(haveneg==1 & valhog>0)

*To calculate the percentage of households that own accounts and deposits usable for payments and that declare a strictly positive value for the balance of those accounts we generate a new variable;
gen np4_5=(p4_5==1 & p4_7_3>0 & p4_7_3~=.)

*To calculate the percentage of households that own listed and that declare a strictly positive value for that portfolio we generate a new variable
gen np4_10=(p4_10==1 & p4_15>0 & p4_15~=.)

*To calculate the percentage of households that own unlisted shares and other equity and that declare a strictly positive value for that portfolio we generate a new variable;
gen np4_18=(p4_18==1 & p4_24>0 & p4_24~=.)

*To calculate the percentage of households that own fixed-income securities and that declare a strictly positive value for that portfolio we generate a new variable;
gen np4_33=(p4_33==1 & p4_35>0 & p4_35~=.)

*To calculate the percentage of households that own mutual funds and that declare a strictly positive value for that portfolio we generate a new variable;
gen np4_27=((p4_27==1 & p4_28>=1 & p4_28~=. & p4_31_1>0 & p4_31_1~=.)| ///
            (p4_27==1 & p4_28>=2 & p4_28~=. & p4_31_2>0 & p4_31_2~=.)| ///
            (p4_27==1 & p4_28>=3 & p4_28~=. & p4_31_3>0 & p4_31_3~=.)| ///
            (p4_27==1 & p4_28>=4 & p4_28~=. & p4_31_4>0 & p4_31_4~=.)| ///
            (p4_27==1 & p4_28>=5 & p4_28~=. & p4_31_5>0 & p4_31_5~=.)| ///
            (p4_27==1 & p4_28>=6 & p4_28~=. & p4_31_6>0 & p4_31_6~=.)| ///
            (p4_27==1 & p4_28>=7 & p4_28~=. & p4_31_7>0 & p4_31_7~=.)| ///
            (p4_27==1 & p4_28>=8 & p4_28~=. & p4_31_8>0 & p4_31_8~=.)| ///
            (p4_27==1 & p4_28>=9 & p4_28~=. & p4_31_9>0 & p4_31_9~=.)| ///
            (p4_27==1 & p4_28>9 & p4_28~=. & p4_31_10>0 & p4_31_10~=.))


*To calculate the percentage of households that own house-purchase saving accounts and/or accounts not usable for payments and that declare a strictly positive value for the balance of those accounts we generate a new variable;
gen cuentas=((p4_3==1 & p4_7_1>0 & p4_7_1~=.)|(p4_4==1 & p4_7_2>0 & p4_7_2~=.))

*To calculate the percentage of households that own pension schemes and that declare a strictly positive value for the balance of those pension schemes we generate a new variable
gen np5_1=((p5_1==1 & p5_1a>=1 & p5_1a~=. & p5_7_1>0 & p5_7_1~=.)| ///
           (p5_1==1 & p5_1a>=2 & p5_1a~=. & p5_7_2>0 & p5_7_2~=.)| ///
           (p5_1==1 & p5_1a>=3 & p5_1a~=. & p5_7_3>0 & p5_7_3~=.)| ///
           (p5_1==1 & p5_1a>=4 & p5_1a~=. & p5_7_4>0 & p5_7_4~=.)| ///
           (p5_1==1 & p5_1a>=5 & p5_1a~=. & p5_7_5>0 & p5_7_5~=.)| ///
           (p5_1==1 & p5_1a>=6 & p5_1a~=. & p5_7_6>0 & p5_7_6~=.)| ///
           (p5_1==1 & p5_1a>=7 & p5_1a~=. & p5_7_7>0 & p5_7_7~=.)| /// 
           (p5_1==1 & p5_1a>=8 & p5_1a~=. & p5_7_8>0 & p5_7_8~=.)| ///
           (p5_1==1 & p5_1a>=9 & p5_1a~=. & p5_7_9>0 & p5_7_9~=.)| ///
           (p5_1==1 & p5_1a>9 & p5_1a~=. & p5_7_10>0 & p5_7_10~=.))
		   

*To calculate the percentage of households that own unit-linked or mixed life insurance we generate a new variable
gen seguro=((p5_9==1 & p5_11a>=1 & p5_11a~=. & p5_13_1==2 & p5_14_1>0 & p5_14_1~=.)| ///
			(p5_9==1 & p5_11a>=2 & p5_11a~=. & p5_13_2==2 & p5_14_2>0 & p5_14_2~=.)| /// 
			(p5_9==1 & p5_11a>=3 & p5_11a~=. & p5_13_3==2 & p5_14_3>0 & p5_14_3~=.)| /// 
			(p5_9==1 & p5_11a>=4 & p5_11a~=. & p5_13_4==2 & p5_14_4>0 & p5_14_4~=.)| ///
			(p5_9==1 & p5_11a>=5 & p5_11a~=. & p5_13_5==2 & p5_14_5>0 & p5_14_5~=.)| ///
			(p5_9==1 & p5_11a==6 & p5_11a~=. & p5_13_6==2 & p5_14_6>0 & p5_14_6~=.)| ///
			(p5_9==1 & p5_11a>=1 & p5_11a~=. & p5_13_1==3 & p5_14_1>0 & p5_14_1~=.)| ///
			(p5_9==1 & p5_11a>=2 & p5_11a~=. & p5_13_2==3 & p5_14_2>0 & p5_14_2~=.)| /// 
			(p5_9==1 & p5_11a>=3 & p5_11a~=. & p5_13_3==3 & p5_14_3>0 & p5_14_3~=.)| ///
			(p5_9==1 & p5_11a>=4 & p5_11a~=. & p5_13_4==3 & p5_14_4>0 & p5_14_4~=.)| ///
			(p5_9==1 & p5_11a>=5 & p5_11a~=. & p5_13_5==3 & p5_14_5>0 & p5_14_5~=.)| ///
			(p5_9==1 & p5_11a>5 & p5_11a~=. & p5_13_6==3 & p5_14_6>0 & p5_14_6~=.))			


*Portfolios under management 
gen cart_gest=((p4_41==1 & p4_42==1) & p4_43>0 & p4_43~=.)

*** DEFINE NOW THE VALUES *** 
*1. REAL ASSETS
*To obtain the value of the main residence we generate a new variable
gen np2_5=p2_5 if p2_1b==1
replace np2_5=p2_5*(p2_1c/100) if p2_1b==2

*To obtain the value of the other real estate properties we generate a new variable
gen otraspr=0
replace otraspr=otraspr+p2_39_1*(p2_37_1/100) if (p2_33>=1 & p2_33~=. & p2_39_1>=0 & p2_39_1~=. & p2_37_1>0 & p2_37_1~=.)
replace otraspr=otraspr+p2_39_2 *(p2_37_2/100) if (p2_33>=2 & p2_33~=. & p2_39_2>=0 & p2_39_2~=. & p2_37_2>0 & p2_37_2~=.)
replace otraspr=otraspr+p2_39_3* (p2_37_3/100) if (p2_33>=3 & p2_33~=. & p2_39_3>=0 & p2_39_3~=. & p2_37_3>0 & p2_37_3~=.)
replace otraspr=otraspr+p2_39_4 if (p2_33>3 & p2_33~=. & p2_39_4>=0 & p2_39_4~=.)

*To obtain the value of the jewellery, works of art and antiques we use the variable p2_84;

* 2. FINANCIAL ASSETS
*To obtain the balance of the accounts and deposits usable for payments we use the variable p4_7_3;

*To obtain the value of the listed shares we use the variable p4_15;

*To obtain the value of the unlisted shares and other equity we use the variable p4_24;

*To obtain the value of the fixed-income securities we use the variable p4_35

/* To obtain the total value of mutual funds we use the variable allf calculated as (i) the addition of the values of each mutual fund that the household owns (p4_31_i; i=1,…,10) 
if the number of these funds is 10 or less, and (ii) the household mutual funds’ total value if this one owns more than 10 (p4_28a) */ 
egen allf=rowtotal(p4_31_1 p4_31_2 p4_31_3 p4_31_4 p4_31_5 p4_31_6 p4_31_7 p4_31_8 p4_31_9 p4_31_10) 
replace allf=p4_28a if p4_28>10 

*To obtain the balance of the house-purchase saving accounts and the accounts and deposits not usable for payments we generate a new variable;  
gen salcuentas=0
replace salcuentas = salcuentas +p4_7_1 if p4_3==1
replace salcuentas = salcuentas + p4_7_2 if p4_4==1

*To obtain the current value of the pension schemes we generate a new variable;
gen valor=0
replace valor = valor +p5_7_1 if (p5_1==1 & p5_7_1>=0 & p5_7_1~=. )
replace valor = valor + p5_7_2 if (p5_1==1 & p5_7_2>=0 & p5_7_2~=.)
replace valor = valor + p5_7_3 if (p5_1==1 & p5_7_3>=0 & p5_7_3~=.)
replace valor = valor + p5_7_4 if (p5_1==1 & p5_7_4>=0 & p5_7_4~=.)
replace valor = valor + p5_7_5 if (p5_1==1 & p5_7_5>=0 & p5_7_5~=.)
replace valor = valor + p5_7_6 if (p5_1==1 & p5_7_6>=0 & p5_7_6~=.)
replace valor = valor + p5_7_7 if (p5_1==1 & p5_7_7>=0 & p5_7_7~=.)
replace valor = valor + p5_7_8 if (p5_1==1 & p5_7_8>=0 & p5_7_8~=.)
replace valor = valor + p5_7_9 if (p5_1==1 & p5_7_9>=0 & p5_7_9~=.)
replace valor = valor + p5_7_10 if (p5_1==1 & p5_7_10>=0 & p5_7_10~=.)

*To obtain the value of the unit-linked or mixed life insurance we generate a new variable;
gen valseg=0
replace valseg = valseg +p5_14_1 if ((p5_13_1==2| p5_13_1==3) & p5_14_1>=0 & p5_14_1~=.)
replace valseg = valseg +p5_14_2 if ((p5_13_2==2| p5_13_2==3) & p5_14_2>=0 & p5_14_2~=.)
replace valseg = valseg +p5_14_3 if ((p5_13_3==2| p5_13_3==3) & p5_14_3>=0 & p5_14_3~=.)
replace valseg = valseg +p5_14_4 if ((p5_13_4==2| p5_13_4==3) & p5_14_4>=0 & p5_14_4~=.)
replace valseg = valseg +p5_14_5 if ((p5_13_5==2| p5_13_5==3) & p5_14_5>=0 & p5_14_5~=.)
replace valseg = valseg +p5_14_6 if ((p5_13_6==2| p5_13_6==3) & p5_14_6>=0 & p5_14_6~=.)

*To obtain the value of the portfolios under management we use the variable p4_43

*To obtain the median of how much is owed to the household, we use the variables valdeuhog and p4_38 and generate a new variable; 
gen odeuhog=0
replace odeuhog = odeuhog + p4_38 if (p4_38>0 & p4_38~=.)
replace odeuhog = odeuhog + p4_116_1 if (p4_116_1>0 & p4_116_1!=.) 
replace odeuhog = odeuhog + p4_116_2 if (p4_116_2>0 & p4_116_2!=.) 
replace odeuhog = odeuhog + p4_116_3 if (p4_116_3>0 & p4_116_3!=.) 
replace odeuhog = odeuhog + p4_116_4 if (p4_116_4>0 & p4_116_4!=.) 
replace odeuhog = odeuhog + p4_116_5 if (p4_116_5>0 & p4_116_5!=.) 
replace odeuhog = odeuhog + p4_116_6 if (p4_116_6>0 & p4_116_6!=.) 


*3. DEBTS 
*To obtain the percentage of households that have outstanding debt from loans used to purchase their main residence, we generate a new variable;
gen np2_8=p2_8
replace np2_8=0 if p2_8==.


*To obtain the value of the outstanding debts from loans used to purchase their main residence, we generate a new variable
gen dvivpral=0
replace dvivpral= dvivpral +p2_12_1 if (p2_8a>=1 & p2_8a~=. & p2_12_1>0 & p2_12_1~=.)
replace dvivpral= dvivpral +p2_12_2 if (p2_8a>=2 & p2_8a~=. & p2_12_2>0 & p2_12_2~=.)
replace dvivpral= dvivpral +p2_12_3 if (p2_8a>=3 & p2_8a~=. & p2_12_3>0 & p2_12_3~=.)
replace dvivpral= dvivpral +p2_12_4 if (p2_8a>3 & p2_8a~=. & p2_12_4>0 & p2_12_4~=.)

*To obtain the percentage of households that have outstanding debts from loans used to purchase other real estate properties different from the main residence, we generate a new variable
gen dpdte=(p2_50_1==1|p2_50_2==1|p2_50_3==1|p2_50_4==1)

*To obtain the value of the outstanding debts from loans used to purchase other real estate properties different from the main residence, we generate four new variables;
*1st real property 
gen dprop1=0
replace dprop1= dprop1+p2_55_1_1 if (p2_51_1>=1 & p2_51_1~=. & p2_55_1_1>0 & p2_55_1_1~=.)
replace dprop1= dprop1+p2_55_1_2 if (p2_51_1>=2 & p2_51_1~=. & p2_55_1_2>0 & p2_55_1_2~=.)
replace dprop1= dprop1+p2_55_1_3 if (p2_51_1>=3 & p2_51_1~=. & p2_55_1_3>0 & p2_55_1_3~=.)

*2nd real property 
gen dprop2=0
replace dprop2= dprop2+p2_55_2_1 if (p2_51_2>=1 & p2_51_2~=. & p2_55_2_1>0 & p2_55_2_1~=.)
replace dprop2= dprop2+p2_55_2_2 if (p2_51_2>=2 & p2_51_2~=. & p2_55_2_2>0 & p2_55_2_2~=.)
replace dprop2= dprop2+p2_55_2_3 if (p2_51_2>=3 & p2_51_2~=. & p2_55_2_3>0 & p2_55_2_3~=.)

*3rd real property
gen dprop3=0
replace dprop3= dprop3+p2_55_3_1 if (p2_51_3>=1 & p2_51_3~=. & p2_55_3_1>0 & p2_55_3_1~=.)
replace dprop3= dprop3+p2_55_3_2 if (p2_51_3>=2 & p2_51_3~=. & p2_55_3_2>0 & p2_55_3_2~=.)
replace dprop3= dprop3+p2_55_3_3 if (p2_51_3>=3 & p2_51_3~=. & p2_55_3_3>0 & p2_55_3_3~=.)

*All other real properties
gen dprop4=0
replace dprop4= dprop4+p2_55_4 if (p2_55_4>0 & p2_55_4~=.)

*Considering all real estate but main residence 
gen deuoprop= dprop1+ dprop2+ dprop3+ dprop4 


*To obtain the value of the outstanding debts from loans with mortgage guarantee used for the purchase of the main residence, we generate a new variable;
gen deuhipv =0
replace deuhipv= deuhipv +p2_12_1 if (p2_8a>=1 & p2_8a~=. & p2_9_1==1 & p2_12_1>0 & p2_12_1~=.)
replace deuhipv= deuhipv +p2_12_2 if (p2_8a>=2 & p2_8a~=. & p2_9_2==1 & p2_12_2>0 & p2_12_2~=.)
replace deuhipv= deuhipv +p2_12_3 if (p2_8a>=3 & p2_8a~=. & p2_9_3==1 & p2_12_3>0 & p2_12_3~=.)
replace deuhipv= deuhipv +p2_12_4 if (p2_8a>3 & p2_8a~=. & p2_9_4==1 & p2_12_4>0 & p2_12_4~=.)

*To obtain the percentage of households that have outstanding debts from mortgages and other secured loans we generate a new variable
gen hipo=(p3_2_1==1| p3_2_2==1| p3_2_3==1| p3_2_4==1|p3_2_5==1| p3_2_6==1| p3_2_7==1| p3_2_8==1| ///
p3_2_1==2| p3_2_2==2| p3_2_3==2| p3_2_4==2|p3_2_4==2|p3_2_5==2| p3_2_6==2| p3_2_7==2| p3_2_8==2| ///
p3_2_1==10| p3_2_2==10| p3_2_3==10| p3_2_4==10|p3_2_5==10| p3_2_6==10| p3_2_7==10| p3_2_8==10)

*To obtain the value of the outstanding debts from mortgages and other loans with real guarantee we generate a new variable;
gen phipo=0
replace phipo = phipo +p3_6_1 if ((p3_2_1==1|p3_2_1==2|p3_2_1==10) & p3_6_1>0 & p3_6_1~=.)
replace phipo = phipo +p3_6_2 if ((p3_2_2==1|p3_2_2==2|p3_2_2==10) & p3_6_2>0 & p3_6_2~=.)
replace phipo = phipo +p3_6_3 if ((p3_2_3==1|p3_2_3==2|p3_2_3==10) & p3_6_3>0 & p3_6_3~=.)
replace phipo = phipo +p3_6_4 if ((p3_2_4==1|p3_2_4==2|p3_2_4==10) & p3_6_4>0 & p3_6_4~=.)
replace phipo = phipo +p3_6_5 if ((p3_2_5==1|p3_2_5==2|p3_2_5==10) & p3_6_5>0 & p3_6_5~=.)
replace phipo = phipo +p3_6_6 if ((p3_2_6==1|p3_2_6==2|p3_2_6==10) & p3_6_6>0 & p3_6_6~=.)
replace phipo = phipo +p3_6_7 if ((p3_2_7==1|p3_2_7==2|p3_2_7==10) & p3_6_7>0 & p3_6_7~=.)
replace phipo = phipo +p3_6_8 if ((p3_2_8==1|p3_2_8==2|p3_2_8==10) & p3_6_8>0 & p3_6_8~=.)

*To obtain the value of the outstanding debts from personal loans we generate a new variable;
gen pperso=0
replace pperso = pperso +p3_6_1 if (p3_2_1==3 & p3_6_1>0 & p3_6_1~=.)
replace pperso = pperso +p3_6_2 if (p3_2_2==3 & p3_6_2>0 & p3_6_2~=.)
replace pperso = pperso +p3_6_3 if (p3_2_3==3 & p3_6_3>0 & p3_6_3~=.)
replace pperso = pperso +p3_6_4 if (p3_2_4==3 & p3_6_4>0 & p3_6_4~=.)
replace pperso = pperso +p3_6_5 if (p3_2_5==3 & p3_6_5>0 & p3_6_5~=.)
replace pperso = pperso +p3_6_6 if (p3_2_6==3 & p3_6_6>0 & p3_6_6~=.)
replace pperso = pperso +p3_6_7 if (p3_2_7==3 & p3_6_7>0 & p3_6_7~=.)
replace pperso = pperso +p3_6_8 if (p3_2_8==3 & p3_6_8>0 & p3_6_8~=.)

*To obtain the value of outstanding credit card balances we generate a new variable;
gen ptmos_tarj=0
replace ptmos_tarj= p8_5a if (p8_5a>0 & p8_5a~=.)

*To obtain the value of the other outstanding debts we generate a new variable;
gen potrasd =0
replace potrasd = potrasd +p3_6_1 if ((p3_2_1==4| p3_2_1==5| p3_2_1==6| p3_2_1==7| p3_2_1==8|p3_2_1==9|p3_2_1==97) & p3_6_1>0 & p3_6_1~=.)
replace potrasd = potrasd +p3_6_2 if ((p3_2_2==4| p3_2_2==5|  p3_2_2==6| p3_2_2==7| p3_2_2==8|p3_2_2==9|p3_2_2==97) & p3_6_2>0 & p3_6_2~=.)
replace potrasd = potrasd +p3_6_3 if ((p3_2_3==4| p3_2_3==5| p3_2_3==6| p3_2_3==7| p3_2_3==8|p3_2_3==9|p3_2_3==97) & p3_6_3>0 & p3_6_3~=.)
replace potrasd = potrasd +p3_6_4 if ((p3_2_4==4| p3_2_4==5| p3_2_4==6| p3_2_4==7| p3_2_4==8|p3_2_4==9|p3_2_4==97) & p3_6_4>0 & p3_6_4~=.)
replace potrasd = potrasd +p3_6_5 if ((p3_2_5==4| p3_2_5==5| p3_2_5==6| p3_2_5==7| p3_2_5==8|p3_2_5==9|p3_2_5==97) & p3_6_5>0 & p3_6_5~=.)
replace potrasd = potrasd +p3_6_6 if ((p3_2_6==4| p3_2_6==5| p3_2_6==6| p3_2_6==7| p3_2_6==8|p3_2_6==9|p3_2_6==97) & p3_6_6>0 & p3_6_6~=.)
replace potrasd = potrasd +p3_6_7 if ((p3_2_7==4| p3_2_7==5| p3_2_7==6| p3_2_7==7| p3_2_7==8|p3_2_7==9|p3_2_7==97) & p3_6_7>0 & p3_6_7~=.)
replace potrasd = potrasd +p3_6_8 if ((p3_2_8==4| p3_2_8==5| p3_2_8==6| p3_2_8==7| p3_2_8==8|p3_2_8==9|p3_2_8==97) & p3_6_8>0 & p3_6_8~=.)

*To obtain the value of the outstanding debt we generate a new variable;
gen vdeuda= dvivpral + deuoprop+ phipo+ pperso+ potrasd + ptmos_tarj

save "$output/2008/wealth_`c'", replace 

** INCOME FLOWS **
use h_* p6_14_* p6_16_* p6_28a_* p6_28b_* p6_28c_* p6_28d_* p6_47_* p6_48a_* p6_49_* p6_50_* /// 
p6_52 p6_54 p6_56 p6_58 p6_60 p6_60c p6_60d p6_60e* p6_60f p6_101_* p6_102_* /// 
p6_103_* p6_104_*  using "$path/2008/`c'/section6_2008_`c'.dta", clear 

drop p6_60es2 

* Take the hh size 
merge 1:m h_2008 using temp1_`c'.dta, keepus(p1) nogen 

* Change the orders for the variables that has the hh_member in the wrong place * 
ds p6_14_* p6_16_* p6_49_* p6_50_* p6_101_* p6_102_* p6_103_* p6_104_*
rename (*_*_*_*) (*_*_*[4]_*[3]) 


* Reshape the selected variables from wide to long * 
ds p6_52 p6_54 p6_56 p6_58 p6_60* h_* p1, not 
local lisvr `r(varlist)'
disp "`lisvr'"
foreach h of local lisvr { 
	local stab = substr("`h'", 1, strrpos("`h'", "_"))
    local stabs `stabs' `stab'
} 
local stabs : list uniq stabs
disp "`stabs'" 
reshape long `stabs', i(h_2008) j(ind_id)
bysort h_2008 : drop if _n>p1 
rensfix _


* Create the sums for labour incomes across different existing jobs * 
egen tot_employee=rowtotal(p6_14_1 p6_14_2 p6_14_3 p6_16_1 p6_16_2 p6_16_3) 
/* We need to ask how to aggregate the self-employed income */ 
egen tot_pension=rowtotal(p6_49_1 p6_49_2 p6_49_3 p6_49_4) 

save "$output/2008/income_`c'", replace 

use "$output/2008/sdem_`c'", clear 
merge 1:1 h_2008 ind_id using "$output/2008/lab_status_`c'", nogen 
merge 1:1 h_2008 ind_id using "$output/2008/income_`c'", nogen 
merge m:1 h_2008 using "$output/2008/wealth_`c'", nogen 

* Create the wealth vars

*REAL ASSETS
gen actreales=0
replace actreales=actreales+np2_5 if (np2_5>0 & np2_5~=.)
replace actreales=actreales+otraspr if (otraspr>0 & otraspr~=.)
replace actreales=actreales+p2_84 if (p2_84>0 & p2_84~=.)
replace actreales=actreales+valhog if (valhog>0 & valhog~=.)

*FINANCIAL ASSETS;
gen actfinanc=0
replace actfinanc=actfinanc+p4_7_3 if (p4_7_3>0 & p4_7_3~=.)
replace actfinanc=actfinanc+p4_15 if (p4_15>0 & p4_15~=.)
replace actfinanc=actfinanc+p4_24 if (p4_24>0 & p4_24~=.)
replace actfinanc=actfinanc+p4_35 if (p4_35>0 & p4_35~=.)
replace actfinanc=actfinanc+allf if (allf>0 & allf~=.)
replace actfinanc=actfinanc+p4_43 if (p4_43>0 & p4_43~=.)
replace actfinanc=actfinanc+salcuentas if (salcuentas>0 & salcuentas~=.)
replace actfinanc=actfinanc+valor if (valor>0 & valor~=.)
replace actfinanc=actfinanc+valseg if (valseg>0 & valseg~=.)
replace actfinanc=actfinanc+odeuhog if (odeuhog>0 & odeuhog~=.)

gen riquezabr=0
replace riquezabr=riquezabr+actreales+actfinanc

gen riquezanet=riquezabr-vdeuda


* Rename household number 
rename h_2008 h_id 
drop h_2005 

rensfix _
gen anno=2008

save "$append/`c'/final_2008_`c'.dta", replace 


} 


**********************************************************************************************************************************

*** FOR THE 2011, 2014 AND 2017 WE CAN LOOP OVER YEARS AND IMPUTATION *** 

local year 2011 2014 2017
foreach c in imp1 imp2 imp3 imp4 imp5 { 
		foreach y of local year {
/* The socio-demographic variables need a reshape from wide to long in order to have individual rows within hh, as the 
questions are asked to each single individual in the hh. However, the wealth information are at the hh level! The different _x 
are now at the asset perspective, not individual one. Therefore, the wealth variables can be simply taken as given, and then
take the total as the rowsum of the asset. However, since 2008 there are changes in the sections for self-employment, that 
are now in the section 4. So, we extract them now, without reshaping, but we need to generate the rowtotal!!  */ 

use h_* facine3  pesopan_* hogar* pan_* p1 p1_1_* p1_2b_* p1_3_* p1_4_* p1_5_* p1_52_* p1_13 p1_14* renthog mrenthog using "$path/`y'/`c'/other_sections_`y'_`c'.dta", clear 
save temp1_`y'_`c'.dta, replace 

* Reshape wide to long 
reshape long pan_@ p1_1_@ p1_2b_@ p1_3_@ p1_4_@ p1_5_@ p1_52_@, i(h_`y') j(ind_id) 

* Drop the additional rows, as not all hh have 9 members 
bysort h_`y' : drop if _n>p1 

/* The information about the firms/commercial activities are at the firm/commercial level. Therefore, we need 
the rowtotal and not a reshape! */ 
 

* Save the socio-demographic 
save "$output/`y'/sdem_`c'", replace 

** LABOUR MARKET VARIABLES ** 
use h_* p6_1c* p6_3_* p6_4_* p6_8_* p6_9_* p6_10_* p6_11_* p6_12_* p6_13_* p6_17_* p6_20_* p6_27* p6_29_* p6_30_* p6_31* p6_33* p6_37_* /// 
p6_77_* p6_78_* p6_79_* p6_103_* using "$path/`y'/`c'/section6_`y'_`c'.dta", clear 


* Take the hh size 
merge 1:m h_`y' using temp1_`y'_`c'.dta, keepus(p1) nogen 

* Adjust the labur market status and sources of income 
forvalues i=1/9 { 
	gen p6_1_`i'=1 if p6_1c1_`i'==1 
	replace p6_1_`i'=2 if p6_1c2_`i'==1 
	replace p6_1_`i'=3 if p6_1c3_`i'==1 
	replace p6_1_`i'=4 if p6_1c4_`i'==1 
	replace p6_1_`i'=5 if p6_1c5_`i'==1 
	replace p6_1_`i'=6 if p6_1c6_`i'==1 
	replace p6_1_`i'=7 if p6_1c7_`i'==1 
	replace p6_1_`i'=8 if p6_1c8_`i'==1 
	
	label define p6_1_`i' 1 "Employee" 2 "Self-employed" 3 "Unemployed" 4 "Retired" 5 "Disabled" 6 "Student" 7 "Housekeeper" 8 "Other inactives"
	label values p6_1_`i' p6_1_`i' 
	
/* Have a check on whether is necessary some adjustments! */ 

	
} 


* Change the orders for the variables that has the hh_member in the wrong place * 
ds p6_10_* p6_11_* p6_12_* p6_13_* p6_17_* p6_20_* p6_33_* p6_37_* p6_103_*
rename (*_*_*_*) (*_*_*[4]_*[3]) 

* Reshape all the variables from wide to long * 
/* reshape long p6_1_ p6_3_ p6_4_ p6_8_ p6_9_ p6_11_1_ p6_11_2_ p6_11_3_ p6_12_1_ p6_12_2_ p6_12_3_ p6_13_1_ p6_13_2_ p6_13_3_ ///
p6_17_1_ p6_17_2_ p6_17_3_ p6_20_1_ p6_20_2_ p6_20_3_ p6_33_1_ p6_33_2_ p6_33_3_ p6_34_1_ p6_34_2_ p6_34_3_ p6_36_1_ ///
p6_36_2_ p6_36_3_ p6_37_1_ p6_37_2_ p6_37_3_ p6_27_ p6_29_ p6_30_ p6_31_ p6_77_ p6_78_ p6_79_ p6_103_, i(h_number) j(ind_id) 
*/ 

foreach v of var p* {
    local stub = substr("`v'", 1, strrpos("`v'", "_"))
    local stubs `stubs' `stub'
}

local stubs : list uniq stubs
reshape long `stubs', i(h_`y') j(ind_id)

bysort h_`y': drop if _n>p1 

* Reshape the variables for the job numer * 
rensfix _
drop p6_1c* 

/* In case we want to reshape the jobs as well: 
reshape long p6_11_ p6_12_ p6_13_ p6_17_ p6_20_ p6_33_ p6_34_ p6_36_ p6_37_ , i(h_number ind_id) j(job_num)
However, there are only 37 observations with more than 1 occupation. Therefore, I leave the corresponding variables separated. 

Furthermore, 
egen row_tot=rowtotal(p6_1c1 p6_1c2 p6_1c3 p6_1c4 p6_1c5 p6_1c6 p6_1c7 p6_1c8) 
there are 136 observations that have non-consistent dummy definition for the labour status. 
count if row_tot>1 --> 136. Thinking of dropping them for inconsistency 
*/ 


save "$output/`y'/lab_status_`c'", replace 


** WEALTH VARIABLES AND INCOME FLOWS FROM WEALTH ** 
use h_* p2_1 p2_1b p2_1c p2_2 p2_3 p2_4 p2_5 p2_7 p2_8 p2_8a p2_22 p2_23 p2_31 p2_32 p2_33 p2_35a_* p2_37_* p2_38_* p2_39_* p2_42_* p2_42s* /// 
p2_43_* p2_39_4 p2_50_* p2_51_* p2_64 p2_72 p2_75 p2_76 p2_79 p2_81 p2_82 p2_84 p2_88 p2_9_* p2_11_* p2_12_* p2_16_* /// 
p2_17_* p2_18_* p2_54_* p2_55_* p2_59_* p2_60_* p2_61_* p2_55_4 p2_61_4 p3_1 p3_2_* p3_5_* p3_6_* p3_9_* p3_10_* ///
p3_11_* p3_19 p3_20 p4_1 p4_2 p4_3 p4_4 p4_5 p4_7_* p4_8* p4_10 p4_12 p4_15 p4_17 p4_18 p4_21 p4_24 p4_27 p4_28 p4_28a p4_31_* ///
p4_33 p4_35 p4_38 p4_39 p5_1 p5_1a p5_6_* p5_6a_* p5_7_* p5_9* p5_10* p5_13_* p5_14_* p5_16_* p5_17a_* p5_17b_* p4_40 p4_41 p4_42 p4_43 p4_16 p2_24 p4_25 p4_36 p4_109_* p4_105_* p4_101 p4_102 ///
p4_104_* p4_111_* p4_112* p4_116_* p8_5a using "$path/`y'/`c'/other_sections_`y'_`c'.dta", clear 
																																								
*To calculate the percentage of households that own their main residence we generate a new variable;
gen np2_1=(p2_1==2 & p2_5>0 & p2_5~=.)

*To calculate the percentage of households that own other real estate properties we generate a new variable
gen np2_32=((p2_32==1 & p2_33>=1 & p2_33~=. & p2_39_1>0 & p2_39_1~=.)| ///
            (p2_32==1 & p2_33>=2 & p2_33~=. & p2_39_2>0 & p2_39_2~=.)| ///
            (p2_32==1 & p2_33>=3 & p2_33~=. & p2_39_3>0 & p2_39_3~=.)| ///
            (p2_32==1 & p2_33>3  & p2_33~=. & p2_39_4>0 & p2_39_4~=.))


*To calculate the percentage of households that own jewellery, works of art and antiques we generate a new variable
gen np2_82=(p2_82==1 & p2_84>0 & p2_84~=.)

*To calculate the percentage of households with some business we use the variables p4_101 and p4_111; 
gen haveneg =(p4_101==1)
gen valhog =0
replace valhog =valhog + p4_111_1 if p4_101==1 & p4_111_1>0 & p4_111_1~=.
replace valhog =valhog + p4_111_2 if p4_101==1 & p4_111_2>0 & p4_111_2~=.
replace valhog =valhog + p4_111_3 if p4_101==1 & p4_111_3>0 & p4_111_3~=.
replace valhog =valhog + p4_111_4 if p4_101==1 & p4_111_4>0 & p4_111_4~=.
replace valhog =valhog + p4_111_5 if p4_101==1 & p4_111_5>0 & p4_111_5~=.
replace valhog =valhog + p4_111_6 if p4_101==1 & p4_111_6>0 & p4_111_6~=.

gen havenegval =(haveneg==1 & valhog>0)

*To calculate the percentage of households that own accounts and deposits usable for payments and that declare a strictly positive value for the balance of those accounts we generate a new variable;
gen np4_5=(p4_5==1 & p4_7_3>0 & p4_7_3~=.)

*To calculate the percentage of households that own listed and that declare a strictly positive value for that portfolio we generate a new variable
gen np4_10=(p4_10==1 & p4_15>0 & p4_15~=.)

*To calculate the percentage of households that own unlisted shares and other equity and that declare a strictly positive value for that portfolio we generate a new variable;
gen np4_18=(p4_18==1 & p4_24>0 & p4_24~=.)

*To calculate the percentage of households that own fixed-income securities and that declare a strictly positive value for that portfolio we generate a new variable;
gen np4_33=(p4_33==1 & p4_35>0 & p4_35~=.)

*To calculate the percentage of households that own mutual funds and that declare a strictly positive value for that portfolio we generate a new variable;
gen np4_27=((p4_27==1 & p4_28>=1 & p4_28~=. & p4_31_1>0 & p4_31_1~=.)| ///
            (p4_27==1 & p4_28>=2 & p4_28~=. & p4_31_2>0 & p4_31_2~=.)| ///
            (p4_27==1 & p4_28>=3 & p4_28~=. & p4_31_3>0 & p4_31_3~=.)| ///
            (p4_27==1 & p4_28>=4 & p4_28~=. & p4_31_4>0 & p4_31_4~=.)| ///
            (p4_27==1 & p4_28>=5 & p4_28~=. & p4_31_5>0 & p4_31_5~=.)| ///
            (p4_27==1 & p4_28>=6 & p4_28~=. & p4_31_6>0 & p4_31_6~=.)| ///
            (p4_27==1 & p4_28>=7 & p4_28~=. & p4_31_7>0 & p4_31_7~=.)| ///
            (p4_27==1 & p4_28>=8 & p4_28~=. & p4_31_8>0 & p4_31_8~=.)| ///
            (p4_27==1 & p4_28>=9 & p4_28~=. & p4_31_9>0 & p4_31_9~=.)| ///
            (p4_27==1 & p4_28>9 & p4_28~=. & p4_31_10>0 & p4_31_10~=.))


*To calculate the percentage of households that own house-purchase saving accounts and/or accounts not usable for payments and that declare a strictly positive value for the balance of those accounts we generate a new variable;
gen cuentas=((p4_3==1 & p4_7_1>0 & p4_7_1~=.)|(p4_4==1 & p4_7_2>0 & p4_7_2~=.))

*To calculate the percentage of households that own pension schemes and that declare a strictly positive value for the balance of those pension schemes we generate a new variable
gen np5_1=((p5_1==1 & p5_1a>=1 & p5_1a~=. & p5_7_1>0 & p5_7_1~=.)| ///
           (p5_1==1 & p5_1a>=2 & p5_1a~=. & p5_7_2>0 & p5_7_2~=.)| ///
           (p5_1==1 & p5_1a>=3 & p5_1a~=. & p5_7_3>0 & p5_7_3~=.)| ///
           (p5_1==1 & p5_1a>=4 & p5_1a~=. & p5_7_4>0 & p5_7_4~=.)| ///
           (p5_1==1 & p5_1a>=5 & p5_1a~=. & p5_7_5>0 & p5_7_5~=.)| ///
           (p5_1==1 & p5_1a>=6 & p5_1a~=. & p5_7_6>0 & p5_7_6~=.)| ///
           (p5_1==1 & p5_1a>=7 & p5_1a~=. & p5_7_7>0 & p5_7_7~=.)| /// 
           (p5_1==1 & p5_1a>=8 & p5_1a~=. & p5_7_8>0 & p5_7_8~=.)| ///
           (p5_1==1 & p5_1a>=9 & p5_1a~=. & p5_7_9>0 & p5_7_9~=.)| ///
           (p5_1==1 & p5_1a>9 & p5_1a~=. & p5_7_10>0 & p5_7_10~=.))
		   

*To calculate the percentage of households that own unit-linked or mixed life insurance we generate a new variable
gen seguro=((p5_9a==1 & p5_10a>=1 & p5_10a~=. & p5_13_1==2 & p5_14_1>0 & p5_14_1~=.)| ///
                    (p5_9a==1 & p5_10a>=2 & p5_10a~=. & p5_13_2==2 & p5_14_2>0 & p5_14_2~=.)| ///
                    (p5_9a==1 & p5_10a>=3 & p5_10a~=. & p5_13_3==2 & p5_14_3>0 & p5_14_3~=.)| ///
                    (p5_9a==1 & p5_10a>=4 & p5_10a~=. & p5_13_4==2 & p5_14_4>0 & p5_14_4~=.)| ///
                    (p5_9a==1 & p5_10a>=5 & p5_10a~=. & p5_13_5==2 & p5_14_5>0 & p5_14_5~=.)| /// 
                    (p5_9a==1 & p5_10a>5 &   p5_10a~=. & p5_13_6==2 & p5_14_6>0 & p5_14_6~=.)| /// 
					(p5_9a==1 & p5_10a>=1 & p5_10a~=. & p5_13_1==3 & p5_14_1>0 & p5_14_1~=.)| ///
                    (p5_9a==1 & p5_10a>=2 & p5_10a~=. & p5_13_2==3 & p5_14_2>0 & p5_14_2~=.)| ///
                    (p5_9a==1 & p5_10a>=3 & p5_10a~=. & p5_13_3==3 & p5_14_3>0 & p5_14_3~=.)| ///
                    (p5_9a==1 & p5_10a>=4 & p5_10a~=. & p5_13_4==3 & p5_14_4>0 & p5_14_4~=.)| ///
                    (p5_9a==1 & p5_10a>=5 & p5_10a~=. & p5_13_5==3 & p5_14_5>0 & p5_14_5~=.)| ///
                    (p5_9a==1 & p5_10a>5 &   p5_10a~=. & p5_13_6==3 & p5_14_6>0 & p5_14_6~=.))


*Portfolios under management 
gen cart_gest=((p4_41==1 & p4_42==1) & p4_43>0 & p4_43~=.)

*** DEFINE NOW THE VALUES *** 
*1. REAL ASSETS
*To obtain the value of the main residence we generate a new variable
gen np2_5=p2_5 if p2_1b==1
replace np2_5=p2_5*(p2_1c/100) if p2_1b==2

*To obtain the value of the other real estate properties we generate a new variable
gen otraspr=0
replace otraspr=otraspr+p2_39_1*(p2_37_1/100) if (p2_33>=1 & p2_33~=. & p2_39_1>=0 & p2_39_1~=. & p2_37_1>0 & p2_37_1~=.)
replace otraspr=otraspr+p2_39_2 *(p2_37_2/100) if (p2_33>=2 & p2_33~=. & p2_39_2>=0 & p2_39_2~=. & p2_37_2>0 & p2_37_2~=.)
replace otraspr=otraspr+p2_39_3* (p2_37_3/100) if (p2_33>=3 & p2_33~=. & p2_39_3>=0 & p2_39_3~=. & p2_37_3>0 & p2_37_3~=.)
replace otraspr=otraspr+p2_39_4 if (p2_33>3 & p2_33~=. & p2_39_4>=0 & p2_39_4~=.)

*To obtain the value of the jewellery, works of art and antiques we use the variable p2_84;

* 2. FINANCIAL ASSETS
*To obtain the balance of the accounts and deposits usable for payments we use the variable p4_7_3;

*To obtain the value of the listed shares we use the variable p4_15;

*To obtain the value of the unlisted shares and other equity we use the variable p4_24;

*To obtain the value of the fixed-income securities we use the variable p4_35

/* To obtain the total value of mutual funds we use the variable allf calculated as (i) the addition of the values of each mutual fund that the household owns (p4_31_i; i=1,…,10) 
if the number of these funds is 10 or less, and (ii) the household mutual funds’ total value if this one owns more than 10 (p4_28a) */ 
egen allf=rowtotal(p4_31_1 p4_31_2 p4_31_3 p4_31_4 p4_31_5 p4_31_6 p4_31_7 p4_31_8 p4_31_9 p4_31_10) 
replace allf=p4_28a if p4_28>10 

*To obtain the balance of the house-purchase saving accounts and the accounts and deposits not usable for payments we generate a new variable;  
gen salcuentas=0
replace salcuentas = salcuentas +p4_7_1 if p4_3==1
replace salcuentas = salcuentas + p4_7_2 if p4_4==1

*To obtain the current value of the pension schemes we generate a new variable;
gen valor=0
replace valor = valor +p5_7_1 if (p5_1==1 & p5_7_1>=0 & p5_7_1~=. )
replace valor = valor + p5_7_2 if (p5_1==1 & p5_7_2>=0 & p5_7_2~=.)
replace valor = valor + p5_7_3 if (p5_1==1 & p5_7_3>=0 & p5_7_3~=.)
replace valor = valor + p5_7_4 if (p5_1==1 & p5_7_4>=0 & p5_7_4~=.)
replace valor = valor + p5_7_5 if (p5_1==1 & p5_7_5>=0 & p5_7_5~=.)
replace valor = valor + p5_7_6 if (p5_1==1 & p5_7_6>=0 & p5_7_6~=.)
replace valor = valor + p5_7_7 if (p5_1==1 & p5_7_7>=0 & p5_7_7~=.)
replace valor = valor + p5_7_8 if (p5_1==1 & p5_7_8>=0 & p5_7_8~=.)
replace valor = valor + p5_7_9 if (p5_1==1 & p5_7_9>=0 & p5_7_9~=.)
replace valor = valor + p5_7_10 if (p5_1==1 & p5_7_10>=0 & p5_7_10~=.)

*To obtain the value of the unit-linked or mixed life insurance we generate a new variable;
gen valseg=0
replace valseg = valseg +p5_14_1 if ((p5_13_1==2| p5_13_1==3) & p5_14_1>=0 & p5_14_1~=.)
replace valseg = valseg +p5_14_2 if ((p5_13_2==2| p5_13_2==3) & p5_14_2>=0 & p5_14_2~=.)
replace valseg = valseg +p5_14_3 if ((p5_13_3==2| p5_13_3==3) & p5_14_3>=0 & p5_14_3~=.)
replace valseg = valseg +p5_14_4 if ((p5_13_4==2| p5_13_4==3) & p5_14_4>=0 & p5_14_4~=.)
replace valseg = valseg +p5_14_5 if ((p5_13_5==2| p5_13_5==3) & p5_14_5>=0 & p5_14_5~=.)
replace valseg = valseg +p5_14_6 if ((p5_13_6==2| p5_13_6==3) & p5_14_6>=0 & p5_14_6~=.)

*To obtain the value of the portfolios under management we use the variable p4_43

*To obtain the median of how much is owed to the household, we use the variables valdeuhog and p4_38 and generate a new variable; 
gen odeuhog=0
replace odeuhog = odeuhog + p4_38 if (p4_38>0 & p4_38~=.)
replace odeuhog = odeuhog + p4_116_1 if (p4_116_1>0 & p4_116_1!=.) 
replace odeuhog = odeuhog + p4_116_2 if (p4_116_2>0 & p4_116_2!=.) 
replace odeuhog = odeuhog + p4_116_3 if (p4_116_3>0 & p4_116_3!=.) 
replace odeuhog = odeuhog + p4_116_4 if (p4_116_4>0 & p4_116_4!=.) 
replace odeuhog = odeuhog + p4_116_5 if (p4_116_5>0 & p4_116_5!=.) 
replace odeuhog = odeuhog + p4_116_6 if (p4_116_6>0 & p4_116_6!=.) 


*3. DEBTS 
*To obtain the percentage of households that have outstanding debt from loans used to purchase their main residence, we generate a new variable;
gen np2_8=p2_8
replace np2_8=0 if p2_8==.


*To obtain the value of the outstanding debts from loans used to purchase their main residence, we generate a new variable
gen dvivpral=0
replace dvivpral= dvivpral +p2_12_1 if (p2_8a>=1 & p2_8a~=. & p2_12_1>0 & p2_12_1~=.)
replace dvivpral= dvivpral +p2_12_2 if (p2_8a>=2 & p2_8a~=. & p2_12_2>0 & p2_12_2~=.)
replace dvivpral= dvivpral +p2_12_3 if (p2_8a>=3 & p2_8a~=. & p2_12_3>0 & p2_12_3~=.)
replace dvivpral= dvivpral +p2_12_4 if (p2_8a>3 & p2_8a~=. & p2_12_4>0 & p2_12_4~=.)

*To obtain the percentage of households that have outstanding debts from loans used to purchase other real estate properties different from the main residence, we generate a new variable
gen dpdte=(p2_50_1==1|p2_50_2==1|p2_50_3==1|p2_50_4==1)

*To obtain the value of the outstanding debts from loans used to purchase other real estate properties different from the main residence, we generate four new variables;
*1st real property 
gen dprop1=0
replace dprop1= dprop1+p2_55_1_1 if (p2_51_1>=1 & p2_51_1~=. & p2_55_1_1>0 & p2_55_1_1~=.)
replace dprop1= dprop1+p2_55_1_2 if (p2_51_1>=2 & p2_51_1~=. & p2_55_1_2>0 & p2_55_1_2~=.)
replace dprop1= dprop1+p2_55_1_3 if (p2_51_1>=3 & p2_51_1~=. & p2_55_1_3>0 & p2_55_1_3~=.)

*2nd real property 
gen dprop2=0
replace dprop2= dprop2+p2_55_2_1 if (p2_51_2>=1 & p2_51_2~=. & p2_55_2_1>0 & p2_55_2_1~=.)
replace dprop2= dprop2+p2_55_2_2 if (p2_51_2>=2 & p2_51_2~=. & p2_55_2_2>0 & p2_55_2_2~=.)
replace dprop2= dprop2+p2_55_2_3 if (p2_51_2>=3 & p2_51_2~=. & p2_55_2_3>0 & p2_55_2_3~=.)

*3rd real property
gen dprop3=0
replace dprop3= dprop3+p2_55_3_1 if (p2_51_3>=1 & p2_51_3~=. & p2_55_3_1>0 & p2_55_3_1~=.)
replace dprop3= dprop3+p2_55_3_2 if (p2_51_3>=2 & p2_51_3~=. & p2_55_3_2>0 & p2_55_3_2~=.)
replace dprop3= dprop3+p2_55_3_3 if (p2_51_3>=3 & p2_51_3~=. & p2_55_3_3>0 & p2_55_3_3~=.)

*All other real properties
gen dprop4=0
replace dprop4= dprop4+p2_55_4 if (p2_55_4>0 & p2_55_4~=.)

*Considering all real estate but main residence 
gen deuoprop= dprop1+ dprop2+ dprop3+ dprop4 


*To obtain the value of the outstanding debts from loans with mortgage guarantee used for the purchase of the main residence, we generate a new variable;
gen deuhipv =0
replace deuhipv= deuhipv +p2_12_1 if (p2_8a>=1 & p2_8a~=. & p2_9_1==1 & p2_12_1>0 & p2_12_1~=.)
replace deuhipv= deuhipv +p2_12_2 if (p2_8a>=2 & p2_8a~=. & p2_9_2==1 & p2_12_2>0 & p2_12_2~=.)
replace deuhipv= deuhipv +p2_12_3 if (p2_8a>=3 & p2_8a~=. & p2_9_3==1 & p2_12_3>0 & p2_12_3~=.)
replace deuhipv= deuhipv +p2_12_4 if (p2_8a>3 & p2_8a~=. & p2_9_4==1 & p2_12_4>0 & p2_12_4~=.)

*To obtain the percentage of households that have outstanding debts from mortgages and other secured loans we generate a new variable
gen hipo=(p3_2_1==1| p3_2_2==1| p3_2_3==1| p3_2_4==1|p3_2_5==1| p3_2_6==1| p3_2_7==1| p3_2_8==1| ///
p3_2_1==2| p3_2_2==2| p3_2_3==2| p3_2_4==2|p3_2_4==2|p3_2_5==2| p3_2_6==2| p3_2_7==2| p3_2_8==2| ///
p3_2_1==10| p3_2_2==10| p3_2_3==10| p3_2_4==10|p3_2_5==10| p3_2_6==10| p3_2_7==10| p3_2_8==10)

*To obtain the value of the outstanding debts from mortgages and other loans with real guarantee we generate a new variable;
gen phipo=0
replace phipo = phipo +p3_6_1 if ((p3_2_1==1|p3_2_1==2|p3_2_1==10) & p3_6_1>0 & p3_6_1~=.)
replace phipo = phipo +p3_6_2 if ((p3_2_2==1|p3_2_2==2|p3_2_2==10) & p3_6_2>0 & p3_6_2~=.)
replace phipo = phipo +p3_6_3 if ((p3_2_3==1|p3_2_3==2|p3_2_3==10) & p3_6_3>0 & p3_6_3~=.)
replace phipo = phipo +p3_6_4 if ((p3_2_4==1|p3_2_4==2|p3_2_4==10) & p3_6_4>0 & p3_6_4~=.)
replace phipo = phipo +p3_6_5 if ((p3_2_5==1|p3_2_5==2|p3_2_5==10) & p3_6_5>0 & p3_6_5~=.)
replace phipo = phipo +p3_6_6 if ((p3_2_6==1|p3_2_6==2|p3_2_6==10) & p3_6_6>0 & p3_6_6~=.)
replace phipo = phipo +p3_6_7 if ((p3_2_7==1|p3_2_7==2|p3_2_7==10) & p3_6_7>0 & p3_6_7~=.)
replace phipo = phipo +p3_6_8 if ((p3_2_8==1|p3_2_8==2|p3_2_8==10) & p3_6_8>0 & p3_6_8~=.)

*To obtain the value of the outstanding debts from personal loans we generate a new variable;
gen pperso=0
replace pperso = pperso +p3_6_1 if (p3_2_1==3 & p3_6_1>0 & p3_6_1~=.)
replace pperso = pperso +p3_6_2 if (p3_2_2==3 & p3_6_2>0 & p3_6_2~=.)
replace pperso = pperso +p3_6_3 if (p3_2_3==3 & p3_6_3>0 & p3_6_3~=.)
replace pperso = pperso +p3_6_4 if (p3_2_4==3 & p3_6_4>0 & p3_6_4~=.)
replace pperso = pperso +p3_6_5 if (p3_2_5==3 & p3_6_5>0 & p3_6_5~=.)
replace pperso = pperso +p3_6_6 if (p3_2_6==3 & p3_6_6>0 & p3_6_6~=.)
replace pperso = pperso +p3_6_7 if (p3_2_7==3 & p3_6_7>0 & p3_6_7~=.)
replace pperso = pperso +p3_6_8 if (p3_2_8==3 & p3_6_8>0 & p3_6_8~=.)

*To obtain the value of outstanding credit card balances we generate a new variable;
gen ptmos_tarj=0
replace ptmos_tarj= p8_5a if (p8_5a>0 & p8_5a~=.)

*To obtain the value of the other outstanding debts we generate a new variable;
gen potrasd =0
replace potrasd = potrasd +p3_6_1 if ((p3_2_1==4| p3_2_1==5| p3_2_1==6| p3_2_1==7| p3_2_1==8|p3_2_1==9|p3_2_1==97) & p3_6_1>0 & p3_6_1~=.)
replace potrasd = potrasd +p3_6_2 if ((p3_2_2==4| p3_2_2==5|  p3_2_2==6| p3_2_2==7| p3_2_2==8|p3_2_2==9|p3_2_2==97) & p3_6_2>0 & p3_6_2~=.)
replace potrasd = potrasd +p3_6_3 if ((p3_2_3==4| p3_2_3==5| p3_2_3==6| p3_2_3==7| p3_2_3==8|p3_2_3==9|p3_2_3==97) & p3_6_3>0 & p3_6_3~=.)
replace potrasd = potrasd +p3_6_4 if ((p3_2_4==4| p3_2_4==5| p3_2_4==6| p3_2_4==7| p3_2_4==8|p3_2_4==9|p3_2_4==97) & p3_6_4>0 & p3_6_4~=.)
replace potrasd = potrasd +p3_6_5 if ((p3_2_5==4| p3_2_5==5| p3_2_5==6| p3_2_5==7| p3_2_5==8|p3_2_5==9|p3_2_5==97) & p3_6_5>0 & p3_6_5~=.)
replace potrasd = potrasd +p3_6_6 if ((p3_2_6==4| p3_2_6==5| p3_2_6==6| p3_2_6==7| p3_2_6==8|p3_2_6==9|p3_2_6==97) & p3_6_6>0 & p3_6_6~=.)
replace potrasd = potrasd +p3_6_7 if ((p3_2_7==4| p3_2_7==5| p3_2_7==6| p3_2_7==7| p3_2_7==8|p3_2_7==9|p3_2_7==97) & p3_6_7>0 & p3_6_7~=.)
replace potrasd = potrasd +p3_6_8 if ((p3_2_8==4| p3_2_8==5| p3_2_8==6| p3_2_8==7| p3_2_8==8|p3_2_8==9|p3_2_8==97) & p3_6_8>0 & p3_6_8~=.)

*To obtain the value of the outstanding debt we generate a new variable;
gen vdeuda= dvivpral + deuoprop+ phipo+ pperso+ potrasd + ptmos_tarj


save "$output/`y'/wealth_`c'", replace 

** INCOME FLOWS **
use h_* p6_14_* p6_16_* p6_28a_* p6_28b_* p6_28c_* p6_28d_* p6_47_* p6_48a_* p6_49_* p6_50_* /// 
p6_52* p6_54 p6_56 p6_58 p6_60* p6_101_* p6_102_* p6_103_* p6_104_*  using "$path/`y'/`c'/section6_`y'_`c'.dta", clear 

drop p6_60es2 p6_601* p6_60c1 p6_60g p6_60h 
capture noisily drop p6_60iz* 


* Take the hh size 
merge 1:m h_`y' using temp1_`y'_`c'.dta, keepus(p1) nogen 

* Change the orders for the variables that has the hh_member in the wrong place * 
ds p6_14_* p6_16_* p6_49_* p6_50_* p6_101_* p6_102_* p6_103_* p6_104_*
rename (*_*_*_*) (*_*_*[4]_*[3]) 


* Reshape the selected variables from wide to long * 
ds p6_52* p6_54 p6_56 p6_58 p6_60* h_* p1, not 
local lisvr `r(varlist)'
disp "`lisvr'"
foreach h of local lisvr { 
	local stab = substr("`h'", 1, strrpos("`h'", "_"))
    local stabs `stabs' `stab'
} 
local stabs : list uniq stabs
disp "`stabs'" 
reshape long `stabs', i(h_`y') j(ind_id)
bysort h_`y' : drop if _n>p1 
rensfix _

* Create the sums for labour incomes across different existing jobs * 
egen tot_employee=rowtotal(p6_14_1 p6_14_2 p6_14_3 p6_16_1 p6_16_2 p6_16_3) 
/* We need to ask how to aggregate the self-employed income */ 
egen tot_pension=rowtotal(p6_49_1 p6_49_2 p6_49_3 p6_49_4) 

save "$output/`y'/income_`c'", replace 

use "$output/`y'/sdem_`c'", clear 
merge 1:1 h_`y' ind_id using "$output/`y'/lab_status_`c'", nogen 
merge 1:1 h_`y' ind_id using "$output/`y'/income_`c'", nogen 
merge m:1 h_`y' using "$output/`y'/wealth_`c'", nogen 


* Genereate the wealth variables 
*REAL ASSETS
gen actreales=0
replace actreales=actreales+np2_5 if (np2_5>0 & np2_5~=.)
replace actreales=actreales+otraspr if (otraspr>0 & otraspr~=.)
replace actreales=actreales+p2_84 if (p2_84>0 & p2_84~=.)
replace actreales=actreales+valhog if (valhog>0 & valhog~=.)

*FINANCIAL ASSETS;
gen actfinanc=0
replace actfinanc=actfinanc+p4_7_3 if (p4_7_3>0 & p4_7_3~=.)
replace actfinanc=actfinanc+p4_15 if (p4_15>0 & p4_15~=.)
replace actfinanc=actfinanc+p4_24 if (p4_24>0 & p4_24~=.)
replace actfinanc=actfinanc+p4_35 if (p4_35>0 & p4_35~=.)
replace actfinanc=actfinanc+allf if (allf>0 & allf~=.)
replace actfinanc=actfinanc+p4_43 if (p4_43>0 & p4_43~=.)
replace actfinanc=actfinanc+salcuentas if (salcuentas>0 & salcuentas~=.)
replace actfinanc=actfinanc+valor if (valor>0 & valor~=.)
replace actfinanc=actfinanc+valseg if (valseg>0 & valseg~=.)
replace actfinanc=actfinanc+odeuhog if (odeuhog>0 & odeuhog~=.)

gen riquezabr=0
replace riquezabr=riquezabr+actreales+actfinanc

gen riquezanet=riquezabr-vdeuda

*Rename the household number 
rename h_`y' h_id  
capture noisily drop h_2008 
capture noisily drop h_2011 
capture noisily drop h_2014

rensfix _
gen anno = `y'

save "$append/`c'/final_`y'_`c'.dta", replace 

	}	
} 




**********************************************************************************************************************************
** Now we have to append all years - by imputation file 

/* The vast majority of variables have the same name. The main changes refers to self-employment that move from individual to household level. 
There are some significant changes in the sector of firm where employed and sector of running business. To homogenize we need to convert in nace rev 1. 
Education as well will be also converted in isced97 to be coherent over time */ 

 
foreach c in imp1 imp2 imp3 imp4 imp5 { 
cd "$append/`c'" 
clear 
fs *.dta
	foreach file in `r(files)' { 
		append using `file', force 
		save "$append/`c'/final_appended_`c'.dta", replace	
	} 	
}

**********************************************************************************************************************************
/* We now generate the income flows variables in each imputation appended file and rename the wealth variables. In this way, we then keep 
for each of imp>=2 files only these needed variables to be merged in one single file */ 

clear 
foreach c in imp1 imp2 imp3 imp4 imp5 { 
	use "$append/`c'/final_appended_`c'.dta", clear 


egen p6_16tot_`c'= rowtotal(p6_16_1 p6_16_2 p6_16_3)  
egen p6_14_tot_`c'= rowtotal(p6_14_1 p6_14_2 p6_14_3) 

egen labincome_employee_`c'= rowtotal(p6_16tot_`c'  p6_14_tot_`c')

/* In the values for account interests (p4_8_1 to 3) there are lot of -9999 values. Although the manual doesn't specify any 
-9999 values, as if -1 and -2 are those not answering/doesn't know, we replace the -9999 with 0 values */ 
replace p4_8_1=0 if p4_8_1==-9999
replace p4_8_2=0 if p4_8_2==-9999
replace p4_8_3=0 if p4_8_3==-9999
replace p4_40=0 if p4_40==-9999

egen account_interests = rowtotal(p4_8_1 p4_8_2 p4_8_3) 
egen financial_income_`c' = rowtotal(p4_16 p4_25 p4_36 p4_40 account_interests)  

egen total_rentings= rowtotal(p2_43_1 p2_43_2 p2_43_3 p2_43_4) 
egen rental_income_`c' = rowtotal(p2_24  total_rentings) 

* 2002 and 2005
*1. set the net profits 
gen net_1=p6_381a_1 - p6_381b_1 
replace net_1=p6_381a_1 if p6_381b_1==. 

gen net_2=p6_381a_2 - p6_381b_2 
replace net_2=p6_381a_2 if p6_381b_2==. 

gen net_3=p6_381a_3 - p6_381b_3 
replace net_3=p6_381a_3 if p6_381b_3==. 

*2. Generate the profit income 
gen prof_1=net_1 if (p6_37_1==1 & p6_34_1>=2) & inlist(anno, 2002, 2005) 
replace prof_1=p6_382_1 if (p6_37_1==2 & p6_34_1>=2) & inlist(anno, 2002, 2005) 
replace prof_1=p6_383_1 if (p6_37_1==3 & p6_34_1>=2) & inlist(anno, 2002, 2005) 

gen prof_2=net_2 if (p6_37_2==1 & p6_34_2>=2) & inlist(anno, 2002, 2005) 
replace prof_2=p6_382_2 if (p6_37_2==2 & p6_34_2>=2) & inlist(anno, 2002, 2005) 
replace prof_2=p6_383_2 if (p6_37_2==3 & p6_34_2>=2) & inlist(anno, 2002, 2005) 

gen prof_3=net_3 if (p6_37_3==1 & p6_34_3>=2) & inlist(anno, 2002, 2005) 
replace prof_3=p6_382_3 if (p6_37_3==2 & p6_34_3>=2) & inlist(anno, 2002, 2005) 
replace prof_3=p6_383_3 if (p6_37_3==3 & p6_34_3>=2) & inlist(anno, 2002, 2005) 

*3. Doing the same for the self-employment i.e. p6_34==1 
gen self_1=net_1 if (p6_37_1==1 & p6_34_1==1) & inlist(anno, 2002, 2005) 
replace self_1=p6_382_1 if (p6_37_1==2 & p6_34_1==1) & inlist(anno, 2002, 2005) 
replace self_1=p6_383_1 if (p6_37_1==3 & p6_34_1==1) & inlist(anno, 2002, 2005) 

gen self_2=net_2 if (p6_37_2==1 & p6_34_2==1) & inlist(anno, 2002, 2005) 
replace self_2=p6_382_2 if (p6_37_2==2 & p6_34_2==1) & inlist(anno, 2002, 2005) 
replace self_2=p6_383_2 if (p6_37_2==3 & p6_34_2==1) & inlist(anno, 2002, 2005) 

gen self_3=net_3 if (p6_37_3==1 & p6_34_3==1) & inlist(anno, 2002, 2005) 
replace self_3=p6_382_3 if (p6_37_3==2 & p6_34_3==1) & inlist(anno, 2002, 2005) 
replace self_3=p6_383_3 if (p6_37_3==3 & p6_34_3==1) & inlist(anno, 2002, 2005) 



* For 2008 onwards 
/* Set the total fixed income in case of missing profits. This fixed income will be set as self-employed income 
if the firm-size is missing */  
egen tot_fix=rowtotal(p6_104_1 p6_104_2 p6_104_3) if inlist(anno, 2008,2011,2014,2017)


*1. set the net profits 
gen net08_1=p4_112_1 - p4_112b_1 if inlist(anno, 2008,2011,2014,2017)
replace net08_1=p4_112_1 if p4_112b_1==. & inlist(anno, 2008,2011,2014,2017)

gen net08_2=p4_112_2 - p4_112b_2 if inlist(anno, 2008,2011,2014,2017)
replace net08_2=p4_112_2 if p4_112b_2==. & inlist(anno, 2008,2011,2014,2017)

gen net08_3=p4_112_3 - p4_112b_3 if inlist(anno, 2008,2011,2014,2017)
replace net08_3=p4_112_3 if p4_112b_3==. & inlist(anno, 2008,2011,2014,2017)

gen net08_4=p4_112_4 - p4_112b_4 if inlist(anno, 2008,2011,2014,2017)
replace net08_4=p4_112_4 if p4_112b_4==. & inlist(anno, 2008,2011,2014,2017)

gen net08_5=p4_112_5 - p4_112b_5 if inlist(anno, 2008,2011,2014,2017)
replace net08_5=p4_112_5 if p4_112b_5==. & inlist(anno, 2008,2011,2014,2017)

gen net08_6=p4_112_6 - p4_112b_6 if inlist(anno, 2008,2011,2014,2017)
replace net08_6=p4_112_6 if p4_112b_6==. & inlist(anno, 2008,2011,2014,2017)

*2. Set the rowtotal for the fixed income components that may replace profits/self if missing p4_112  

gen prof_08_1=net08_1 if p4_109_1>=2 & inlist(anno, 2008,2011,2014,2017)
replace prof_08_1=tot_fix if (net08_1==. & tot_fix!=. & p4_109_1>=2 & p4_109_1!=.) & inlist(anno, 2008,2011,2014,2017) 
 
gen prof_08_2=net08_2 if p4_109_2>=2 & inlist(anno, 2008,2011,2014,2017)
replace prof_08_2=tot_fix if (net08_2==. & tot_fix!=. & p4_109_2>=2 & p4_109_2!=.) &  inlist(anno, 2008,2011,2014,2017)

gen prof_08_3=net08_3 if p4_109_3>=2 & inlist(anno, 2008,2011,2014,2017)
replace prof_08_3=tot_fix if  (net08_3==. & tot_fix!=. & p4_109_3>=2 & p4_109_3!=.) &  inlist(anno, 2008,2011,2014,2017)

gen prof_08_4=net08_4 if p4_109_4>=2 & inlist(anno, 2008,2011,2014,2017)
replace prof_08_4=tot_fix if (net08_4==. & tot_fix!=. & p4_109_4>=2 & p4_109_4!=.) &  inlist(anno, 2008,2011,2014,2017)

gen prof_08_5=net08_5 if p4_109_5>=2 &  inlist(anno, 2008,2011,2014,2017)
replace prof_08_5=tot_fix if (net08_5==. & tot_fix!=. & p4_109_5>=2 & p4_109_5!=.) &  inlist(anno, 2008,2011,2014,2017)

gen prof_08_6=net08_6 if p4_109_6>=2 &  inlist(anno, 2008,2011,2014,2017)
replace prof_08_6=tot_fix if (net08_6==. & tot_fix!=. & p4_109_6>=2 & p4_109_6!=.) &  inlist(anno, 2008,2011,2014,2017)

*3. doing the same for self-employed income
gen self_08_1=net08_1 if p4_109_1==1 &  inlist(anno, 2008,2011,2014,2017)
 
gen self_08_2=net08_2 if p4_109_2==1 &  inlist(anno, 2008,2011,2014,2017)

gen self_08_3=net08_3 if p4_109_3==1 &  inlist(anno, 2008,2011,2014,2017)

gen self_08_4=net08_4 if p4_109_4==1 &  inlist(anno, 2008,2011,2014,2017)

gen self_08_5=net08_5 if p4_109_5==1 &  inlist(anno, 2008,2011,2014,2017)

gen self_08_6=net08_6 if p4_109_6==1 &  inlist(anno, 2008,2011,2014,2017)

* Define the totals 
egen profits_`c'=rowtotal(prof_*) 
replace profits_`c'=. if (prof_1==. & prof_2==. & prof_3==.) & anno<2008
replace profits_`c'=. if (prof_08_1==. & prof_08_2==. & prof_08_3==. & prof_08_4==. & prof_08_5==. & prof_08_6==.) & anno>=2008

egen labincome_selfemp_`c'=rowtotal(self_*)
replace labincome_selfemp_`c'=. if (self_1==. & self_2==. & self_3==.) & anno<2008
replace labincome_selfemp_`c'=. if (self_08_1==. & self_08_2==. & self_08_3==. & self_08_4==. & self_08_5==. & self_08_6==.) & anno>=2008


** Rename the wealth variables ** 
gen net_wealth_`c'=riquezanet 
gen real_wealth_`c'=actreales 
gen financial_wealth_`c'=actfinanc
gen overall_debts_`c'=vdeuda 

save "$final_append/final_appended_`c'.dta", replace 
} 


**** Create a unique file from the different 5 imputations ****
*1. Keep the flows and stock variables in imp2-imp5
clear 
foreach c in imp2 imp3 imp4 imp5 { 
	use "$final_append/final_appended_`c'", replace 
	keep h_id ind_id anno profits_`c' labincome_selfemp_`c' financial_income_`c' labincome_employee_`c' rental_income_`c' net_wealth_`c' real_wealth_`c' financial_wealth_`c' overall_debts_`c' 
	save "$final_append/keep_`c'.dta", replace 
} 


*2. Now merge with imp1 
use "$final_append/final_appended_imp1.dta", clear 
merge 1:1 h_id ind_id anno using "$final_append/keep_imp2.dta", nogen 
merge 1:1 h_id ind_id anno using "$final_append/keep_imp3.dta", nogen 
merge 1:1 h_id ind_id anno using "$final_append/keep_imp4.dta", nogen 
merge 1:1 h_id ind_id anno using "$final_append/keep_imp5.dta", nogen 


save "$final_append/complete_spain.dta", replace 


******* LABELLING AND HOMOGENIZE THE POSSIBLE SOCIO-ECONOMIC VARIABLES *******
use "$final_append/complete_spain.dta", clear 
 
*Gender & age 
recode p1_1 (1 = 1 "Male") (2=0 "Female"), gen(gender) 
gen age=anno - p1_2b 

rename p1_2b birth_year 

rename hogarpanel hh_panel 

* HH size, marital status 
gen hh_size=p1 

recode p1_4 (1 = 2 "Single") (2 3 = 1 "Married/with partner") (4 5 = 3 "Separated/Divorced" ) (6 = 4 "Widowed"), gen(marital_status) 

*Education 
gen isced97=. 
replace isced97=1 if p1_5==1 
replace isced97=2 if inlist(p1_5, 2,3) 
replace isced97=3 if inlist(p1_5, 4,5)
replace isced97=4 if inlist(p1_5, 6,7,8)
replace isced97=5 if inlist(p1_5, 9, 10, 11, 1001, 1002) 
replace isced97=6 if p1_5==12 
label define isced97 1 "Isced 0" 2 "Isced 1" 3 "Isced 2" 4 "Isced 3" 5 "Isced 5" 6 "Isced 6" 
label values isced97 isced97 

* Social background 
gen father_occupation=p1_14_1 
gen mother_occupation=p1_14_2 

label define parent_occupation 1 "Managers & Legislators" 2 "Professionals" 3 "Technicians & associate professionals" 4 "Clerks" 5 "Service workers" 6 "Skilled agricultural workers" 7 "Craft & related trade workers" 8 "Plant and machine operators assembler" 9 "Elementary occupations" 10 "Armed forces" 11 "Home-care" 
label values father_occupation parent_occupation
label values mother_occupation parent_occupation

* Labour market variables 
recode p6_1 (1=1 "Employee") (2=2 "Self-employed") (4=3 "Retired") (3=4 "Unemployed") (6=5 "Students") (5 7 8 = 6 "Other inactives"), gen(emp_status) 

rename p6_13_1 contract_type_1 
rename p6_13_2 contract_type_2 
rename p6_13_3 contract_type_3 

label define contract_type 1 "Permanent" 2 "Temporary" 3 " Without contract" 4 "Other contracts" 
label values contract_type_* contract_type 

recode p6_11_* (1 = 0 ) (2=1 )
rename p6_11_1 ptime_1
rename p6_11_2 ptime_2
rename p6_11_3 ptime_3

label define ptime 0 "No" 1 "Yes" 
label values ptime_* ptime 

* The working hourse need to be combined for both self-employed and employees 
gen wk_hours_1= p6_12_1 
replace wk_hours_1=p6_33_1 if emp_status==2

gen wk_hours_2= p6_12_2 
replace wk_hours_2=p6_33_2 if emp_status==2

gen wk_hours_3= p6_12_3 
replace wk_hours_3=p6_33_3 if emp_status==2


gen occupation_es=p6_3 
label define occupation_es 1 "Managers & Legislators" 2 "Professionals" 3 "Technicians & associate professionals" 4 "Clerks" 5 "Service workers" 6 "Skilled agricultural workers" 7 "Craft & related trade workers" 8 "Plant and machine operators assembler" 9 "Elementary occupations" 10 "Armed forces"
label values occupation_es occupation_es

gen nace1=. 
replace nace1=1 if p6_4==1 
replace nace1=2 if p6_4==2 
replace nace1=3 if p6_4==3 
replace nace1=4 if (p6_4==4 & anno<=2008) | ((p6_4==4 | p6_4==5) & anno>=2011) 
replace nace1=5 if (p6_4==5 & anno<=2008) | (p6_4==6 & anno>=2011) 
replace nace1=6 if (p6_4==6 & anno<=2008) | (p6_4==7 & anno>=2011) 
replace nace1=7 if (p6_4==7 & anno<=2008) | (p6_4==9 & anno>=2011) 
replace nace1=8 if (p6_4==8 & anno<=2008) | ((p6_4==8 | p6_4==10) & anno>=2011) 
replace nace1=9 if (p6_4==9 & anno<=2008) | (p6_4==11 & anno>=2011) 
replace nace1=10 if (p6_4==10 & anno<=2008) | (inlist(p6_4, 12,13,14) & anno>=2011)
replace nace1=11 if (p6_4==11 & anno<=2008) | (p6_4==15 & anno>=2011) 
replace nace1=12 if (p6_4==12 & anno<=2008) | (p6_4==16 & anno>=2011)
replace nace1=13 if (p6_4==13 & anno<=2008) | (p6_4==17 & anno>=2011)
replace nace1=14 if (p6_4==14 & anno<=2008) | ((p6_4==18 | p6_4==19)  & anno>=2011)
replace nace1=15 if (p6_4==16 & anno<=2008) | (p6_4==20 & anno>=2011)
replace nace1=16 if (p6_4==16 & anno<=2008) | (p6_4==21 & anno>=2011)

label define nace1 1 "A+B - Agriculture & Fishery" 2 "C - Mining and quarrying" 3 "D- Manufacturing" 4 "E - Electricity, gas, water supply" 5 "F - Construction" 6 "G- Wholesale and retail" 7 "H- hotels & restaurants" ///
8 "I - Transport & comm." 9 "J- Financial intermediation" 10 "K - Real estate, renting and business activities" 11 "L - Public administration" 12 "M-Education" 13 "N - Health" 14 "O- Other personal services" 15 "P - Private hh services" 16 "Q- Extraterritorial organizations" 

label values nace1 nace1 

* Firm size for employees 
rename p6_20_1 firm_size_1
rename p6_20_2 firm_size_2
rename p6_20_3 firm_size_3

label define firm_size 1 "<10" 2 "10-19" 3 "20-99" 4 "100-499" 5 ">=500" 
label values firm_size_* firm_size 

/* Firm size for employers - need a unique variable over time. 
However, from 2005 to 2008 we passed from 3 to 6 firms asked */ 
gen num_employees_1=p6_34_1 
replace num_employees_1=p4_109_1 if p4_109_1!=. 

gen num_employees_2=p6_34_2
replace num_employees_2=p4_109_2 if p4_109_2!=. 

gen num_employees_3=p6_34_3 
replace num_employees_3=p4_109_3 if p4_109_3!=. 

rename p4_109_4 num_employees_4
rename p4_109_5 num_employees_5
rename p4_109_6 num_employees_6


rename p6_37_1 type_selfemp_1
rename p6_37_2 type_selfemp_2
rename p6_37_3 type_selfemp_3

label define type_selfemp 1 "Liberal professions, autonomous, single entrepreneur" 2 "Family business" 3 "Firm partner" 
label values type_selfemp_* type_selfemp 


* Tenant status 
gen tenant_status=p2_1 
replace tenant_status=5 if tenant_status==97 
label define tenant_status 1 "Owner" 2 "Tenant" 3 "Usufructuary/free rent" 5 "Other" , modify 
label values tenant_status tenant_status 


**** COLLAPSE AT THE HH LEVEL **** 
/* The variable cfdic defines whether the individual respondent is the household-head. However, we cannot collapse the set 
by the hh head to obtain values at the household level, as we have socio-demographic charactersitcs that cannot be collased as sum. Therefore,
we firstly create the total values of income within the hh - as the wealth values are already at hh level - and then keep only the hh==1. */ 

/* Computing the total of individual flows at the hh level - 
excluding: financial income, wealth, rental income, overall debts and profits after 2005 that are already at hh level */ 
*Profits
bysort h_id anno : egen hh_profits1=total(profits_imp1) if anno<=2005
replace hh_profits1=profits_imp1 if anno>=2008 
bysort h_id anno : egen hh_profits2=total(profits_imp2) if anno<=2005
replace hh_profits2=profits_imp2 if anno>=2008
bysort h_id anno : egen hh_profits3=total(profits_imp3) if anno<=2005
replace hh_profits3=profits_imp3 if anno>=2008
bysort h_id anno : egen hh_profits4=total(profits_imp4) if anno<=2005
replace hh_profits4=profits_imp4 if anno>=2008
bysort h_id anno : egen hh_profits5=total(profits_imp5) if anno<=2005
replace hh_profits5=profits_imp5 if anno>=2008 

*Employee income
bysort h_id anno : egen hh_labincome_emp1=total(labincome_employee_imp1) 
bysort h_id anno : egen hh_labincome_emp2=total(labincome_employee_imp2) 
bysort h_id anno : egen hh_labincome_emp3=total(labincome_employee_imp3) 
bysort h_id anno : egen hh_labincome_emp4=total(labincome_employee_imp4) 
bysort h_id anno : egen hh_labincome_emp5=total(labincome_employee_imp5) 

*Self-employed income
bysort h_id anno : egen hh_selfimp1=total(labincome_selfemp_imp1) if anno<=2005 
replace hh_selfimp1=labincome_selfemp_imp1 if anno>=2008
bysort h_id anno : egen hh_selfimp2=total(labincome_selfemp_imp2)
replace hh_selfimp2=labincome_selfemp_imp2 if anno>=2008
bysort h_id anno : egen hh_selfimp3=total(labincome_selfemp_imp3)
replace hh_selfimp3=labincome_selfemp_imp3 if anno>=2008
bysort h_id anno : egen hh_selfimp4=total(labincome_selfemp_imp4)
replace hh_selfimp4=labincome_selfemp_imp4 if anno>=2008
bysort h_id anno : egen hh_selfimp5=total(labincome_selfemp_imp5)
replace hh_selfimp5=labincome_selfemp_imp5 if anno>=2008


******* NEED TO GENERATE THE OVERALL MEAN ACROSS FLOWS AND STOCK 5 IMPUTATIONS ******* 
*Individual level
egen profits=rowmean(profits_*) 
egen labincome_employee=rowmean(labincome_employee_*) 
egen labincome_selfemp=rowmean(labincome_selfemp_*)


*Household level 
egen hh_profit=rowmean(hh_profits*) 
egen hh_inc_employee=rowmean(hh_labincome_emp*)
egen hh_inc_selfemp=rowmean(hh_selfimp*)
egen financial_income=rowmean(financial_income_*)
egen rental_income=rowmean(rental_income_*)
egen net_wealth=rowmean(net_wealth_*)
egen real_wealth=rowmean(real_wealth_*)
egen financial_wealth=rowmean(financial_wealth_*)
egen overall_debts=rowmean(overall_debts_*) 


** We need to multiply by 12 the labour income and rental income 
gen hh_employee12=hh_inc_employee*12 
gen rental_income12=rental_income*12 

* Set to missing the profits that are 0 but refers to individual without profits 
replace hh_profit=. if (prof_1==. & prof_2==. & prof_3==.) & anno<2008
replace hh_inc_selfemp=. if (self_1==. & self_2==. & self_3==.) & anno<2008 

* Keep only the householder 
keep if p1_3==1 

/* tab anno 
The total number of observations at hh level exactly correspond to the provided hh-level files */ 

save "$final_append/hh_spain_final.dta", replace 


******* IDENTIFY THE IMPUTED VALUES AND SET THEM TO MISSING ******* 
/* If the 5 imputations are all different, then the value has been imputed. On the contrary, 
for identical values across the 5 imputations, there wasn't any imputation */    
egen flag_profits=diff(hh_profits*) 
egen flag_labincome_employee=diff(hh_labincome_emp*) 
egen flag_selfincome=diff(hh_selfimp*) 
egen flag_financial=diff(financial_income_*) 
egen flag_rental=diff(rental_income_*)
egen flag_netwealth=diff(net_wealth_*)
egen flag_realwealth=diff(real_wealth_*)
egen flag_financialwealth=diff(financial_wealth_*)
egen flag_debts=diff(overall_debts_*) 

* Set to missing those elements with corresponding flag==1 
replace hh_profit=. if flag_profits==1 
replace hh_inc_employee=. if flag_labincome_employee==1 
replace hh_inc_selfemp=. if flag_selfincome==1 
replace financial_income=. if flag_financial==1 
replace rental_income=. if flag_rental==1 
replace net_wealth=. if flag_netwealth==1 
replace real_wealth=. if flag_realwealth==1 
replace financial_wealth=. if flag_financialwealth==1 
replace overall_debts=. if flag_debts==1 
replace hh_employee12=. if hh_inc_employee==. 
replace rental_income12=. if rental_income==. 

save "$final_append/hh_spain_noimputation_final.dta", replace 



******* DESCRIPTIVES *******
use "$final_append/hh_spain_final.dta", clear 

* Create the deflator 
gen deflator=0.7586 if anno==2002 
replace deflator=0.8333 if anno==2005
replace deflator=0.9241 if anno==2008
replace deflator=0.9694 if anno==2011
replace deflator=1.0063 if anno==2014
replace deflator=1.013 if anno==2017



* Applying deflator and log-transformation 
foreach v of var hh_employee12 hh_inc_selfemp hh_profit financial_income rental_income12 net_wealth real_wealth financial_wealth overall_debts { 
	gen defl_`v'=`v'/deflator	
	gen log_`v'=ln(defl_`v')
} 


* Computing the share of hh with values greater than 0 for all income and wealth components 
bysort anno : egen tot_hh=count(h_id) 
foreach v of var hh_employee12 hh_inc_selfemp hh_profit financial_income rental_income12 net_wealth real_wealth financial_wealth overall_debts { 
	bysort anno : egen `v'_no0=count(`v') if `v'>0 & `v'!=. 
	gen pp_`v'= (`v'_no0/tot_hh)*100
} 



/* 
levelsof anno, local(a) 
foreach v of var defl_* { 
	foreach anno in `a' { 
		sum `v' if anno== `anno' [aw=facine3], det 
		}
} 
*/ 

estpost tabstat defl_hh_employee12 defl_hh_inc_selfemp defl_hh_profit defl_net_wealth [aw=facine3] , by(anno) stat(mean sd min max) not 
esttab using summaries.csv, cells("defl_hh_employee12 defl_hh_inc_selfemp defl_hh_profit defl_net_wealth") 

* Doing it with values greater than 0 
foreach v of var defl_hh_employee12 defl_hh_inc_selfemp defl_hh_profit defl_net_wealth { 
	estpost tabstat `v' if `v'>0 [aw=facine3], by(anno) stat(mean sd min max) not 
	esttab using summaries_no0.csv, cells("mean sd min max") append
} 


* 2. Starts with plotting the distribution at different points in time 
* Employee income
twoway kdensity log_hh_employee12 if anno==2002 [aw=facine3], lpattern(dash) || kdensity log_hh_employee12 if anno==2005 [aw=facine3] || kdensity log_hh_employee12 if anno==2008 [aw=facine3] || kdensity log_hh_employee12 if anno==2011 [aw=facine3] || kdensity log_hh_employee12 if anno==2014 [aw=facine3] || kdensity log_hh_employee12 if anno==2017 [aw=facine3] ///
, legend(label(1 "2002") label(2 "2005") label(3 "2008") label(4 "2011") label(5 "2014") label(6 "2017")) ytitle("Density") xtitle("(log) employee income") 
graph export "$graphs/employee_income.png", replace 

pshare log_hh_employee12 if anno==2002 [pw=facine3], nq(10)  
pshare log_hh_employee12 if anno==2017 [pw=facine3], nq(10)  

* The bottom 10 slightly decreased its share, while the top 10% is almost constant as the median 

* Self-employed income 
twoway kdensity log_hh_inc_selfemp if anno==2002 [aw=facine3], lpattern(dash) || kdensity log_hh_inc_selfemp if anno==2005 [aw=facine3] ||  kdensity log_hh_inc_selfemp if anno==2008 [aw=facine3] ||  kdensity log_hh_inc_selfemp if anno==2011 [aw=facine3] || kdensity log_hh_inc_selfemp if anno==2014 [aw=facine3] || kdensity log_hh_inc_selfemp if anno==2017 [aw=facine3] ///
, legend(label(1 "2002") label(2 "2005") label(3 "2008") label(4 "2011") label(5 "2014") label(6 "2017")) ytitle("Density") xtitle("(log) Self-employ income") 
graph export "$graphs/selfemployed_income.png", replace 


* Negative profits are only 390 observations. Therefore, I use the log-transformation for its plot. 
twoway kdensity log_hh_profit if anno==2002 [aw=facine3], lpattern(dash) || kdensity log_hh_profit if anno==2005 [aw=facine3] ||  kdensity log_hh_profit if anno==2008 [aw=facine3] ||  kdensity log_hh_profit if anno==2011 [aw=facine3] || kdensity log_hh_profit if anno==2014 [aw=facine3] || kdensity log_hh_profit if anno==2017 [aw=facine3] ///
, legend(label(1 "2002") label(2 "2005") label(3 "2008") label(4 "2011") label(5 "2014") label(6 "2017")) ytitle("Density") xtitle("(log) Profits") 
graph export "$graphs/profits.png", replace 

/* the 2017 seems to have some "weird" pattern compared to all other years. Indeed, it has the lowest mean (excluding the 0) compared to all other years. */ 

* Financial income 
twoway kdensity log_financial_income if anno==2002 [aw=facine3], lpattern(dash) || kdensity log_financial_income if anno==2005 [aw=facine3] ||  kdensity log_financial_income if anno==2008 [aw=facine3] ||  kdensity log_financial_income if anno==2011 [aw=facine3] || kdensity log_financial_income if anno==2014 [aw=facine3] || kdensity log_financial_income if anno==2017 [aw=facine3] ///
, legend(label(1 "2002") label(2 "2005") label(3 "2008") label(4 "2011") label(5 "2014") label(6 "2017")) ytitle("Density") xtitle("(log) Financial income") 
graph export "$graphs/financial_income.png", replace 


* Rental income 
twoway kdensity log_rental_income12 if anno==2002 [aw=facine3], lpattern(dash) || kdensity log_rental_income12 if anno==2005 [aw=facine3] ||  kdensity log_rental_income12 if anno==2008 [aw=facine3] ||  kdensity log_rental_income12 if anno==2011 [aw=facine3] || kdensity log_rental_income12 if anno==2014 [aw=facine3] || kdensity log_rental_income12 if anno==2017 [aw=facine3] ///
, legend(label(1 "2002") label(2 "2005") label(3 "2008") label(4 "2011") label(5 "2014") label(6 "2017")) ytitle("Density") xtitle("(log) Rental income") 
graph export "$graphs/rental_income.png", replace 


* Net wealth 
pshare net_wealth if anno==2002 [pw=facine3], nq(10) 
pshare net_wealth if anno==2017 [pw=facine3], nq(10) 

* Wealth accumulates strongly at the top: the top 90th is the only one gaining in wealth shares 

* Net wealth with log-transformation
twoway kdensity log_net_wealth if anno==2002 [aw=facine3], lpattern(dash) || kdensity log_net_wealth if anno==2005  [aw=facine3] ||  kdensity log_net_wealth if anno==2008  [aw=facine3] ||  kdensity log_net_wealth if anno==2011 [aw=facine3] || kdensity log_net_wealth if anno==2014 [aw=facine3] || kdensity log_net_wealth if anno==2017  [aw=facine3] ///
, legend(label(1 "2002") label(2 "2005") label(3 "2008") label(4 "2011") label(5 "2014") label(6 "2017")) ytitle("Density") xtitle("(log) Net wealth") 
graph export "$graphs/net_wealth.png", replace 

/* There are some deviations in 2005, which is the year with outstanding mean. */ 


***** Rename variables for append to complete set ****** 
rename hh_employee12 empl_income_es 
rename hh_inc_selfemp selfemp_income_es 
rename hh_profit profit_es 
rename financial_income financial_income_es 
rename rental_income12 rental_income_es 

rename h_id hh_id 
rename anno year 

recode p6_10_1 (1=1 "Yes") (2=0 "No"), gen(main_occupation_1)  
recode p6_10_2 (1=1 "Yes") (2=0 "No"), gen(main_occupation_2)  
recode p6_10_3 (1=1 "Yes") (2=0 "No"), gen(main_occupation_3)  


keep hh_id year gender age birth_year hh_size marital_status father_occupation mother_occupation isced97 tenant_status hh_panel emp_status occupation_es nace1 main_occupation_* type_selfemp_* contract_type_* ptime_* firm_size_* num_employees_* empl_income_es /// 
selfemp_income_es financial_income_es rental_income_es profit_es net_wealth real_wealth financial_wealth overall_debts facine3 pesopan_1 pesopan_2 deflator 

gen country="Spain" 

* Reset to 0 the profits and self-employed income that are missing 
replace profit_es=0 if profit_es==. 
replace selfemp_income_es=0 if selfemp_income_es==. 

save "$final_append/ready_to_append_es.dta", replace  







