import pandas as pd
import numpy as np
import csv
from datetime import datetime, timedelta


bsak = pd.read_csv("BSAK_BKPF_AltColTitles.csv",dtype={
       'Company_Code':np.str,'Account_Number_of_Vendor_or_Creditor':np.str})
t001 = pd.read_csv("T001_AltColTitles.csv",dtype={'Company_Code':np.str})
lfa1 = pd.read_csv("LFA1_AltColTitles.csv",dtype={'Account_Number_of_Vendor_or_Creditor':np.str})

# join bsak,lfa1,t001 using the ERD diagram of AP-Invoice File and get the AP-Invoice File data
bsak_lfa1 = pd.merge(bsak, lfa1, on = 'Account_Number_of_Vendor_or_Creditor', how='left')
invoice_file = pd.merge(bsak_lfa1, t001, 
	left_on=['Company_Code'], right_on=['Company_Code'], how='left')

# For the "EKPO_EKKO_AltColTitles2.csv", some rows have wrong number of columns because some closed cells are combined as one cell in the original data. 
# Here we will split them into correct columns and shift columns into correct positions.
# load the EKPO_EKKO_AltColTitles2.csv data
lines = list(csv.reader(open('EKPO_EKKO_AltColTitles.csv', encoding='latin1')))

# split wrong number of columns into correct columns and shift columns into correct positions
for i in range(len(lines)):
    l = lines[i]
    if len(l) > 29: # for len(l) = 30, 31, 32 or 33
        l[6:29] = l[len(l)-23:]
        lines[i] = l[0:29]
    elif len(l) == 28: # 14575 rows
        tmp1 = l[6:]
        tmp2 = l[5].split(',')
        lines[i] = l[0:5] + [tmp2[0][:-1]] + [tmp2[1][:-1]] + tmp1
    elif len(l) == 27: # 11 rows
        tmp1 = l[6:]
        tmp2 = l[5].split(',')
        lines[i] = l[0:5] + [tmp2[0][:-1]] + [''] + [tmp2[2][:-1]] + tmp1
# convert to dataframe
ekpo = pd.DataFrame(lines[1:], columns = lines[0])

# load other two tables of AP-Inv PO Line File
rseg = pd.read_csv("RSEG_RBKP_AltColTitles.csv",dtype={
       'Purchasing_Document_Number':np.str,'Item_Number_of_Purchasing_Document':np.str})
ekbe = pd.read_csv("EKBE_AltColTitles.csv",dtype={
       "Vendor_Account_Number":np.str,"Reference_Document_Number":np.str,
    'Purchasing_Document_Number':np.str,'Item_Number_of_Purchasing_Document':np.str},engine='python')

# join rseg, ekpo, ekbe using the ERD diagram of AP-Inv PO Line File and get the AP-Inv PO Line File data
rseg_ekpo = pd.merge(rseg, ekpo, 
	on=['Purchasing_Document_Number','Item_Number_of_Purchasing_Document'], 
	how='outer')
po_file = pd.merge(rseg_ekpo, ekbe, 
	left_on=['Purchasing_Document_Number','Item_Number_of_Purchasing_Document'], 
	right_on=['Purchasing_Document_Number','Item_Number_of_Purchasing_Document'], 
	how='outer')


# Task 1: Vendor Paid By Both Purchase Order And Sundry Expense
# exclude Ducument_Type of ZP and KZ
invoice_file_set = invoice_file[(invoice_file['Document_Type'] != 'ZP') & 
								(invoice_file['Document_Type'] != 'KZ')]

# get instances that Reference_Document_Number in both invoice_file and po_file(paid by po)						
both_in_invoice_po = pd.merge(invoice_file_set, 
	po_file[["Vendor_Account_Number","Reference_Document_Number"]], 
	left_on=["Account_Number_of_Vendor_or_Creditor","Reference_Document_Number"], 
	right_on=["Vendor_Account_Number","Reference_Document_Number"], 
	how='inner')

# drop Vendor_Account_Number which is the same as Account_Number_of_Vendor_or_Creditor
both_in_invoice_po = both_in_invoice_po.drop("Vendor_Account_Number",axis=1)
# drop duplicates
both_in_invoice_po.drop_duplicates(inplace=True)

# get all the invoices of all the vendors in the both_in_invoice_po
all_vendor_invoice = invoice_file_set[
						invoice_file_set["Account_Number_of_Vendor_or_Creditor"]\
						.isin(both_in_invoice_po["Account_Number_of_Vendor_or_Creditor"])]

# exclude the data of both_in_invoice_po from all_vendor_invoice 
# get sundy expenses for those vendors paid by both po and sundry expense		
notin_po_invoice = all_vendor_invoice.merge(both_in_invoice_po, how='outer', indicator=True)\
						.query('_merge == "left_only"').drop('_merge', 1)

# both_sundry is the sundy expenses for those vendors paid by both po and sundry expense
both_sundry = notin_po_invoice

# only_sundry is the sundy expenses for those vendors paid by only sundry expense
only_sundry = invoice_file_set.merge(
    all_vendor_invoice, how='outer', indicator=True
).query('_merge == "left_only"').drop('_merge', 1)

# both_po is the po invoices for those vendors paid by both po and sundry expense
both_po = both_in_invoice_po[both_in_invoice_po["Account_Number_of_Vendor_or_Creditor"]\
                              .isin(notin_po_invoice["Account_Number_of_Vendor_or_Creditor"])]

# only_po is the po invoices for those vendors paid by only po 
only_po = both_in_invoice_po[~both_in_invoice_po["Account_Number_of_Vendor_or_Creditor"]\
                               .isin(notin_po_invoice["Account_Number_of_Vendor_or_Creditor"])]

# create a new column "type" to show the type of each invoice in the four dataframe 
both_sundry['type'] = 'both_sundry'
only_sundry['type'] = 'only_sundry'
both_po['type'] = 'both_po'
only_po['type'] = 'only_po'

# concat the four dataframe to get the whole invoice_file_set
# using this data to better visual in Tableau
invoice_file_type = pd.concat([both_sundry, only_sundry, both_po, only_po])
invoice_file_type = invoice_file_type.reset_index(drop=True)

# output the result table
# invoice_file_type.to_csv("invoice_file_type.csv")


# Task 2: Invoice Frequency
# for each Account_Number_of_Vendor_or_Creditor, identify the counts of distinct Accounting_Document_Number
invoice_frequency = invoice_file_set.groupby('Account_Number_of_Vendor_or_Creditor')\
	                     ['Accounting_Document_Number'].nunique().reset_index(name="Invoice_Frequency")


# Task 3: Recurring Sundry Expense Review
# get all the sundry expense in invoice_file_set
notin_po_allinvoice  = invoice_file_set.merge(both_in_invoice_po, 
	how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', 1)

# For each Accounting_Document_Number of each vendor, get sum amounts both in local and document currency
sum_amount= notin_po_allinvoice.groupby(['Account_Number_of_Vendor_or_Creditor',
										'Accounting_Document_Number',
										'Document_Date_in_Document'])\
[['Amount_in_Local_Currency','Amount_in_Document_Currency']].sum().reset_index()

# keep the same amount of the same vendor that appears more than 1 time (recurring amount)
recur_amount = sum_amount[sum_amount.duplicated(['Account_Number_of_Vendor_or_Creditor', 
	'Amount_in_Document_Currency'], keep = False)]

# change Document_Date to datetime type and then get the month, the week of the year from the date
recur_amount['date'] = pd.to_datetime(recur_amount['Document_Date_in_Document'])
recur_amount['month'] = recur_amount['date'].dt.month
recur_amount['week'] = recur_amount['date'].dt.strftime('%W').astype(int)

# get the counts for the same amount of the same vendor
recur_times = recur_amount.groupby(['Account_Number_of_Vendor_or_Creditor',
	                              'Amount_in_Document_Currency']).size().reset_index(name="times")

# get the list of all the months which the same amount of the same vendor appears in 
month_list = recur_amount.groupby(['Account_Number_of_Vendor_or_Creditor',
	                'Amount_in_Document_Currency'])['month'].apply(list).reset_index(name="month_lists")

# get how many months the same amount of the same vendor appears in (not consecutive month) 
recur_month = recur_amount.groupby(['Account_Number_of_Vendor_or_Creditor',
	                              'Amount_in_Document_Currency']).month.nunique().reset_index()

# merge the month_list and month_times as "recur_month"
recur_month = pd.merge(month_list,recur_month, left_on=['Account_Number_of_Vendor_or_Creditor',
	                   'Amount_in_Document_Currency'], right_on=['Account_Number_of_Vendor_or_Creditor',
	                   'Amount_in_Document_Currency'], how='inner')

# get the list of all the weeks which the same amount of the same vendor appears in 
week_lists = recur_amount.groupby(['Account_Number_of_Vendor_or_Creditor',
	                   'Amount_in_Document_Currency'])['week'].apply(list).reset_index(name="week_lists")

# define a function count_consec to calculate consecutive numbers
def count_consec(lst):
    consec = [1]
    for x, y in zip(lst, lst[1:]):
        if x == y - 1:
            consec[-1] += 1
        else:
            consec.append(1)
    return consec

# get the consecutive weeks.\ 
# For example: if week_lists = [1,4,5,6,7,9,19,21,22,23,24], then the result of week_times = [1,4,1,1,4].\
# As in, there is one singular number, followed by 4 consecutive numbers, followed by one\
# singular number, followed by one singular number, followed by 4 consecutive numbers.
week_times = []
for i, row in week_lists.iterrows():
    week_times.append(count_consec(row['week_lists']))

week_lists['week_times'] = week_times
recur_week = week_list

# merge recur_amount, recur_month, recur_week and recur_times
recur_invoice = pd.merge(recur_amount,recur_month, left_on=['Account_Number_of_Vendor_or_Creditor',
	'Amount_in_Document_Currency'], right_on=['Account_Number_of_Vendor_or_Creditor',
	'Amount_in_Document_Currency'], how='inner')

recur_invoice= pd.merge(recur_invoice,recur_week, left_on=['Account_Number_of_Vendor_or_Creditor',
	'Amount_in_Document_Currency'], right_on=['Account_Number_of_Vendor_or_Creditor',
	'Amount_in_Document_Currency'], how='inner')

recur_invoice= pd.merge(recur_invoice,recur_times, left_on=['Account_Number_of_Vendor_or_Creditor',
	'Amount_in_Document_Currency'], right_on=['Account_Number_of_Vendor_or_Creditor',
	'Amount_in_Document_Currency'], how='inner')

# find the max consecutive weeks number 
recur_invoice["max_consecutive_weeks"] = recur_invoice["week_times"]\
                                  .apply(lambda x: max(map(int, x[1:-1].split(','))))

# output the result table
# recur_invoice.to_csv("recur_invoice.csv")


# Task 4: Invoices For One-Time Vendors
# get the vendors with only one distinct Accounting_Document_Number (one-time vendor)
one_time_vendors = invoice_frequency[invoice_frequency['Invoice_Frequency'] == 1]

# some of key output results fields
z = invoice_file_set.loc[:,['Company_Code','Account_Number_of_Vendor_or_Creditor',
                            'Debit_Credit_Indicator','Amount_in_Local_Currency','Currency_Key_y',
                            'Amount_in_Document_Currency','Currency_Key_x',
                            'Accounting_Document_Number','Document_Date_in_Document',
                            'Reference_Document_Number']]
z.drop_duplicates(inplace=True)

# merge to get the key output results fields
one_time_vendors_invoices = pd.merge(one_time_vendors, z, on = 'Account_Number_of_Vendor_or_Creditor',
	                                 how='left')

# get the correct order and names of key output results fields
one_time_vendors_invoices = one_time_vendors_invoices[['Company_Code',
                           'Account_Number_of_Vendor_or_Creditor','Debit_Credit_Indicator',
                           'Amount_in_Local_Currency','Currency_Key_y','Amount_in_Document_Currency',
                           'Currency_Key_x','Accounting_Document_Number','Document_Date_in_Document',
                           'Reference_Document_Number']]
one_time_vendors_invoices = one_time_vendors_invoices.rename(columns = {
	                'Currency_Key_y':'Local_Currency_Key','Currency_Key_x':'Document_Currency_Key'})

# output the result table
# one_time_vendors_invoices.to_csv("one_time_vendors_invoices.csv")


# Task 5: Outstanding Debtors Greater Than 60 Days
# load three tables of AR-Accounts_Rec_Listing File
bsid_bkpf = pd.read_csv("BSID_BKPF_AltColTitles.csv",
	dtype={'Company_Code':np.str,'Customer_Number':np.str},encoding='latin-1')
knkk = pd.read_csv("KNKK_AltColTitles.csv",dtype={'Customer_Number':np.str})
t001 = pd.read_csv("T001_AltColTitles.csv",dtype={'Company_Code':np.str})

# join bsid_bkpf,knkk,t001 using the ERD diagram of AR-Accounts_Rec_Listing File and get the AR-Accounts_Rec_Listing File data
bsid_bkpf_knkk = pd.merge(bsid_bkpf, knkk, 
	left_on=['Customer_Number'], right_on=['Customer_Number'], how='left')
Accounts_Rec_Listing = pd.merge(bsid_bkpf_knkk, t001, 
	left_on=['Company_Code'], right_on=['Company_Code'], how='left')

# For each Reference_Document_Number of each Customer_Number, calculate the amount of Debit and amount of Credit
a = Accounts_Rec_Listing[Accounts_Rec_Listing['Debit_Credit_Indicator'] == 'S']
b = Accounts_Rec_Listing[Accounts_Rec_Listing['Debit_Credit_Indicator'] == 'H']
a_group_amount = a.groupby(['Customer_Number','Reference_Document_Number'])\
				['Amount_in_Local_Currency'].sum().reset_index()
b_group_amount = b.groupby(['Customer_Number','Reference_Document_Number'])\
				['Amount_in_Local_Currency'].sum().reset_index()
total_group_amount = pd.merge(a_group_amount,b_group_amount,
	left_on =['Customer_Number','Reference_Document_Number'], 
	right_on = ['Customer_Number','Reference_Document_Number'], 
	how='outer')
total_group_amount=total_group_amount.rename(columns = 
	{'Amount_in_Local_Currency_x':'Amount_S','Amount_in_Local_Currency_y':'Amount_H'})

# Using amount of Debit minus amount of Credit to get the balance 
total_group_amount['Amount_H'].fillna(0, inplace=True)
total_group_amount['Amount_S'].fillna(0, inplace=True)
total_group_amount['Balance'] = total_group_amount['Amount_S'] - total_group_amount['Amount_H']
balance_amount = pd.merge(total_group_amount,Accounts_Rec_Listing, 
	on = ['Customer_Number','Reference_Document_Number'], how = 'left')
balance_amount = balance_amount[['Customer_Number','Reference_Document_Number',
						'Amount_S','Amount_H','Balance','Document_Date_in_Document']]
balance_amount.drop_duplicates(inplace=True)

# get the minimum Document_Date for each Reference_Document_Number of each Customer_Number 
y = balance_amount.groupby(['Customer_Number','Reference_Document_Number'])\
				['Document_Date_in_Document'].min().reset_index()
balance_amount = pd.merge(balance_amount, y, 
	on = ['Customer_Number','Reference_Document_Number','Document_Date_in_Document'],how='inner')

# For Balance <= 0, outstanding day is zero; 
# For Balance > 0,using the system date minus minimum Document_Date to calculate the outstanding day  
for i in range(len(balance_amount)):
	if balance_amount.loc[i,'Balance'] <= 0:
		balance_amount.loc[i,'outstanding_days'] = int(0)
	elif balance_amount.loc[i,'Balance'] > 0:
		balance_amount.loc[i,'outstanding_days'] = \
       		int((pd.to_datetime(datetime.today().strftime('%Y-%m-%d')) - 
       			pd.to_datetime(balance_amount.loc[i,'Document_Date_in_Document'])).days)
balance_amount['outstanding_days'] = balance_amount['outstanding_days'].astype('int64')

# using the outstanding_days to find the type of '60-90', '90-120', '> 120', '< 60' or 'not outstanding'
for i in range(len(balance_amount)):
    if balance_amount.loc[i,'outstanding_days'] >= 60 and balance_amount.loc[i,'outstanding_days'] <= 90:
        balance_amount.loc[i,'type'] = '60-90'
    elif balance_amount.loc[i,'outstanding_days'] > 90 and balance_amount.loc[i,'outstanding_days'] <= 120:
        balance_amount.loc[i,'type'] = '90-120'
    elif balance_amount.loc[i,'outstanding_days'] > 120:
        balance_amount.loc[i,'type'] = '> 120'
    elif balance_amount.loc[i,'outstanding_days'] < 60 and balance_amount.loc[i,'outstanding_days'] > 0:
        balance_amount.loc[i,'type'] = '< 60'
    elif balance_amount.loc[i,'outstanding_days'] == 0:
        balance_amount.loc[i,'type'] = 'not outstanding'

# merge with knkk table to get the Customer_s_credit_limit
balance_amount = pd.merge(balance_amount, knkk, on = 'Customer_Number', how= 'left')

# compare balance with Customer_s_credit_limit to get the information of above or below limit
for i in range(len(balance_amount)):
    if balance_amount.loc[i,'Balance'] > balance_amount.loc[i, 'Customer_s_credit_limit']:
        balance_amount.loc[i,'above_limit'] = 'above limit'
    elif balance_amount.loc[i,'Balance'] <= balance_amount.loc[i, 'Customer_s_credit_limit']:
        balance_amount.loc[i,'above_limit'] = 'below limit'
    else:
        balance_amount.loc[i,'above_limit'] = 'no limit'

# output the result table
# balance_amount.to_csv("balance_amount.csv")


