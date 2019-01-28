"""
This code_for_tasks.py code file is to get the key output results of five tasks 
in "Fall 2018 Capstone Analytics". 
"""

import pandas as pd
import numpy as np
import csv
from datetime import datetime, timedelta

def merge_data():
	"""
	this function merge_data() is to get the AP-Invoice File and AP-Inv PO Line File
	task 1-4 all use this two files
	"""
	# load three tables of AP-Invoice File
	print('Start reading tables...')
	bsak = pd.read_csv("BSAK_BKPF_AltColTitles.csv",dtype={
	       'Company_Code':np.str,'Account_Number_of_Vendor_or_Creditor':np.str})
	t001 = pd.read_csv("T001_AltColTitles.csv",dtype={'Company_Code':np.str})
	lfa1 = pd.read_csv("LFA1_AltColTitles.csv",dtype={'Account_Number_of_Vendor_or_Creditor':np.str})
	
	# join bsak,lfa1,t001 using the ERD diagram of AP-Invoice File and get the AP-Invoice File data
	bsak_lfa1 = pd.merge(bsak, lfa1, on = 'Account_Number_of_Vendor_or_Creditor', how='left')
	invoice_file = pd.merge(bsak_lfa1, t001, 
		left_on=['Company_Code'], right_on=['Company_Code'], how='left')
	print('Invoice_file is ready.')

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
	print('PO_file is ready.')
	return invoice_file, po_file


# Task 1: Vendor Paid By Both Purchase Order And Sundry Expense
def task1(invoice_file, po_file):
	"""
	This function task1() is to get the key output results of Task 1
	"""
	# exclude Ducument_Type of ZP and KZ
	print('Start tast 1...')
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

	# get all the instances of all the vendors in the both_in_invoice_po
	all_vendor_invoice = invoice_file_set[
							invoice_file_set["Account_Number_of_Vendor_or_Creditor"]\
							.isin(both_in_invoice_po["Account_Number_of_Vendor_or_Creditor"])]

	# exclude the data of both_in_invoice_po from all_vendor_invoice 
	# get sundy expenses for those vendors paid by both po and sundry expense		
	notin_po_invoice = all_vendor_invoice.merge(both_in_invoice_po, how='outer', indicator=True)\
							.query('_merge == "left_only"').drop('_merge', 1)

	# get all the instances for those vendors paid by both po and sundry expense
	vendor_both_po_sundry = all_vendor_invoice[
								all_vendor_invoice["Account_Number_of_Vendor_or_Creditor"].\
								isin(notin_po_invoice["Account_Number_of_Vendor_or_Creditor"])]

	# key output results fields
	out_columns = ['Company_Code','Name_of_Company_Code_or_Company','Account_Number_of_Vendor_or_Creditor',
	                'Amount_in_Local_Currency','Currency_Key_y','Amount_in_Document_Currency', 
	                'Currency_Key_x', 'Accounting_Document_Number','Document_Date_in_Document',
					'Document_Type','Reference_Document_Number']
	outfile = vendor_both_po_sundry.loc[:, out_columns].reset_index(drop=True)
	outfile = outfile.rename(columns = {'Currency_Key_y':'Local_Currency_Key',
		                                'Currency_Key_x':'Document_Currency_Key'})

	# output the result table
	outfile.to_csv("task1.csv")
	print('Task 1 is finished. Output is ready.')
	return invoice_file_set, both_in_invoice_po


# Task 2: Invoice Frequency
def task2(invoice_file_set):
	"""
	This function task2() is to get the key output results of Task 2
	The input of task2() function is the output invoice_file_set of task1
	"""
	# for each Account_Number_of_Vendor_or_Creditor, identify the counts of distinct Accounting_Document_Number
	print('Start task 2...')
	invoice_frequency = invoice_file_set.groupby('Account_Number_of_Vendor_or_Creditor')\
	                     ['Accounting_Document_Number'].nunique().reset_index(name="Invoice_Frequency")
	invoice_frequency.to_csv("task2.csv")
	print('Task 2 is finished. Output is ready.')
	return invoice_frequency


# Task 3: Recurring Sundry Expense Review
def task3(invoice_file_set, both_in_invoice_po):
	"""
	This function task3() is to get the key output results of Task 3 
	The input of task3() function is the output invoice_file_set and both_in_invoice_po of task1
	"""
	# get all the sundry expense in invoice_file_set
	print('Start task 3...')
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

	# some of key output results fields
	a = notin_po_allinvoice[['Company_Code','Name_of_Company_Code_or_Company',
		'Account_Number_of_Vendor_or_Creditor', 'Accounting_Document_Number',
		'Document_Type','Reference_Document_Number','Currency_Key_x','Currency_Key_y']]

	# merge to get the key output results fields
	recur_invoice = pd.merge(recur_amount, a, 
		left_on=['Account_Number_of_Vendor_or_Creditor','Accounting_Document_Number'], 
		right_on=['Account_Number_of_Vendor_or_Creditor','Accounting_Document_Number'], 
		how='left')
	# drop duplicates
	recur_invoice.drop_duplicates(inplace=True)

	# get the correct order and names of key output results fields
	recur_invoice = recur_invoice[['Company_Code','Name_of_Company_Code_or_Company','Account_Number_of_Vendor_or_Creditor',
	'Amount_in_Local_Currency','Currency_Key_y','Amount_in_Document_Currency','Currency_Key_x',
	'Accounting_Document_Number','Document_Date_in_Document','Document_Type','Reference_Document_Number']]
	recur_invoice = recur_invoice.rename(columns = {'Currency_Key_y':'Local_Currency_Key',
		                                'Currency_Key_x':'Document_Currency_Key'})

	# output the result table
	recur_invoice.to_csv("task3.csv")
	print('Task 3 is finished. Output is ready.')


# Task 4: Invoices For One-Time Vendors
def task4(invoice_file_set,invoice_frequency):
	"""
	This function task4() is to get the key output results of Task 4
	The input of task4() function is the output invoice_file_set of task1 and the output invoice_frequency of task2
	"""
	
	# for each Account_Number_of_Vendor_or_Creditor, identify the counts of distinct Number_of_Line_Item_Within_Accounting_Document
	print('Start task 4...')
	line_number_frequency = invoice_file_set.groupby('Account_Number_of_Vendor_or_Creditor')\
	             ['Number_of_Line_Item_Within_Accounting_Document'].nunique().reset_index(name="line_number")

	# merge line_number_frequency with invoice frequency 
	invoice_line_frequency = pd.merge\
	     (invoice_frequency, line_number_frequency , on = 'Account_Number_of_Vendor_or_Creditor', how='left')
	     
	# get the vendors with only one distinct Accounting_Document_Number \
	# and Number_of_Line_Item_Within_Accounting_Document is equal to 1 (one-time vendor)
	one_time_vendors = invoice_line_frequency[(invoice_line_frequency['Invoice_Frequency'] == 1)\
	                                           &(invoice_line_frequency['line_number'] == 1)]

	# some of key output results fields
	z = invoice_file_set.loc[:,['Company_Code','Name_of_Company_Code_or_Company','Account_Number_of_Vendor_or_Creditor',
	                            'Debit_Credit_Indicator','Amount_in_Local_Currency','Currency_Key_y',
	                            'Amount_in_Document_Currency','Currency_Key_x',
	                            'Accounting_Document_Number','Document_Date_in_Document',
	                            'Reference_Document_Number']]
	z.drop_duplicates(inplace=True)

	# merge to get the key output results fields
	one_time_vendors_invoices = pd.merge(one_time_vendors, z, on = 'Account_Number_of_Vendor_or_Creditor',
		                                 how='left')

	# get the correct order and names of key output results fields
	one_time_vendors_invoices = one_time_vendors_invoices[['Company_Code','Name_of_Company_Code_or_Company',
	                           'Account_Number_of_Vendor_or_Creditor','Debit_Credit_Indicator',
	                           'Amount_in_Local_Currency','Currency_Key_y','Amount_in_Document_Currency',
	                           'Currency_Key_x','Accounting_Document_Number','Document_Date_in_Document',
	                           'Reference_Document_Number']]
	one_time_vendors_invoices = one_time_vendors_invoices.rename(columns = {
		                'Currency_Key_y':'Local_Currency_Key','Currency_Key_x':'Document_Currency_Key'})
	
	# output the result table
	one_time_vendors_invoices.to_csv("task4.csv")
	print('Task 4 is finished. Output is ready.')

# Task 5: Outstanding Debtors Greater Than 60 Days
def task5():
	"""
	This function task5() is to get the key output results of Task 5
	"""
	# load three tables of AR-Accounts_Rec_Listing File
	print('Start task 5...')
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
	
	# only keep Outstanding Debtors Greater Than 60 Days
	balance_amount = balance_amount[(balance_amount['type'] == '60-90')\
	                          |(balance_amount['type'] == '90-120')|(balance_amount['type'] == '> 120')]
	
	# For each customer, get the total outstanding amount,
	# local Amount 60 – 90 Days,local Amount 90 – 120 Days and local Amount >120 days
	balance_amount = balance_amount.sort_values("Customer_Number").reset_index(drop=True)
	Counts = []
	cid = 0
	s1, s2, s3, s = 0, 0, 0, 0
	for i in range(len(balance_amount)):
	    balance = balance_amount.loc[i, "Balance"]
	    if balance_amount.loc[i, "Customer_Number"] != cid:
	        Counts.append([cid, s1, s2, s3, s])
	        cid = balance_amount.loc[i, "Customer_Number"]
	        s1, s2, s3, s = 0, 0, 0, balance
	        if balance_amount.loc[i, "type"] == "60-90":
	            s1 = balance
	        elif balance_amount.loc[i, "type"] == "90-120":
	            s2 = balance
	        elif balance_amount.loc[i, "type"] == "> 120":
	            s3 = balance
	    else:
	        s += balance
	        if balance_amount.loc[i, "type"] == "60-90":
	            s1 += balance
	        elif balance_amount.loc[i, "type"] == "90-120":
	            s2 += balance
	        elif balance_amount.loc[i, "type"] == "> 120":
	            s3 += balance
	Counts.append([cid, s1, s2, s3, s])
	sum_by_type = pd.DataFrame(Counts[1:], columns = ['Customer_Number', "60-90", "90-120", "> 120", "total"])
	
	# get some key output results fields
	x = Accounts_Rec_Listing.loc[:,['Customer_Number','Company_Code','Name_of_Company_Code_or_Company',
	                                'Currency_Key_y','Customer_s_credit_limit','Credit_Control_Area']]
	x.drop_duplicates(inplace=True)

	# merge togther to get the whole key output results table
	sum_by_type = pd.merge(sum_by_type, x, on = 'Customer_Number',how= 'left')
	# exclude Credit_Control_Area "CAN" and keep Credit_Control_Area "US01" or NaN
	sum_by_type = sum_by_type[sum_by_type['Credit_Control_Area'] != 'CAN']

	# get the correct order and names of key output results fields
	sum_by_type = sum_by_type[['Customer_Number','Company_Code','Name_of_Company_Code_or_Company',
	                           'total','Currency_Key_y','Customer_s_credit_limit','60-90','90-120','> 120']]
	sum_by_type= sum_by_type.rename(columns = {'Currency_Key_y':'Local_Currency_Key',
		                                       'total':'Total_Amount_Local_Currency',
		                                       '60-90':'Local_Amount_60-90_Days',
		                                       '90-120':'Local_Amount_90-120_Days',
		                                       '> 120':'Local_Amount_>120_Days'})

	# output the result table
	sum_by_type.to_csv("task5.csv")
	print('Task 5 is finished. Output is ready.')

if __name__ == "__main__":
	invoice_file, po_file = merge_data()
	invoice_file_set, both_in_invoice_po = task1(invoice_file, po_file)
	invoice_frequency = task2(invoice_file_set)
	task3(invoice_file_set, both_in_invoice_po)
	task4(invoice_file_set, invoice_frequency)
	task5()





