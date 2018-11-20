
############################################################################
# usage: to chage VMT csv files
# date: June 1, 2018
# auther: Ehsan Mosadegh
#
# NOTES: for each VV-RR combinations, we have several FF types. I changed FF type to 09 for each VV-RR combination
# and summed VMT of each VV-RR combination and assigned the total VMT to new SCC=09-VV-RR. This new SCC coresponds to
# previous VV-RR combination with several FF types.
# I made a copy of input DF and worked on that, filtered for each county
# then changed FF to 09 in the filtered DF, then filtered each VV-RR combination and summed VMTs.
# I worked on a filtered county DF and modified that slice to form the favorable format.
# in the end, I joined that filtered DF to the bottom of original file. so all modifications happen inside the extracted/filtered county DF.
#
############################################################################

import os
import pandas as pd

############################################################
# set input parameters:
State_cd = 'NV' # change to 'CA' or 'NV'

############################################################

if State_cd == 'NV':
    input_file_name = 'VPOP_NEI_v2_2011_from_v5_partial_E85_noCalif_24dec2015_v2.csv'
    StateCode =  '32'  # should involve "" is a part of str
    NV_ref_County_list = ['001']#['001','003','005','007','009','011','013','015','017','019','021','023','027','029','031','033','510']

elif State_cd == 'CA':
    input_file_name = 'VPOP_NEI_v2_2011_from_v5_partial_E85_Calif_08jan2016_v3.csv'
    StateCode =  '06'  # should involve "" is a part of str
    CA_ref_County_list = ['001','003','005','007','009','011','013','015','017','019','021','023','025','027','029','031','033','035','037','039','041','043','045','047','049','051','053','055','057','059','061','063','065','067','069','071','073','075','077','079','081','083','085','087','089','091','093','095','097','099','101','103','105','107','109','111','113','115']

##########################################################################################
# set pathes based on work directory:

work_dir = '/Users/ehsan/Documents/my_Python_codes/VMT_2040_scenario'
script_dir = work_dir+'/scripts'
input_dir = work_dir+'/inputs'
output_dir = work_dir+'/outputs'

# set input/output file names:
mode = 'RPD'
input_file_path = os.path.join(input_dir,input_file_name)
output_filename = '2040scenario_'+input_file_name
sccxref_filename = 'SCCXREF_'+mode+'_state_'+StateCode
#sccxref_nodup_filename = 'SCCXREF_no_dup_sccxref_'+mode+'_state_'+StateCode

##########################################################################################
if StateCode == '32':
    print('-> State is: %s' %State_cd)
    print('-> list of counties included is: %s' %NV_ref_County_list)
    ref_county_list = NV_ref_County_list

elif StateCode == '06':
    print('-> State is: %s' %State_cd)
    print('-> list of counties in %s is: %s' %(State_cd,CA_ref_County_list))
    ref_county_list = CA_ref_County_list
print('-> work directory is: "%s"' %(os.getcwd()))
print('-> script directory is: "%s"' %(os.getcwd()))
print('-> input file full path is: %s' %input_file_path)
print('-> output directory is %s' %output_dir)
print('-> current directory is %s' %os.getcwd()) # getcwd() () is needed.
print('-> changing directory to work directory ...')
os.chdir(work_dir)

##########################################################################################
# read input DF, set the header of the file first, and parse SCC col.

print('-> read in "df_input_vpop" DF ...')
df_input_vpop = pd.read_csv(input_file_path , sep=',' , comment='#' , quotechar='"' , quoting=3) # # define comments by #!

# must copy DF to new one, because df_input_vpop does not change
#print('-> copy input DF to keep records ...')
#df_original = df_input_vpop.copy()

# parse region and SCC columns first
print('-> parse "region_cd" and "SCC" columns of DF ...')
df_input_vpop['State'] = df_input_vpop.region_cd.str[1:3] # parse region col.
df_input_vpop['County'] = df_input_vpop.region_cd.str[3:6]
df_input_vpop['mobile'] = df_input_vpop.scc.str[1:3] # parse 22
df_input_vpop['FF'] = df_input_vpop.scc.str[3:5] # parse FF col. ; cos includes " in the begining, index start from 1
df_input_vpop['VV'] = df_input_vpop.scc.str[5:7] # parse VV col.
df_input_vpop['RR'] = df_input_vpop.scc.str[7:9] # parse RR
df_input_vpop['PP'] = df_input_vpop.scc.str[9:11] # parse PP

##########################################################################################
# define the final SCCXREF and SUM_total DF that will get filled inside the loop
#sccxref_df_total = pd.DataFrame()

# define total summed df- stores all counties,
df_stack_state_EV_SCC = pd.DataFrame()#columns=df_filtered_VMTsummed.colu...)


# define loop over all CountyCode numbers inside the list
for cnty_cd in range(len(ref_county_list)):
    CountyCode = ref_county_list[cnty_cd]
    print('==================================================================')
    print('-> START the new loop for: CountyCode= %s:' %CountyCode)

    # define filter1 for state-county
    filter_st_cnty = (df_input_vpop.State == StateCode) & (df_input_vpop.County == CountyCode)

    # extract the slice that matches the filter=state&county of interest
    df_filtered_st_cnty = df_input_vpop[filter_st_cnty]

   # QA check to see if county is inside the filter- look inside county col
    County_col = df_filtered_st_cnty.County

    if County_col.size == 0: # size shows no. of elements in DF- why with () does not work?
        print('-> WARNING: CountyCode %s is NOT inside input file' %CountyCode)
    else:
        print('-> CountyCode %s is inside input file' %CountyCode)


##########################################################################################
# Task : filter each county and SUM VMTs for all VV-RR types and assign to EV=09 FF type

        # create an empty DF to store summed values
        stack_of_cnty_VV_RR_slices = pd.DataFrame(columns = df_filtered_st_cnty.columns.tolist())

        print('-> set new filter and loop over VV-RR types for county code: %s'%CountyCode)
        # here for each combination of VV-RR I extract VMTs
        for vv_type in ['11']:#,'21','31','32','41','42','43','51','52','53','54','61','62']:
            for rr_type in ['01']:#,'02','03','04','05']:

                # define filter2 for VV-RR
                filter_vv_rr = (df_filtered_st_cnty.VV == vv_type) & (df_filtered_st_cnty.RR == rr_type) # makes those spaces inside df_filtered_st_cnty TRUE if the conditions meets; boolean
                # apply filter and copy DF
                df_filtered_st_cnty_VV_RR = df_filtered_st_cnty[filter_vv_rr]

                # check if DF is empty or not
                if df_filtered_st_cnty_VV_RR.empty == True:
                    print('-> NOTE: DF is empty for VV=%s and RR=%s, meaning we do NOT have VPOP for this SCC!' %(vv_type,rr_type))

                elif df_filtered_st_cnty_VV_RR.empty != True:
                    print('-> NOTE: DF is NOT empty for VV=%s and RR=%s, meaning we DO have VPOP for this SCC!' %(vv_type,rr_type))

##########################################################################################
# copy the slice, change VPOP, work on copy
                    # copy the slice and then will change the original slice to change the original DF=df_input_vpop
                    slice_copy = df_filtered_st_cnty_VV_RR.copy()

#                    print('-> VPOP originally was:')
#                    print(df_filtered_st_cnty_VV_RR['ann_value'])

                    # change VPOP in original DF: df_input_vpop
#                    print('-> VPOP after 50% reduction is:')
                    df_input_vpop.loc[df_filtered_st_cnty_VV_RR.index,'ann_value'] = df_filtered_st_cnty_VV_RR['ann_value']*0.5
#                    print(df_input_vpop.loc[df_filtered_st_cnty_VV_RR.index,'ann_value'])


                    # now work on filtered DF: slice_copy
                    # update FF col for EV=09
                    slice_copy['FF'] = '09'

#                    print('-> VPOP in slice_copy DF originally was:')
#                    print(slice_copy.ann_value)
#                    print('-> VPOP in slice_copy DF after 50% reduction: ')

# change VPOP
                    slice_copy['ann_value'] = slice_copy['ann_value']*0.5
#                    print(slice_copy.ann_value)

                    # calculate VPOP total,
                    #df_filtered_VMTsummed.loc[row_index] = slice_copy.sum(axis=0 , numeric_only=True)
                    # define index, else SUMs overwrite the old one
                    row_index = 'VPOP_total_'+StateCode+'_'+CountyCode+'_'+vv_type+'_'+rr_type
                    stack_of_cnty_VV_RR_slices.loc[row_index] = slice_copy.sum(axis=0 , numeric_only=True)

#                    print('-> stack DF before update is:')
#                    print(stack_of_cnty_VV_RR_slices)
                    # replace NaN with strings from 1st row
                    stack_of_cnty_VV_RR_slices.loc[row_index] = stack_of_cnty_VV_RR_slices.loc[row_index].combine_first(slice_copy.iloc[0])

#                    print('-> stack DF after update is:')
#                    print(stack_of_cnty_VV_RR_slices)

        print('-> append "stack_of_cnty_VV_RR_slices" DF to total "df_stack_state_EV_SCC" for CountyCode: %s' %CountyCode)
        df_stack_state_EV_SCC = pd.concat([df_stack_state_EV_SCC,stack_of_cnty_VV_RR_slices],axis=0)# adds rows of df1 to df2

print('=============================== end of loop ======================================')

##########################################################################################
# join parsed col.s to create updated SCC col. with FF=09
df_stack_state_EV_SCC['scc_updated_for_EV'] = '"'+df_stack_state_EV_SCC['mobile']+df_stack_state_EV_SCC['FF']+df_stack_state_EV_SCC['VV']+df_stack_state_EV_SCC['RR']+df_stack_state_EV_SCC['PP']+'"'   # makes just one col.
# update SCC column with updated SCC=FF=09; get col "scc_updated_for_EV" and drop from DF "df_stack_state_EV_SCC"
df_stack_state_EV_SCC['scc'] = df_stack_state_EV_SCC.pop('scc_updated_for_EV')


# concat two DFs: "df_input_vpop" and "df_total_SCCupdated_VMTsummed", and create a final DF: "df_merged_inputVMT_EVupdated"
df_merged_inputVMT_EVupdated = pd.concat( [df_input_vpop , df_stack_state_EV_SCC] , axis=0)


# delete extra columns
df_merged_inputVMT_EVupdated.drop(['State','County','mobile','FF','VV','RR','PP'] , axis=1 , inplace=True)

print('-> tail of "df_merged_inputVMT_EVupdated" for writing is:')
print(df_merged_inputVMT_EVupdated.tail(10))
print('-> info of "df_merged_inputVMT_EVupdated" for writing is:')
print(df_merged_inputVMT_EVupdated.info())

##########################################################################################
# write out section

# check if files are available first
output_filename_full_path = os.path.join(output_dir , output_filename)
file_available = os.path.isfile(output_filename_full_path)

if (file_available == True):
    print('-> output file was available and was deleted,\
    now we will write a new file')
    os.remove(output_filename_full_path)
else:
    print('-> output file was not there before,\
    we will write it now')

print('=============================== writting files ====================================')
df_merged_inputVMT_EVupdated.to_csv(output_filename_full_path , index=False , sep=',' , quoting=3 , quotechar='"')     # puting the quotechar solves the issue of several quotes arounf strings
print('-> following file was written as VPOP output file: "%s"' %output_filename_full_path)
print('-> *** finished SUCCESSFULLY ***')
print('===================================================================================')
##########################################################################################

