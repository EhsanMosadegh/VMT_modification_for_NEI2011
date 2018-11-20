
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
    input_file_name = 'VMT_NEI_v2_2011_from_v6_partial_E85_noCalif_24dec2015_v2.csv'
    StateCode =  '32'  # should involve "" is a part of str
    NV_ref_County_list = ['001']#,'003','005','007','009','011','013','015','017','019','021','023','027','029','031','033','510']

elif State_cd == 'CA':
    input_file_name = 'VMT_NEI_v2_2011_from_v6_partial_E85_Calif_24dec2015_v1.csv'
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
sccxref_nodup_filename = 'SCCXREF_no_dup_sccxref_'+mode+'_state_'+StateCode

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
# read the csv file, set the header of the file first, and parse SCC col.
print('-> opening DataFrame ...')
df_input_vmt = pd.read_csv(input_file_path , sep=',' , comment='#' , quotechar='"' , quoting=3) # # define comments by #!

# must copy DF to new one, because df_input_vmt does not change
print('-> copy input DF to keep records ...')
df_original = df_input_vmt.copy()

# parse df_original, not the input- region and scc column before anything
print('-> parse columns of original DF ...')
df_original['State'] = df_original.region_cd.str[1:3] # parse region col.
df_original['County'] = df_original.region_cd.str[3:6]
df_original['mobile'] = df_original.scc.str[1:3] # parse 22
df_original['FF'] = df_original.scc.str[3:5] # parse FF col. ; cos includes " in the begining, index start from 1
df_original['VV'] = df_original.scc.str[5:7] # parse VV col.
df_original['RR'] = df_original.scc.str[7:9] # parse RR
df_original['PP'] = df_original.scc.str[9:11] # parse PP

##########################################################################################
# define the final SCCXREF and SUM_total DF that will get filled inside the loop
sccxref_df_total = pd.DataFrame()
# define total summed df- stores all counties,
df_total_SCCupdated_VMTsummed= pd.DataFrame()#columns=df_filtered_VMTsummed.colu...)


# define loop over all CountyCode numbers inside the list
for cnty_cd in range(len(ref_county_list)):
    CountyCode = ref_county_list[cnty_cd]
    print('==================================================================')
    print('-> START the new loop for: CountyCode= %s:' %CountyCode)

    # define state/county filter
    filter_cnty = (df_original.State == StateCode) & (df_original.County == CountyCode)
    # add filter as a col
    df_original['filter_cnty'] = filter_cnty
    # extract the part that matches the filter=county of interest
    df_filtered_cnty = df_original[df_original.filter_cnty==True].copy()
    # QA check to see if county is inside the filter- look inside county col
    County_col = df_filtered_cnty[df_filtered_cnty.filter_cnty==True].County
    # look inside county col-note: elements are string
    if County_col.size == 0: # size shows no. of elements in DF- why with () does not work?
        print('-> WARNING: CountyCode %s is NOT inside input file' %CountyCode)
    else:
        print('-> CountyCode %s is inside input file' %CountyCode)

    #print('-> FILTER: State is %s and County is %s' %(StateCode,CountyCode))
    #df_filtered_cnty = df_original[filter_cnty].copy()
##################
    # check to see if CountyCode is inside input file
#    if df_filtered_cnty.empty ==True:
#        print('-> WARNING: CountyCode %s is NOT inside input file' %CountyCode)
#    else:
#        print('-> CountyCode %s is inside input file' %CountyCode)
######################
    # set the filter on original DF and make a copy

##########################################################################################
# Task 1: modify SCCs to define FF type EV=09
# set FF09,PP00,PP40 columns for coresponding FF-VV-RR to EV vehicles

    # define break/tire SCC (PP=40) for full SCCs- update SCCXREF for EV cars
        df_filtered_cnty['PP40'] = df_filtered_cnty['PP']
        df_filtered_cnty['PP40'] = '40'
        df_filtered_cnty['full_scc'] = '"'+df_filtered_cnty['mobile']+df_filtered_cnty['FF']+df_filtered_cnty['VV']+df_filtered_cnty['RR']+df_filtered_cnty['PP40']+'"'
        # define EV SCC for reference SCC
        df_filtered_cnty['FF09'] = df_filtered_cnty['FF']
        df_filtered_cnty['FF09'] = '09'
        df_filtered_cnty['PP00'] = df_filtered_cnty['PP']
        df_filtered_cnty['PP00'] = '00'
        df_filtered_cnty['reference_scc'] = '"'+df_filtered_cnty['mobile']+df_filtered_cnty['FF09']+df_filtered_cnty['VV']+df_filtered_cnty['RR']+df_filtered_cnty['PP00']+'"'

#        print('-> head of df_filtered_cnty after adding PP40-PP00-FF09 and full_SCC/ref_SCC col.s is:')
#        print(df_filtered_cnty.head())

        # define columns of SCCXREF to concate
        columns = [df_filtered_cnty.full_scc , df_filtered_cnty.reference_scc]

        # concate columns to form a DataFrame
        sccxref_df = pd.concat(columns , axis=1)
        # add this step to sccxref_df_total DF outside the loop
        print('-> append SCCXREF DF to total DF for CountyCode: %s' %CountyCode)
        sccxref_df_total = pd.concat([sccxref_df_total,sccxref_df],axis=0)
#        print('-> head of SCCXREF file is:')
#        print(sccxref_df.head())

##########################################################################################
# Task 2: filter each county and SUM VMTs for all VV-RR types and assign to EV=09 FF type

        # create an empty DF to store summed values
        df_filtered_VMTsummed = pd.DataFrame(columns = df_filtered_cnty.columns.tolist())

        print('-> loop over VV/RR types for county code: %s'%CountyCode)
        # here for each combination of VV-RR I extract VMTs
        for vv_type in ['11']:#,'21','31','32','41','42','43','51','52','53','54','61','62']:
            for rr_type in ['01']:#,'02','03','04','05']:
                # define filter for VV/RR
                filter_vv_rr = (df_filtered_cnty.VV == vv_type) & (df_filtered_cnty.RR == rr_type) # makes those spaces inside df_filtered_cnty TRUE if the conditions meets; boolean
                # apply VV-RR filter and copy DF
                df_filtered_cnty_VV_RR = df_filtered_cnty[filter_vv_rr].copy()

                # check if DF is empty or not
                if df_filtered_cnty_VV_RR.empty == True:
                    print('-> NOTE: DF is empty for VV=%s and RR=%s, meaning we do NOT have VMT for this SCC!' %(vv_type,rr_type))

                elif df_filtered_cnty_VV_RR.empty != True:
                    print('-> NOTE: DF is NOT empty for VV=%s and RR=%s, meaning DO have VMT for this SCC!' %(vv_type,rr_type))

                    # define row index for stacked DF
                    row_index = 'VMT_total_'+StateCode+'_'+CountyCode+'_'+vv_type+'_'+rr_type
                    #print('-> row_index is: %s' %row_index)
                    # calculate total VMT of each col in filtered DF for a VV-RR combination and then assign the total to
                    # new/coresponding 09-VV-RR SCC, which indicates VV-RR combination change from FF to EV, but keeps the same baseline VMT
                    df_filtered_VMTsummed.loc[row_index] = df_filtered_cnty_VV_RR.sum(axis=0 , numeric_only=True)
                    # fills NaN in each df_filtered_VMTsummed line with values from df_filtered_cnty_VV_RR, replacing NaN with strings from original DF
                    df_filtered_VMTsummed.loc[row_index] = df_filtered_VMTsummed.loc[row_index].combine_first(df_filtered_cnty_VV_RR.iloc[0])

        print('-> append "df_sum" DF to total "df_sum_total" for CountyCode: %s' %CountyCode)
        df_total_SCCupdated_VMTsummed = pd.concat([df_total_SCCupdated_VMTsummed,df_filtered_VMTsummed],axis=0)# adds rows of df1 to df2
##########################################################################################
# write out output file
#df_total_SCCupdated_VMTsummed = pd.DataFrame(list_of_total_sum)
print('=============================== end of loop ======================================')

# join parsed col.s to create updated SCC col. with FF=09
df_total_SCCupdated_VMTsummed['scc_updated_for_EV'] = '"'+df_total_SCCupdated_VMTsummed['mobile']+df_total_SCCupdated_VMTsummed['FF09']+df_total_SCCupdated_VMTsummed['VV']+df_total_SCCupdated_VMTsummed['RR']+df_total_SCCupdated_VMTsummed['PP']+'"'   # makes just one col.
#print('-> head of df_total_SCCupdated_VMTsummed after loop over counties and before dropping columns is:')
#print(df_total_SCCupdated_VMTsummed.head())

# update SCC column with updated SCC=FF=09
df_total_SCCupdated_VMTsummed['scc'] = df_total_SCCupdated_VMTsummed['scc_updated_for_EV']

# delete columns
df_total_SCCupdated_VMTsummed.drop(['mobile','FF','FF09','VV','RR','PP','PP40','PP00','scc_updated_for_EV','State','County','reference_scc','full_scc','filter_cnty'] , axis=1 , inplace=True)

#print('-> head of "df_total_SCCupdated_VMTsummed" after droppoing columns is:')
#print(df_total_SCCupdated_VMTsummed.head())
#print('-> info of "df_total_SCCupdated_VMTsummed" is:')
#print(df_total_SCCupdated_VMTsummed.info())

##########################################################################################
# write out final merged DF

# concat two DFs: "df_input_vmt" and "df_total_SCCupdated_VMTsummed", and create a final DF: "df_merged_inputVMT_EVupdated"
df_merged_inputVMT_EVupdated = pd.concat([df_input_vmt,df_total_SCCupdated_VMTsummed] , axis=0)

print('-> tail of "df_merged_inputVMT_EVupdated" for writing is:')
print(df_merged_inputVMT_EVupdated.tail(10))
print('-> info of "df_merged_inputVMT_EVupdated" for writing is:')
print(df_merged_inputVMT_EVupdated.info())

# check if files are available
output_filename_full_path = os.path.join(output_dir , output_filename)
file_available = os.path.isfile(output_filename_full_path)

if (file_available == True):
    print('-> output file was available and was deleted,\
    now we will write a new file')
    os.remove(output_filename_full_path)
else:
    print('-> output file was not there before,\
    we will write it now')

# write out the final DF
df_merged_inputVMT_EVupdated.to_csv(output_filename_full_path , index=False , sep=',' , quoting=3 , quotechar='"')     # puting the quotechar solves the issue of several quotes arounf strings
#print('-> following file was written as output file: "%s"' %output_filename_full_path)

##########################################################################################
# write out the total SCCXREF file

# full path to SCCXREF file
sccxref_full_path = os.path.join(output_dir , sccxref_filename)
print('-> writing out SCCXREF file: %s' %sccxref_full_path)

# check if the SCCXREF file exists
sccxref_file_available = os.path.isfile(sccxref_full_path)
if (sccxref_file_available == True):
    print('-> similar SCCXREF file was available and was deleted,\
    now we will write a new SCCXREF file')
    os.remove(sccxref_full_path)
else:
    print('-> SCCXREF file was not there before,\
    we will write it now')
# write SCCXREF DataFrame to csv
sccxref_df_total.to_csv(sccxref_full_path , index=False , sep=',' , quoting=3 , quotechar='"')
#print('-> following file was written as SCCXREF file: "%s"' %sccxref_full_path)


##########################################################################################
# sanity check for duplicate entries in "sccxref_df_total"

# define a filter for duplicated entries
sccxref_df_total['is_dup'] = sccxref_df_total.duplicated(['full_scc','reference_scc']) # finds duplicated rows
# want to get no-dup enteries
no_dup_sccxref = sccxref_df_total.loc[sccxref_df_total.is_dup==False]

# sanity check= for True, select one col and SUM to see if we have duplicate
if no_dup_sccxref[no_dup_sccxref.is_dup==True].full_scc.sum() == 0:
    print('-> there is no dupliucate entry in "no_dup_sccxref" DF')
else:
    print('-> duplicate entry was found in "no_dup_sccxref" DF')

print('-> write SCCXREF no_dup_sccxref DF to a csv file...')
no_dup_sccxref.drop(['is_dup'] , axis=1 , inplace=True)
# set the path
sccxref_nodup_full_path = os.path.join(output_dir , sccxref_nodup_filename)
# write "no_dup_sccxref" DF to csv file
no_dup_sccxref.to_csv(sccxref_nodup_full_path , index=False , sep=',' , quoting=3 , quotechar='"')
print('=============================== writting files ======================================')
print('-> following file was written as  VMT output file: "%s"' %output_filename_full_path)
print('-> following file was written as SCCXREF file: "%s"' %sccxref_full_path)
print('-> file: "%s" was written to path:"%s"' %(sccxref_nodup_filename,sccxref_nodup_full_path))
print('-> *** SUCCESSFULLY finished ***')
print('=====================================================================================')



