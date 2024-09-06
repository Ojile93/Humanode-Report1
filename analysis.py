import numpy as np 
import pandas as pd
from itertools import count

# Function to cleanse the table 
def adjust_tab(path,tab_name):
 ar =[]
 ar2 =[]
 ar3 =[]
 df = pd.read_csv(path)
 df = df.replace(np.NaN,0)
 adr = df["address"]
 rewd = df["rewards, HMND, 14 days"]
 epoch = df["number of epochs"]
 for x in adr:
  y = x[:5] +"..."+ x[-5:]
  ar.append(y)
 df2 = pd.DataFrame({"Address":ar,"Epochs":epoch,"Rewards":rewd},index=[x for x in range(1,len(ar)+1)])
 dic = dict()
 arr = [x for x in df2.Epochs]
 for x in arr:
  if x not in dic:
   ae = arr.count(x)
   dic[x] = ae
  else:
   pass
 for key,value in dic.items():
  ar2.append(key)
  ar3.append(value)
 df3 = pd.DataFrame({"Epochs":ar2,"Uptime":ar3},index = [x for x in range(1,len(ar2)+1)])
 arrr2 =[(x/np.sum(df3.Uptime))*100 for x in df3.Uptime]
 df3["% TUs"] = arrr2
 dic2 =dict()
 ar4 =[x for x in df2.Epochs]
 for index, row in df2.iterrows():
  if row["Epochs"] not in dic2:
   dic2[row["Epochs"]] = row["Rewards"]
  else:
   dic2[row["Epochs"]]+= row["Rewards"]
 df3["Rewards"] = df3["Epochs"].map(dic2)
 arrr = [(x/np.sum(df2.Rewards))*100 for x in df3.Rewards]
 df3["% TDR"] = arrr
 df3 = df3.round(2)
 df3 = df3.replace(np.NaN,0)
 df3.to_csv(str(tab_name+"_.csv"))
 df2 = df2.replace(np.NaN,0)
 df2.to_csv(tab_name+"_adr.csv")
 
#adjust_tab("epochs9.csv","sep1")

#data tables 
df = pd.read_csv("jul1_adr.csv")
df2 = pd.read_csv("jul2_adr.csv")
df3 = pd.read_csv("aug1_adr.csv")
df4 = pd.read_csv("aug2_adr.csv")
df5 = pd.read_csv("sep1_adr.csv")
ta = pd.read_csv("sep1_.csv")
print(ta)

# address columns of data tables
adr1 = [x for x in df.Address]
adr2 = [x for x in df2.Address]
adr3 = [x for x in df3.Address]
adr4 = [x for x in df4.Address]
adr5 = [x for x in df5.Address]

# sets of address columns of data tables
set1 = set(x for x in adr1)
set2 =set(x for x in adr2)
set3 = set(x for x in adr3)
set4 = set(x for x in adr4)
set5 = set(x for x in adr5)

# set intersections
ve = set1 & set2
ve2 = set2 & set3
ve3 = set3 & set4
ve4 = set4 & set5
ve5 = set1 & set2 & set3 & set4 & set5

 
# function to extract new participants 
def get_new_participants(addr: list,set: set):
 cont =0
 for x in addr:
  if x not in set:
   cont+=1
  else:
   pass
 return cont

# new participants from jul2 to sep2
jul2 = get_new_participants(adr1,ve)
aug1 = get_new_participants(adr2,ve2)
aug2 = get_new_participants(adr3,ve3)
sep1 = get_new_participants(adr4,ve4)

# function to get total participants
def get_total():
 cont =0
 cont2 =0
 cont3 =0
 cont4 =0
 cont5 =0
 for x in adr1:
  if x not in ve5:
   cont+=1
  else:
   pass
 for x in adr2:
  if x not in ve5:
   cont2+=1
  else:
   pass
 for x in adr3:
  if x not in ve5:
   cont3+=1
  else:
   pass
 for x in adr4:
  if x not in ve5:
   cont4+=1
  else:
   pass
 for x in adr5:
  if x not in ve5:
   cont5+=1
  else:
   pass
 return cont+cont2+cont3+cont4+cont5+len(ve5)


print("Total participants for the period: ",get_total(),"\n") # 2986 uniques addresses participated

print("Addresses that participated all through the period: ",len(ve5),"\n") # 838 addresses participated all through the period

# tabulization of new and total participated addresses per period
tab = pd.DataFrame({("Epochs",""):["Jul1","Jul2","Aug1","Aug2","sept1"],("Participants","New"):[np.NaN,jul2,aug1,aug2,sep1],("Participants","Total"):[len(adr1),len(adr2),len(adr3),len(adr4),len(adr5)]
},index = [x for x in range(1,6)])
tab.to_csv("tab1.csv")
#print(tab)


# function to extract epoch performance of consistent and inconsistent addresses
def get_const_addr_perfo(df):
 arr =[]
 ar2 =[]
 ar3 =[]
 for x in ve5:
  arr.append(x)
 for index,row in df.iterrows():
   if row["Address"] in arr:
    ar2.append(row["Epochs"])
   else:
    ar3.append(row["Epochs"])
 return {"Const_Addr":ar2,"Unconst_Addr":ar3}
 
# Epoch performance for each period
perf1 = get_const_addr_perfo(df)
perf2 = get_const_addr_perfo(df2)
perf3 = get_const_addr_perfo(df3)
perf4 = get_const_addr_perfo(df4)
perf5 = get_const_addr_perfo(df5)
const_adr =[x for x in ve5]

# tabulization of  consistent address epoch performance
tab2 = pd.DataFrame({"Address":const_adr,"Epoch_3518-3601":perf1["Const_Addr"],"Epoch_3602-3685":perf2["Const_Addr"],"Epoch_3686-3769":perf3["Const_Addr"],"Epoch_3770-3853":perf4["Const_Addr"],"Epoch_3854-3937":perf5["Const_Addr"]},index = [x for x in range(1,len(const_adr)+1)])
#tab2.to_csv("tab2.csv")

print(tab2)

# function to get 84 and 83 epoch uptime for each period
def get_perfo(df):
 cont =0
 for index,row in df.iterrows():
   if row["Address"] in ve5 and row["Epochs"] == 84:
    cont+=1
   else:
    pass
 ep = [x for x in df.Epochs]
 per = ep.count(84)
 per2 = per + ep.count(83)
 return {"per":(cont/per)*100,"total":per,"total2":per2}

# applying the function to each epoch period
ta = get_perfo(df)
ta2 = get_perfo(df2)
ta3 = get_perfo(df3)
ta4 = get_perfo(df4)
ta5 = get_perfo(df5)

# summary table
tab3 = pd.DataFrame({
"Epochs":["Epoch_3518-3601","Epoch_3602-3685","Epoch_3686-3769","Epoch_3770-3853","Epoch_3854-3937"],
"% CAC":[ta["per"],ta2["per"],ta3["per"],ta4["per"],ta5["per"]],
"TPE":[ta["total"],ta2["total"],ta3["total"],ta4["total"],ta5["total"]],
"% TPE":[ta["total"]/len(adr1)*100, ta2["total"]/len(adr2)*100,ta3["total"]/len(adr3)*100,ta4["total"]/len(adr4)*100,ta5["total"]/len(adr5)*100],"TPE2":[ta["total2"],ta2["total2"],ta3["total2"],ta4["total2"],ta5["total2"]],
"% TPE2":[ta["total2"]/len(adr1)*100,ta2["total2"]/len(adr2)*100,ta3["total2"]/len(adr3)*100,ta4["total2"]/len(adr4)*100,ta5["total2"]/len(adr5)*100],
"TPA":[len(adr1),len(adr2),len(adr3),len(adr4),len(adr5)],
"NPA":[np.NaN,jul2,aug1,aug2,sep1],
"R_HMND":[np.sum(df.Rewards),np.sum(df2.Rewards),np.sum(df3.Rewards),np.sum(df4.Rewards),np.sum(df5.Rewards)]
},index = [x for x in range(1,6)])
#tab3.to_csv("tab3.csv")

print(tab3)



