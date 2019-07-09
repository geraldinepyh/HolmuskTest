# Background Data Summary 
From the `raw_data.background` table in the `mindlinc` database. 
Last Update: Geraldine Pang on 9th July 2019


## Marital Status
After cleaning free text entries and taking the 5 most frequent entries (excluding the most frequent: "Unknown"): 

![Marital Status Top 5](../results/plots/marital_status_top5.png)

## Income
Income column is also entered as text values. After cleaning out invalid strings, filtering for a reasonable income range (Between 1000 and 100,000) and converting to integers:

![Income Levels](../results/plots/income_hist.png)

