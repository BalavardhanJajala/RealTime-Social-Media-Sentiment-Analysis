### FOR THIS TEST, WE HAVE TAKEN 1 LAKH RECORDS EACH IN REDDIT & YOUTUBE AND PERFORMED THE FOLLOWING TEST #################


import pandas as pd
from scipy.stats import chi2_contingency

# We are going with two hypotheses.
# Null Hypothesis (H₀): There is no significant association between sentiment values and hate speech labels.
# Alternative Hypothesis (H₁): There is a significant association between sentiment values and hate speech labels.


# Reading data for the "Reddit"
file_path = '/Users/kvpvishnuvardhan/Documents/Pythonpractice/reddit_output.csv'
df = pd.read_csv(file_path)

print(" \n Displaying the REDDIT RESULTS \n")
# displaying few rows first
print(df.head())

# Contigency table
value_hate_cross = pd.crosstab(df['value'], df['hatevalue'])
print(value_hate_cross)

# Performing the Chi-Square test - in our case, this is to determine the relation between the "Negative" sentiment and hate speech
chi_stat, pvalue, df, exp_freq = chi2_contingency(value_hate_cross)
print('chi_stat: {}, pvalue: {}, df: {}'.format(chi_stat, pvalue, df))

# Results
if pvalue < 0.05:
    print("Reject the null hypothesis. There is a significant association between sentiment values and hate speech labels.")
else:
    print("Fail to reject the null hypothesis. There is no significant association between sentiment values and hate speech labels.")


# Repeating the same for Youtube as well

# Reading data for the "YOUTUBE"
file_path = '/Users/kvpvishnuvardhan/Documents/Pythonpractice/yt_output.csv'
df = pd.read_csv(file_path)

print(" \n Displaying the YOUTUBE RESULTS \n")

# displaying few rows first
print(df.head())

# Contigency table
value_hate_cross = pd.crosstab(df['value'], df['hatevalue'])
print(value_hate_cross)

# Performing the Chi-Square test - in our case, this is to determine the relation between the "Negative" sentiment and hate speech
chi_stat, pvalue, df, exp_freq = chi2_contingency(value_hate_cross)
print('chi_stat: {}, pvalue: {}, df: {}'.format(chi_stat, pvalue, df))

# Results
if pvalue < 0.05:
    print("Reject the null hypothesis. There is a significant association between sentiment values and hate speech labels.")
else:
    print("Fail to reject the null hypothesis. There is no significant association between sentiment values and hate speech labels.")
