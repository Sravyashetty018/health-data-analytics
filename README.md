# health-data-analytics

Data source: 
https://www.datafiles.samhsa.gov/sites/default/files/field-uploads-protected/studies/MH-CLD-2021/MH-CLD-2021-datasets/MH-CLD-2021-DS0001/MH-CLD-2021-DS0001-bundles-with-study-info/MH-CLD-2021-DS0001-bndl-data-csv_v1.zip

Transformations:
a. Converted the categorical variables (GENDER, RACE, ETHNICITY, MARITAL, EMPLOY, INCOME) from numeric values to appropriate string or categorical data types.
b. Converted that the CASEID variable is stored as an integer data type.
c. Converted the numeric variables (MENHLTH, PHYHLTH, POORHLTH) to float data types.
d. Normalize the numeric variables (MENHLTH, PHYHLTH, POORHLTH) using min-max scaling to bring them to a common scale between 0 and 1
e. Filter out the Missing/unknown/not collected/invalid and Standardize the numeric variables (MENHLTH, PHYHLTH, POORHLTH) using z-score normalization to have a mean of 0 and a standard deviation of 1
f. Splitted the dataset into training and testing sets, ensuring a representative split based on the demographic variables (GENDER, RACE, ETHNICITY, MARITAL, EMPLOY, INCOME).
g. Implemented stratified sampling to create the training and testing sets, maintaining the same proportions of the demographic variables in both sets.
c. Created a validation set from the training set using a similar stratified sampling approach.

The rationale behind the Medallion Architecture implementation
1. Challenges in Data Organization:
2. Data lakes can become chaotic due to the lack of structure. Storing raw data without predefined schemas can lead to confusion and inefficiencies.
Organizations struggle with organizing data, ensuring quality, and maintaining governance within the lakehouse architecture.


Medallion Architecture Overview:
Incremental Enhancement: Medallion Architecture proposes a systematic approach to enhance data quality gradually. It divides data into three layers:
Bronze: Raw data with minimal processing.
Silver: Enhanced data with basic transformations (e.g., cleaning, schema enforcement).
Gold: Refined data ready for advanced analytics and machine learning.

Flexibility: Medallion allows data to evolve as it moves through these layers, ensuring agility and adaptability.
Governance: By enforcing structure and quality at each stage, Medallion promotes better governance.
Performance: Optimized data processing and query performance are achieved by refining data incrementally.

Logging:
We can log the run failures, number of records processed and good record and bad record counts including excpetions
Monitoring: We can monitor the job success and failure by writing logs to log analytics workspace and triggering emails and creating tickets
Exception Handling: If the job fails with exceptions we can have retry mechanisms to retrigger the job

Steps to run:
1. Download the notebook
2. Import it in your environment
3. Click on run and grab a cup of coffee, it will be completed before you finish your coffee :)
