# Description of the project:

## Overview of the used datasets:

1- [World Happiness Report](https://www.kaggle.com/unsdsn/world-happiness): The happiness scores and rankings use data from the Gallup World Poll Survey, the dataset tries to estimate the extent to which each of six factors – economic production, social support, life expectancy, freedom, absence of corruption, and generosity – contribute to making life evaluations better.

2- [All 250 Country Data](https://www.kaggle.com/souhardyachakraborty/all-250-country-data): This Dataset contains 250 rows, Each row is a Country. There are 11 columns representing: Name, Region, Subregion, Population, Area, Gini, Real growth rate, Literacy rate, Inflation, Unemployment.

3- [Life Expectancy (WHO)](https://www.kaggle.com/kumarajarshi/life-expectancy-who): Health data from the year 2000-2015 for 193 countries to estimate the life expectancy through immunization factors, mortality factors, economic factors, social factors, and other health-related factors as well.

## Overview of the project goals and the motivation for it:

This project is implemented as a part of the Data Engineering CSEN1095 course at the GUC. Our main goal in this project is to design a complete data engineering process to answer questions we have (mentioned below) about the datasets described above. The Data Engineering process that we implement includes:

1. Cleaning, tidying, plotting, integrating, exploring, and finding appropriate research questions.
2. Feature Engineering and Visualizing.
3. Designing ETL pipeline using Airflow.
4. Deploying the project on a Big Data platform.

## Descriptive steps used for the work done in milestone 1.

1- **Data Integration**: Data from the year "2015" was only considered from the "World Happiness Report" and "All 250 Country Data" datasets. Then, we merged the three datasets on column "Country" to get our integrated dataset.

2- **Data Tidying**: We converted data in columns("Real Growth Rating(%)", "Literacy Rate(%)", "Inflation(%)", "Unemployement(%)") from string into float to better be able to clean the data and run statistical analysis.

3- **Data Cleaning**:

A- **Missing Data:**

1. **"Unemployment(%)"**: We imputed the missing values using the mean(normally-distributed data) of the unemployment rate of countries from the same region as the country with the missing value.
2. **"Literacy Rate(%)"**: We imputed the missing values using the median(left-skewed data) of the Literacy rate of countries from the same region and same status (Developed/ Undeveloped) as the country with the missing value.
3. **"Real Growth Rating(%)"**: We imputed the missing values using the median(right-skewed data) of the Real Growth Rating of countries from the same region and same status (Developed/ Undeveloped) as the country with the missing value.
4. **"Inflation(%)"**: We imputed the missing values using the median(right-skewed data) of the Inflation rate of countries from the same region and same status (Developed/ Undeveloped) as the country with the missing value.
5. **"Alcohol", "Total expenditure"**: We dropped the 2 columns as they had 133/135 missing values.
6. **Population**: Dropped duplicate column, (We preferred to use the other column as it had no missing values).
7. **"Gini"**: We imputed the missing values using the mean(normally-distributed data) of the Gini of countries from the same region and status as the country with the missing value.
8. **"GDP"**: We used data provided from [World Bank](https://data.worldbank.org/indicator/NY.GDP.PCAP.CD?end=2015&start=2015) in 2015 for countries, to replace the original column as it included many wrong values.
9. **"thinness 1-19 years"**: We imputed the missing values using the mean(normally-distributed data) of the "thinness 1-19 years" of countries from the same region as the country with the missing value.
10. **"thinness 5-9 years"**: We imputed the missing values using the mean(normally-distributed data) of the "thinness 5-9 years" of countries from the same region as the country with the missing value.
11. **"BMI"**: We imputed the missing values using the mean(normally-distributed data) of the BMI of countries from the same region as the country with the missing value.
12. **"Hepatitis B"**: We imputed the missing values using the median(Left skewed data) of the BMI of countries from the same region as the country with the missing value.

B- **Outliers:**

- Outliers were detected using box-plots for all columns that we use in our analysis separately, then we checked if those outliers contain wrong values that should be removed. If the values were correct we keep the outliers and if not we remove/impute them.
- The decision for every column's outliers is written as a Text cell below the code that we use for detecting the outliers in that column.

## Descriptive steps used for the work done in milestone 2.

1. **Feature Engineering:**

- Feature 1: We added a new Column (GDP) describing the whole Gross Domestic Product which is the multiplication of GDP per Capita and the population. The feature is visualized using a bar plot where the x-axis represents the Region and the y-axis represents the GDP.

- Feature 2: We added a new column(Population/KM2) that describes the number of people per km2 and is calculated by dividing the population over the area. The feature is visualized using a bar plot where the x-axis represents the Region and the y-axis represents the Population per Kilometer squared.

- Feature 3: We added 3 new columns(Schooling Index, Life Expectancy Index, Income Index) to calculate the fourth column and to add it which is The Income Index Per Capita for each country. To compute those indices we used this [Link](https://ourworldindata.org/human-development-index#:~:text=The%20HDI%20is%20calculated%20as,and%20expected%20years%20of%20schooling). The feature is visualized using a bar plot where the x-axis represents the Region and the y-axis represents the GNI value.

2. **ETL Pipeline:**

We use airflow to create our Extract-Transform-Load pipeline

- 1. Data Extraction: We define three functions in the DAG file "extract_data_250_country", "extract_data_Life_exp", "extract_data_2015", "extract_GDP_per_capita" that extracts the three datasets as CSV and convert them into Dataframes and return them (Task1, Task2, Task3, Task 4).
- 2. Data Transformation:

1. Data Integration: We defined a function "merge_data" that takes the three Dataframes, merges them, and returns the merged Dataframe (Task 5).
2. Data Cleaning: We defined a function "clean_data" that takes the merged Dataframe, cleans it as described above, and returns the cleaned Dataframe (Task 6).
3. Feature Engineering: We defined a function "feature_engieering" that takes the cleaned Dataframe, adds the engineered features as described above, and returns the finalized Dataframe (Task 7).

- 3. Data Loading: We define a function "store_data" that takes the finalized Dataframe and convert it to CSV and save it (Task 8).

- Notes:

1. Tasks are run in order (Task1>>Task2>>Task3>>Task4>>Task5>>Task6>>Task7>>Task8 to ensure smooth implementation of our ETL pipeline.
2. XCOM is used to pass the Dataframes between the tasks.

## Data Exploration/Research questions:

1. Does the literacy rate affect life expectancy, hepatitis b, measles, aids, diphtheria, and polio?
2. Do a country's gini and its unemployment rate affect happiness?
3. Does the region of a country affect its GDP per capita and the mortality of its adults?
4. Does a country's area affect its population?
5. Do a country's population and its GDP per capita affect the unemployment rate?
6. Does the status of a country affects the schooling years or not?
7. Does the Happiness score have a positive or negative relationship with Life Expectancy?
8. Does GDP affect a country's Happiness score?
9. Does the Population/KM2 increases the number of Measles reported cases?
10. How does GNI compare with GDP for countries?
11. How did the Happiness score change for Regions from year 2015 to 2019?
12. Does Life Expectency increase from year 2000 to year 2015 due to the medical advancements?
