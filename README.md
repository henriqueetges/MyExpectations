## Data Integrity

This project aims at creating tools that would allow me to annotate and test my datasets for 
data quality issues. While I am aware that there are other packages around pypl that handle this job,
I have found myself getting lost while trying to setup some of those. So I started testing developing
my own.

I am aiming at being able to detect and address:

- Completeness - Data being incomplete
- Consistency - Data not being consistent between sources
- Timeliness - Data being outdated
- Accuracy - Data not being comparable to real world observations / rules
- Uniqueness - Data does not have duplication issues
- Validity - Data is at the right format

## How is this project structured?

There are three main components:
- Configs:

    Contains the configuration and metadata for each dataset (table), which defines where to fetch data from and which file to use to test the columns. 

- Columns metadata:

    Contains which columns will be tested, what are the tests and which arguments to use in each. In this project it is defined of the folders in data, because I wanted to keep it structured that way
    for my project. As long as the configs file is correctly poiting to them, you are fine with putting it anywhere!

- Annotator package

    Inside of the utils package, we have two classes that will handle annotating and checking the datasets:
    - Annotator: Handle table operations, checking each column of the dataset against the pre-defined tests
        Types of tests:
            - Completeness: is_null 
            - Timeliness: not_timely
            - Validity: outside_of_rules
            - Uniqueness: duplicated
            - Conformity: To be implemented, I want to test three things here
                - Conformity with other columns in a differnt table
                - Data type conformity
                - Pattern conformity
    - Annotator Manager: Main handles of this project, it is the guy who takes the spark session and table metadata and runs all of the checks.

## How can I reproduce this?

1 - Install required packages
2 - Make sure that you have pyspark working in your environment
3 - Make the appropriate changes to configs and metadata files

## How can you help?

By suggesting changes

# IMPORTANT!

Right now, the handler only loads parquet files. I may update this in the future to include support to other sources.
