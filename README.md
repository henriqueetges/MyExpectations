## Data Integrity

This project aims at creating tools that would allow me to annotate and test my datasets for 
data quality issues. While I am aware that there are other packages around pypl that handle this job,
I have found myself getting lost while trying to setup some of those. So I started testing developing
my own.

I am aiming at being able to detect and address:

Completeness - Data being incomplete
Consistency - Data not being consistent between sources
Timeliness - Data being outdated
Accuracy - Data not being comparable to real world observations / rules
Uniqueness - Data does not have duplication issues
Validity - Data is at the right format

## How is this project structured?

There are two main components:
- Configs:

    Contains the configuration for each dataset to be created/ present in my environemnt. Inside of the yml, we have the definitions
    of the datasets and which columns are in it, their types, as well as the tests to be applied against each column of the dataset

- Annotator package

    Inside of the utils package, we have two classes that will handle annotating and checking the datasets:
    - Annotator: Handle table operations, checking each column of the dataset against the pre-defined tests
    - Annotator Manager: Handles assigning table configurations to the annotator class

## How can you help?

By suggesting changes