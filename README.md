# data_gov_sg_extractor

This Python package provides a convenient way to extract data from the data.gov.sg portal, specifically focusing on collections and datasets.

## Introduction

The data.gov.sg portal hosts a wealth of raw data, including collections and datasets related to various aspects of Singaporean society, economy, and government. This package offers a streamlined method for extracting and managing this data for analysis and use in data pipelines.

## Features

- **Collection and Dataset APIs:** Utilizes data.gov.sg's Collection and Dataset APIs for easy access to structured data.
- **Wrapper Function:** Provides a wrapper function for downloading entire collections of datasets, simplifying the data acquisition process.
- **CSV Backup:** Allows users to save downloaded data as CSV files for backup and offline use.
- **Pandas DataFrame Integration:** Supports conversion of downloaded data into Pandas DataFrames for analysis and integration into data processing pipelines.
- **Flexible Download Options:** Offers flexibility to re-download datasets only if they have been updated, minimizing bandwidth usage.

## Example

```python

# Define collection id
collection_id = "2"

# Download entire collection
download_collection(collection_id, chk='Yes', combcsv='Yes', indcsv='Yes', csvdir='data')

## Known Issues

- **dtype Warnings:** Loading CSV files into Pandas DataFrames may result in dtype warnings. This can be addressed by saving and using dtype dictionaries in JSON format alongside CSV files.


