# convert2i2b2

Exploring conversion and summarization of data to i2b2 format

### General logic

1.  Get curated datasets (parquet files)

2.  Get i2b2 concepts map (csv file)

3.  Read curated datasets in parquet format into separate data frames

4.  Convert data into a format usable for export to i2b2/DRC

    1.  Separate unique digital health variables within all data exports into individual data frames

    2.  Summarize data according to downstream analysis/i2b2/DRC specifications

    3.  Reformat into i2b2 format

    4.  Combine all data frames into single data frame ready for export
