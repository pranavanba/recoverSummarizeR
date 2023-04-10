# convert2i2b2

Conversion and summarization of parquet data to i2b2 format

### General logic

1.  Get curated datasets (parquet files)
2.  Get i2b2 concepts map (csv file)
3.  Read curated parquet datasets into separate data frames
4.  Summarize data
5.  Convert output into the desired format for export

### Usage
1.  Run [get_data.R](/get_data.R) to fetch datasets
2.  Run [main.R](/main.R) to process, summarize, and format the data, then return the output
