## About this Project
This project is used to fetch data from Synapse, then process, summarize, and format the data, and finally write the output to CSV files and store them in Synapse

## Getting Started
Two methods to install and use this project: Docker and local
### Docker
1. Build the docker image
```sh
docker build...
```
2. Run the docker container*
```sh
docker run...
```
3. Forward local port 8787
4. Connect to http://localhost:8787/
5. Login to RStudio Server
6. Run [main.R](/main.R)

*Must pass `SYNAPSE_AUTH_TOKEN` as an environment variable to `docker run...` unless the variable has already been added to ~/.bash_profile and exported in the same session (Synapse token is required to use the `synapser` package). For help with using Synapse, Synapse API, Synapse clients, Synapse personal access tokens, etc., refer to the [Synapse documentation](https://help.synapse.org/docs/)

### Local (requires local installation of R)
1. Clone the repo
```sh
git clone repo-name
```
2. Navigate to the repo
```sh
cd /path/to/repo/
```
3. Run [main.R](/main.R)

## Code Flow

1.  Get curated datasets (parquet files)
2.  Get i2b2 concepts map (csv file)
3.  Read curated parquet datasets into data frames
4.  Summarize data
5.  Process and transform the output into the desired format for export
6.  Export output to CSV and Synapse
