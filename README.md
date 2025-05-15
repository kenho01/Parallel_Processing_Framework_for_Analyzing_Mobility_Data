# AY2425_Ken_Process-Optimisation
Parallel Processing Framework for Analyzing Mobility Data

[Project Description]
The objective of this projects are: 
- Utilizing Spark Framework to Enhance Efficiency and Performance of Processing Large-scale Data in Local Environment.
- Expand, Fine-tune and Profile Performance of Spark Session for Optimize Data Cleaning and Processing in Cluster Environment.
- Building a Scalable Data Warehouse and Query Performance Analysis on Different Data Models.


## Important Notes/Assumption
- Dataset used consists of 
    - Taxi trips data between 01 January 2022 to 31 December 2022 obtained from the Land Transport Authority (LTA).
- DATA and OUTPUT (results) folders not stored in this repository. Thus, to run the code, DATA and OUTPUT folders are to be added at 
```
   [your working Folder]
   ├── RESOURCES                            <- Store Data
   ├── RESULTS                              <- Output directory for any results from the git repository
   ├── this git repository                  <- Implementation and Experiment Script of this project 
```


## This Project CODE 
```
    .
    ├── queryPerformanceAnalysis            <- All code associated to query performance analysis 
    │   ├── bigQuery.ipynb                  <- Testing data pull using BigQuery API
    │   ├── bigQueryDataModel.py            <- Building Fact Dimension Data Model with Spark
    │   ├── factDimension.py                <- Queries ran on Fact-Dimension Data Model
    │   ├── OBT.py                          <- Queries ran on One Big Table Data Model
    ├── cluster                             <- All code associated to running spark in HPC cluster 
    │   ├── main_cluster_debugging.py       <- Cluster debugging script
    │   ├── main_cluster.py                 <- Spark code submit to spark-master
    ├── main_debugging.py                   <- Local mode debugging script
    ├── main.py                             <- Initialize spark locally
    └── README.md
```


## Setup Process (Add Files):
1. Create Folders (If not exist):
```
    ../resources
    ../results
```

2. Place Datasets [Datasetlink -- from Ms.Team](google.com) into /resources

3. Ensure `PYTHONPATH` points to current working directory

4. Ensure Spark, Java & Hadoop are installed 


## Run Data Processing using Spark Locally 
1. Create a virtual environment, python3 -m venv venv
2. Activate virtual environement, source venv/bin/activate
3. Ensure you have software installed, python --version, java --version, pyspark
4. Run main.py


## Flow of Functions
### main.py
1. Raw data ingestion 
2. Verify if each row has exactly 9 columns
3. Verify if DT_START & DT_END has valid expression
4. Cast data to correct data types & invoke User-Defined Function, within_boundary()
5. Filter trips with invalid pickups or dropoffs
6. Remove duplicated trips 
7. Calculate duration of trip and haversine distance of each trip using its pickup and dropoff latitude & longitude
8. Connect trips
9. Remove anomalous trip
10. Output processed data into CSV format

## Versions and Dependencies 
- Python 3.12.4
- Java 22.0.2
- Spark 3.5.3


## Contributors
* **Ken** - [kenho01](https://github.com/kenho01)
* **Lidet** - [lidetys](https://github.com/yslidet)


## Citing us
If you use or build on our work, please consider citing us:

```bibtex
https://ethzchsec.sharepoint.com/sites/URECA2023-P2Pdata/Shared%20Documents/Forms/AllItems.aspx?csf=1&web=1&e=cjGqCX&CID=05f04aa6%2D6f41%2D40f4%2D9c96%2D64119770521f&FolderCTID=0x0120007F6A6F89104374449309F506145AE92A&id=%2Fsites%2FURECA2023%2DP2Pdata%2FShared%20Documents%2FGeneral%2FTeam%20%2D%20Ken&viewid=8deb5145%2D267d%2D40c3%2D8a6d%2D64940906ae78
```
