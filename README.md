# CNPJ Data Transformation

## 📋 Description
Data transformation aims to enhance the visualization of CNPJ data provided by the Brazilian government, making it easier for clients to access and utilize the data effectively. To achieve this, the following components have been developed:

1. _CSV to Parquet File Converter_: This tool reduces file size by 87% compared to the original formats, optimizing storage and data retrieval.

2. _Parquet File Unification_: The unification process combines separate Parquet files representing different "tables" into a cohesive view, enabling a unified and streamlined visualization.

3. _Streamlit Data Visualization_: After the unification step, users can efficiently and easily visualize all the desired items based on their chosen filters or even access the initial items provided by the system.

These components work together to provide an efficient and user-friendly data exploration experience, enhancing the accessibility and usability of government-provided CNPJ data. 

## 💻 Prerequisites

Before getting started, please ensure that you meet the following prerequisites:

* You have Java installed on your machine.
* You have Python version 3.10 or higher installed.
* You have the Pyspark package installed.
* You have the Streamlit package installed.
* You have the streamlit-option-menu package installed.


## 🐍 How to Run
To execute the project on your machine:

$ cd src/
$ python main.py