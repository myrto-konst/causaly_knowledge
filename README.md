#Causaly Knowledge Playground

This project aims to give developers a space to test simple features/changes to the integration between locally saved data and a simple MySQL server. The project has been divided into several iterations: 

### Iteration 1
1. Create a MySQL database called `causaly_knowledge` on a MySQL server and add a table called `metadata` with the following columns: `article_name`, `article_ID`, `journal_name`
2. Populate the metadata table with 10 articles on (happiness)[https://pubmed.ncbi.nlm.nih.gov/)] 
3. Write a Python program that reads a local csv file, connects to the MySQL server and adds the local data to the `metadata` table in the MySQl server

Disclaimer: The table columns in the local pandas table are `name`, `ID`, `journal`, so the columns need to be renamed when fetching/inserting data to the `metadata` table on the server.

### Iteration 2
1. Same as Iteration 1, but doesn't allow already existent rows to be added to the server's `metadata` table. (Deduplication)


Running `main.py` should read the `extra_metadata.csv` file located in the data folder and then add it to your already created mySQL `metadata` table.

## Set Up 
1. Create a virtual environment at the root called `causaly_knowledge`.
2. Use `pip install` to install the following packages:
- Python: 3.11.1
- mysql-connector-python: 8.0.31
- pandas: 1.5.2
- pip: 22.3.1
- pytest: 7.2.0
3. Initiate a new MySQL server, with username and password 'root'. If you decide to change either of those values, change the values in `server_credentials` in the `constants.py` accordingly.
4. Create a database in the MySQL server called `causaly_knowledge`, and inside create a table called `metadata` with the columns seen in the description of Iteration 1, step 1. 
5. Populate the `metadata` table with the rows found in `data/metadata.csv`.
6. You are all set!



## Tests
In order to run the tests, use the following command, while in the root of the project:
`python -m pytest -v tests/`