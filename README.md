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
2. Initiate a new MySQL server, with username and password. By convention, they should both be 'root'.
3. Add two environment variables to your computer, named `MYSQL_USER` and `MYSQL_PASSWORD`. Their values should correspond to the username and password set on step 2.
4. Create a database in the MySQL server called `causaly_knowledge`, and inside create a table called `metadata` with the columns seen in the description of Iteration 1, step 1. 
5. Populate the `metadata` table with the rows found in `data/metadata.csv`.
6. You are all set!



## Tests
In order to run the tests, use the following command, while in the root of the project:
`python -m pytest -v tests/`