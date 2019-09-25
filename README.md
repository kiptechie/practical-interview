# Interview Question
- A link was provided that contains a json file which contains data extracted from Calltronix clients


## Description
- Create an algorithm that decodes the json  file provided and inserts the content  into a single table in the database .
- Perform database normalization on the  table upto a realistic normaization Level based on your analysis.
- From the normalization outcome of part two above, create joins and retrieve and display the information on a simple UI.


### Development set up

-   Check that python 3 is installed:

    ```
    python --version
    >> Python 3.7.1
    ```

-   Install virtualenv:

    ```
    pip3 install virtualenv venv
    ```

-   Check that postgres is installed:

    ```
    postgres --version
    >> postgres (PostgreSQL) 10.1
    ```

-   Create a .env file outside the app folder and update the variables accordingly:

    ```
    Add the following
    >> DATABASE_URL="dbname={'your-db-name} host='127.0.0.1' port='5432' user={'user'} password={'password'}"

    ```

-   Run source .env:

    ```
    source .env

    ```

-   Clone the the repo and cd into it:

    ```
    git clone https://github.com/salma-nyagaka/practical-interview.git

    ```

-   Activate your virtual env:

    ```
    source venv/bin/activate

    ```

-   Install dependencies:

    ```
    pip3 install -r requirements.txt

    ```


-   cd into the app directory and run the application:

    ```
    python database.py

    ```

## Database schema

<br />
<img width="500" alt="database-schema" src="https://github.com/salma-nyagaka/practical-interview/blob/develop/app/schema.jpg">
<br />
