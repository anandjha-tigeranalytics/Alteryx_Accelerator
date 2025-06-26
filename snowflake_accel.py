import snowflake.connector
# connect to snowflake

conn = snowflake.connector.connect(
    account = 'HDFEYMR-TIGERANALYTICS_PARTNER',
    user = 'ANAND.JHA@TIGERANALYTICS.COM',
    password = 'A6jhamintu@1990',
    role = 'DWHBI_DEVELOPER',
    warehouse = 'DWHBI_DEVELOPER_WH',
    database = 'SNOWFLAKE_COE',
    schema = 'DATA_VAULT'
)

# create a cursor object
cursor = conn.cursor()

try:
    # execute a query
    cursor.execute("SELECT * FROM SNOWFLAKE_COE.DATA_VAULT.EMPLOYEE LIMIT 10")

    # Fetch results
    results = cursor.fetchall()

    #print results
    for row in results:
        print(row)

finally:
    print("Code Executed successfully")
