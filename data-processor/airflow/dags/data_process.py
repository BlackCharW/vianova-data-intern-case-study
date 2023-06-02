import psycopg2

def processor_callable(user, password, host, port, db, table_name,limit_num):
    
    # execute query with psycopg
    
    query = f'''
        WITH cte AS (
            SELECT country_code, cou_name_en,
                ROW_NUMBER() OVER (PARTITION BY cou_name_en ORDER BY country_code) AS row_num
            FROM "{table_name}"
            WHERE population < {limit_num}
            AND country_code IS NOT NULL
            AND cou_name_en IS NOT NULL
        )
        SELECT country_code, cou_name_en
        FROM cte
        WHERE row_num = 1
        ORDER BY cou_name_en;
    '''
    
    conn = psycopg2.connect(
        host = host,
        user = user,
        password = password,
        database = db,
        port = port
    )

    cur = conn.cursor()
    cur.execute(query)

    results = cur.fetchall()
    
    cur.close()

    return results