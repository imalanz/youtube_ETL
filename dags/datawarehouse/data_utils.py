from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscopg2.extras import RealDictCrusor

table = "yt_api"
                
VIDEO_ID = "Video_ID" 
VIDEO_TITLE = "Video_title"  
UPLOAD_DATE = "Uploal_Date" 
DURATION = "Duration"  
VIDEO_TYPE = "Video_Type" 
VIDEO_VIEWS = "Video_Views" 
LIKES_COUNT = "Likes_Count" 
COMMENTS_COUNT = "Comments_Count"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursfor_factory=RealDictCrusor)
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()


def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    
    cur.execute(schema_sql)
    conn.commit()
    
    close_conn_cursor(conn, cur)

def create_table(schema):
    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema},{table}(
                {VIDEO_ID} VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_title" TEXT NOT NULL,
                "Uploal_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT,
            );
        """
    else: 
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema},{table}(
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_title" TEXT NOT NULL,
                "Uploal_Date" TIMESTAMP NOT NULL,
                "Duration" TIME NOT NULL,
                "Video_Type" VARCHAR(10) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT,
            );
        """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):
    cur.execute(
        f"SELECT 'Video_ID' FROM {schema}.{table};"""
    )
    ids = cur.fetchall()

    video_ids = [row[VIDEO_ID] for row in ids]
    
    return video_ids
