import os
import logging
from datetime import datetime
from parsons import Table, Redshift

# Define the default logging config for Canales scripts
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')

TMC_CIVIS_DATABASE = 815
TMC_CIVIS_DATABASE_NAME = 'TMC'

RESERVED_WORDS = ['AES128', 'AES256', 'ALL', 'ALLOWOVERWRITE', 'ANALYSE', 'ANALYZE', 'AND', 'ANY',
                  'ARRAY', 'AS', 'ASC', 'AUTHORIZATION', 'BACKUP', 'BETWEEN', 'BINARY',
                  'BLANKSASNULL', 'BOTH', 'BYTEDICT', 'BZIP2', 'CASE', 'CAST', 'CHECK', 'COLLATE',
                  'COLUMN', 'CONSTRAINT', 'CREATE', 'CREDENTIALS', 'CROSS', 'CURRENT_DATE',
                  'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'CURRENT_USER_ID',
                  'DEFAULT', 'DEFERRABLE', 'DEFLATE', 'DEFRAG', 'DELTA', 'DELTA32K', 'DESC',
                  'DISABLE', 'DISTINCT', 'DO', 'ELSE', 'EMPTYASNULL', 'ENABLE', 'ENCODE', 'ENCRYPT',
                  'ENCRYPTION', 'END', 'EXCEPT', 'EXPLICIT', 'FALSE', 'FOR', 'FOREIGN', 'FREEZE',
                  'FROM', 'FULL', 'GLOBALDICT256', 'GLOBALDICT64K', 'GRANT', 'GROUP', 'GZIP',
                  'HAVING', 'IDENTITY', 'IGNORE', 'ILIKE', 'IN', 'INITIALLY', 'INNER', 'INTERSECT',
                  'INTO', 'IS', 'ISNULL', 'JOIN', 'LEADING', 'LEFT', 'LIKE', 'LIMIT', 'LOCALTIME',
                  'LOCALTIMESTAMP', 'LUN', 'LUNS', 'LZO', 'LZOP', 'MINUS', 'MOSTLY13', 'MOSTLY32',
                  'MOSTLY8', 'NATURAL', 'NEW', 'NOT', 'NOTNULL', 'NULL', 'NULLS', 'OFF', 'OFFLINE',
                  'OFFSET', 'OLD', 'ON', 'ONLY', 'OPEN', 'OR', 'ORDER', 'OUTER', 'OVERLAPS',
                  'PARALLEL', 'PARTITION', 'PERCENT', 'PERMISSIONS', 'PLACING', 'PRIMARY', 'RAW',
                  'READRATIO', 'RECOVER', 'REFERENCES', 'RESPECT', 'REJECTLOG', 'RESORT', 'RESTORE',
                  'RIGHT', 'SELECT', 'SESSION_USER', 'SIMILAR', 'SOME', 'SYSDATE', 'SYSTEM',
                  'TABLE', 'TAG', 'TDES', 'TEXT255', 'TEXT32K', 'THEN', 'TIMESTAMP', 'TO', 'TOP',
                  'TRAILING', 'TRUE', 'TRUNCATECOLUMNS', 'UNION', 'UNIQUE', 'USER', 'USING',
                  'VERBOSE', 'WALLET', 'WHEN', 'WHERE', 'WITH', 'WITHOUT']

def set_env_var(name, value, overwrite=False):
    """
    Set an environment variable to a value.
    `Args:`
        name: str
            Name of the env var
        value: str
            New value for the env var
        overwrite: bool
            Whether to set the env var even if it already exists
    """
    # Do nothing if we don't have a value
    if not value:
        return

    # Do nothing if env var already exists
    if os.environ.get(name) and not overwrite:
        return

    os.environ[name] = value

def setup_environment(redshift_parameter="REDSHIFT", aws_parameter="AWS"):
    """
    Sets up environment variables needed for various common services used by our scripts.
    Call this at the beginning of your script.
    `Args:`
        redshift_parameter: str
            Name of the Civis script parameter holding Redshift credentials. This parameter
            should be of type "database (2 dropdown)" in Civis.
        aws_parameter: str
            Name of the Civis script parameter holding AWS credentials.
        copper_parameter: str
            Name of the Copper script parameter holding Copper user email and API key
        google_sheets_parameter: str
            Name of the Google Sheets script parameter holding a base64-encoded Google
            credentials dict
    """

    env = os.environ

    # Civis setup

    set_env_var('CIVIS_DATABASE', str(TMC_CIVIS_DATABASE))

    # Redshift setup

    set_env_var('REDSHIFT_PORT', '5432')
    set_env_var('REDSHIFT_DB', 'dev')
    set_env_var('REDSHIFT_HOST', env.get(f'{redshift_parameter}_HOST'))
    set_env_var('REDSHIFT_USERNAME', env.get(f'{redshift_parameter}_CREDENTIAL_USERNAME'))
    set_env_var('REDSHIFT_PASSWORD', env.get(f'{redshift_parameter}_CREDENTIAL_PASSWORD'))

    # AWS setup

    set_env_var('S3_TEMP_BUCKET', 'parsons-tmc')
    set_env_var('AWS_ACCESS_KEY_ID', env.get(f'{aws_parameter}_USERNAME'))
    set_env_var('AWS_SECRET_ACCESS_KEY', env.get(f'{aws_parameter}_PASSWORD'))

def main():

    # Instantiate class for TMC Redshift
    setup_environment()
    rs = Redshift()

    # Get a single timestamp to "batch" together results
    today = datetime.now()
    today_str = datetime.strftime(today, '%Y-%m-%d %H:%M:%S')

    # Get the list of tables to compare
    comp_tables = rs.query("SELECT * FROM coc.dbsync_config")

    for row in comp_tables:

        tbl = row['source']
        object_name = tbl.replace('core_', "")

        if rs.table_exists(f"coc_ak.{tbl}") and rs.table_exists(f"coc_ak.{tbl}_dbs"):
            logger.info(f"Comparing results for {tbl}")
            
            updated_sql = f"MAX({row['sortkey']})::TIMESTAMP AS"
            null_cast = ""
            if row['sortkey'] is None:
                updated_sql = "'' AS"
                null_cast = "NULL AS"
            
            sql = f"""
                INSERT INTO coc_ak.sync_comparison (
                    WITH 
                    dms_{object_name} AS (
                        SELECT '{tbl}'::TEXT AS table_name
                        , {updated_sql} dms_updated_at
                        , COUNT(*) AS dms_row_count
                        FROM coc_ak.{tbl}
                    )

                    , dbs_{object_name} AS (
                        SELECT '{tbl}'::TEXT AS table_name
                        , {updated_sql} dbs_updated_at
                        , COUNT(*) AS dbs_row_count
                        FROM coc_ak.{tbl}_dbs
                    )

                    SELECT '{today_str}'::TIMESTAMP AS snap_time
                    , table_name
                    , {null_cast} dms_updated_at
                    , {null_cast} dbs_updated_at
                    , dms_row_count
                    , dbs_row_count
                    FROM dms_{object_name}
                    LEFT JOIN dbs_{object_name}
                    USING(table_name)
                )
            """
            rs.query(sql)

        else:
            logger.info(f"{tbl} not being handled by both syncs. Skipping.")

if __name__ == '__main__':
    main()
