# Civis Container Script: https://platform.civisanalytics.com/spa/#/scripts/containers/91271562
import os
import logging
import ast
from parsons import Redshift, MySQL, Postgres, DBSync, S3

# Define the default logging config for Canales scripts
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')

TMC_CIVIS_DATABASE = 815
TMC_CIVIS_DATABASE_NAME = 'TMC'

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

def get_new_rows(table, db, primary_key, cutoff_value, offset=0, chunk_size=None):
        """
        Get rows that have a greater primary key value than the one
        provided.
        It will select every value greater than the provided value.
        """

        if cutoff_value is not None:
            where_clause = f"where {primary_key} >= '{cutoff_value}'"
        else:
            where_clause = ''

        sql = f"""
               SELECT
               *
               FROM {table}
               {where_clause}
               """

        if chunk_size:
            sql += f" LIMIT {chunk_size}"

        sql += f" OFFSET {offset}"

        return db.query(sql)

def table_sync_incremental_upsert(self, source_table, destination_table, primary_key,
                               updated_col, distinct_check=True, **kwargs):
    """
    Incremental sync of table from a source database to a destination database
    using an incremental primary key.

    `Args:`
        source_table: str
            Full table path (e.g. ``my_schema.my_table``)
        destination_table: str
            Full table path (e.g. ``my_schema.my_table``)
        if_exists: str
            If destination table exists either ``drop`` or ``truncate``. Truncate is
            useful when there are dependent views associated with the table.
        primary_key: str
            The name of the primary key. This must be the same for the source and
            destination table.
        distinct_check: bool
            Check that the source table primary key is distinct prior to running the
            sync. If it is not, an error will be raised.
        **kwargs: args
            Optional copy arguments for destination database.
    `Returns:`
        ``None``
    """

    # Create the table objects
    source_tbl = self.source_db.table(source_table)
    destination_tbl = self.dest_db.table(destination_table)

    # Check that the destination table exists. If it does not, then run a
    # full sync instead.
    if not destination_tbl.exists:
        self.table_sync_full(source_table, destination_table)

    # If the source table contains 0 rows, do not attempt to copy the table.
    if source_tbl.num_rows == 0:
        logger.info('Source table contains 0 rows')
        return None

    # Check that the source table primary key is distinct
    if distinct_check and not source_tbl.distinct_primary_key(primary_key):
        raise ValueError(f'{primary_key} is not distinct in source table.')

    # Get the max source table and destination table primary key
    source_max_updated = source_tbl.max_primary_key(updated_col)
    dest_max_updated = destination_tbl.max_primary_key(updated_col)

    # Check for a mismatch in row counts; if dest_max_pk is None, or destination is empty
    # and we don't have to worry about this check.
    if dest_max_updated is not None and dest_max_updated > source_max_updated:
        raise ValueError('Destination DB table updated later than source DB table.')

    # Do not copied if row counts are equal.
    elif dest_max_updated == source_max_updated:
        logger.info('Tables are in sync.')
        return None

    else:
        # Get count of rows to be copied.
        if dest_max_updated is not None:
            new_row_count = source_tbl.get_new_rows_count(updated_col, str(dest_max_updated))
        else:
            new_row_count = source_tbl.num_rows

        logger.info(f'Found {new_row_count} updated rows in source table since {str(dest_max_updated)}')

        copied_rows = 0
        # Copy rows in chunks.
        while copied_rows < new_row_count:
            # Get a chunk
            logger.info(f"OFFSET: {copied_rows}")
            rows = get_new_rows(source_tbl.table, self.source_db, primary_key=updated_col,
                                           cutoff_value=str(dest_max_updated),
                                           offset=copied_rows,
                                           chunk_size=self.chunk_size)

            row_count = rows.num_rows if rows else 0
            if row_count == 0:
                break

            # Copy the chunk
            self.dest_db.upsert(rows, destination_table, primary_key, **kwargs)

            # Update the counter
            copied_rows += row_count

    self._row_count_verify(source_tbl, destination_tbl)

    logger.info(f'{source_table} synced to {destination_table}.')

def main():

    # Instantiate class for TMC Redshift
    setup_environment()
    rs = Redshift()

    # Parse Civis Parameters from environment variables to useful objects
    temp_bucket_region = os.environ['AWS_REGION'] if os.environ['AWS_REGION']!='Default' else None

    if ',' in os.environ['TABLE_CONFIG']:
        table_config_str = os.environ['TABLE_CONFIG'].replace(', ',',')
        table_config = list(ast.literal_eval(table_config_str)) if '},' in table_config_str else [ast.literal_eval(table_config_str)]
    else:
        table_config = rs.query(f"SELECT * FROM {os.environ['TABLE_CONFIG']}")
    reverse = False if os.environ['DIRECTION']=='tmc_to_outside' else True

    # Instantiate other DB based on type
    if os.environ['DB_TYPE'].lower() == 'redshift':
        destination_db = Redshift(username=os.environ['DESTINATION_CREDENTIAL_USERNAME'],
            password=os.environ['DESTINATION_CREDENTIAL_PASSWORD'],
            host=os.environ['DESTINATION_HOST'],
            db=os.environ['DESTINATION_DB'],
            port=5439,
            timeout=1000,
            s3_temp_bucket='parsons-tmc',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
                                
    elif os.environ['DB_TYPE'].lower() == 'mysql':
        destination_db = MySQL(
            host=os.environ['DESTINATION_HOST'],
            username=os.environ['DESTINATION_CREDENTIAL_USERNAME'],
            password=os.environ['DESTINATION_CREDENTIAL_PASSWORD'],
            db=os.environ['DESTINATION_DB'])
                                
    elif os.environ['DB_TYPE'] == 'postgres':
        destination_db = Postgres(username=os.environ['DESTINATION_CREDENTIAL_USERNAME'],
            password=os.environ['DESTINATION_CREDENTIAL_PASSWORD'],
            host=os.environ['DESTINATION_HOST'],
            db=os.environ['DESTINATION_DB'],
            timeout=1000)
                                
    else:
        raise ValueError("Unsupported DB Type provided!")

    # Extend DBSync class with Yotam's upsert version
    DBSync.table_sync_incremental_upsert = table_sync_incremental_upsert

    if reverse:
        dbsync = DBSync(destination_db, rs)
    else:
        dbsync = DBSync(rs, destination_db)

    for tbl in table_config:
        logger.info(f"Running {tbl['type']} on {tbl['source']} to {tbl['destination']}...")

        if tbl['type'] == 'full_refresh':

            dbsync.table_sync_full(source_table = tbl['source'],
                               destination_table = tbl['destination'],
                               if_exists=tbl['if_exists'],
                               temp_bucket_region=temp_bucket_region,
                               alter_table=True,
                               distkey=tbl['distkey'],
                               sortkey=tbl['sortkey'])
        elif tbl['type'] == 'incremental':

            # Default to distinct check being True
            distinct_check = tbl.get('distinct_check', 'true') == 'true'

            dbsync.table_sync_incremental_upsert(source_table = tbl['source'],
                               destination_table = tbl['destination'],
                               primary_key=tbl.get('primary_key') or tbl['distkey'],
                               updated_col=tbl['sortkey'],
                               distinct_check=distinct_check,
                               temp_bucket_region=temp_bucket_region,
                               alter_table=True,
                               distkey=tbl['distkey'],
                               sortkey=tbl['sortkey'])
        else:
            raise ValueError("The only options for type are full_refresh and incremental!")


if __name__ == '__main__':
    main()