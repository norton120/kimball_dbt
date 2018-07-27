from sqlalchemy import create_engine
import pandas as pd
import pandas_profiling as pp
import sys
import time

'''
    INTENT: query the slave server and generate an HTML document
        profiling the given entity. 

    SIGNATURE:
        python stat_profile_gen.py <entity> [pii_colummn another_pii_column]
        - entity (string) the table or view to hit
        - pii_column (string) each column name to exclude from freq 
    
    RETURNS:
        boolean success or failure of process
''' 
def stat_profile_gen(*args):
    entity = args[1].lower()
    
    pii_concerns = []

    for arg in args[2:]:
        pii_concerns.append(arg.lower())            


    try:
        engine = create_engine('postgresql://fivetran@localhost:2345/ecom')
    except:
        print('failed to connect to postgres. Is your jumper SSH connected?')
        sys.exit()
    
    query_string = '''
                    SELECT 
                        * 
                    FROM {0} 
                    TABLESAMPLE BERNOULLI ((
                        SELECT 
                            CASE 
                                WHEN (SELECT COUNT(*) FROM {0}) <= 1000 THEN 100
                                WHEN (SELECT COUNT(*) FROM {0}) BETWEEN 1001 AND 100000 THEN 10
                                ELSE 1
                            END))'''.format(entity)

    print('attempting to sample entity {}'.format(entity))
    timer = time.time()
    
    try:
        data = pd.read_sql_query(query_string, con=engine)
    except:
        print('unable to load records for {}'.format(entity))
        sys.exit()

    print('analyzing {} rows from {}'.format(data.shape[0], entity))
    
    try:
        prof = pp.ProfileReport(data)

        ## TODO: remove PII from freq report
        #for col in pii_concerns:
        #    prof['freq'][col] = 'obfuscated due to PII concerns'
    
        prof.to_file(outputfile = entity.upper() + '.html')  

    except:
        print('something went wrong when profiling the data for {} :('.format(entity))
        sys.exit()

    print('profile file {}.html has been created in {} seconds!'.format(entity.upper(), round(time.time() - timer) ))


if __name__ == '__main__':
    stat_profile_gen(*sys.argv)
