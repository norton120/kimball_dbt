from sqlalchemy import create_engine
import pandas as pd
import pandas_profiling as pp
import sys
import time

'''
    INTENT: query the slave server and generate an HTML document
        profiling the given entity. 

    SIGNATURE:
        python stat_profile_gen.py <entity> [method]
        - entity (string) the table or view to hit
        - method (string) accepts bernoulli (default), random (for non-tables) or all (the whole entity)
    RETURNS:
        boolean success or failure of process
''' 
def stat_profile_gen(*args):
    entity = args[1].lower()

    if len(args) > 2:
        method = args[2]
    else:
        method = 'bernoulli'

    try:
        engine = create_engine('postgresql://fivetran@localhost:2345/ecom')
    except:
        print('failed to connect to postgres. Is your jumper SSH connected?')
        sys.exit()
    
    query_string = '''
                    SELECT 
                        * 
                    FROM {0}'''.format(entity)
    
    bernoulli = ''' 
                    TABLESAMPLE BERNOULLI ((
                        SELECT 
                            CASE 
                                WHEN (SELECT COUNT(*) FROM {0}) <= 1000 THEN 100
                                WHEN (SELECT COUNT(*) FROM {0}) BETWEEN 1001 AND 100000 THEN 10
                                ELSE 1
                            END))'''.format(entity)
    
    random = '''
                WHERE RANDOM() >= 0.1 LIMIT 100000'''
    
    ## assemble the record based on the method passed
    if method == 'bernoulli':
        query_string += bernoulli
    elif method == 'random':
        query_string += random
    elif method == 'all':
        pass
    else:
        print('invalid method of selection')
        sys.exit()

    print('attempting to sample entity {} via {}'.format(entity, method))
    timer = time.time()
    
    try:
        data = pd.read_sql_query(query_string, con=engine)
    except:
        print('unable to load records for {}'.format(entity))
        sys.exit()

    print('analyzing {} rows from {}'.format(data.shape[0], entity))
    
    try:
        pp.check_correlation = False
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
