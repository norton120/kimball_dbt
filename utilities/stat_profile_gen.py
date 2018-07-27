from sqlalchemy import create_engine
import pandas as pd
import pandas_profiling as pp
import sys
import time

'''
    INTENT: query the slave server and generate an HTML document
        profiling the given entity. 

    ARGS: 
        - entity (string) the name of the entity to profile
    
    RETURNS:
        boolean success or failure of process
''' 
def stat_profile_gen(entity):
    entity = entity.lower()    


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
                                ELSE 10
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

        ## TODO: add PII wash here before creating the html doc    
        prof.to_file(outputfile = entity.upper() + '.html')  

    except:
        print('something went wrong when profiling the data for {} :('.format(entity))
        sys.exit()

    print('profile file {}.html has been created in {} seconds!'.format(entity.upper(), round(time.time() - timer) ))


if __name__ == '__main__':
    stat_profile_gen(sys.argv[1])
