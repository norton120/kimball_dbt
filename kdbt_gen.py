#!/usr/bin/python

'''
    INTENT: kdbt_gen generates stub model files from templates.
    
        Templates live in the /templates folder with a .template file extension.

    ARGS: invoke with an argument string. see help method
'''
import os
import yaml
import sys

class kdbt_gen:
    
    def __init__(self, args):
        
        self._model_type = args[1].lower()
        self._model_name = args[2].upper()
        
        ## check for help
        for arg in args:
            if arg[3:].upper() == 'HELP':
                self.help() 

        ## additional args
        kwargs = {}
        
        ## for each additional arg, if the arg has double dashes,
        ## take it as a key. If not feed it as a value.

        for index, arg in enumerate(args[3:]):
            if arg[:2] == '--':

                ## if 2 keys are back to back error out
                if args[index+4] and args[index+4][:2] == '--':
                    raise ValueError('Incorrect argument values')    
        
                ## if a key has no value, set it to True.
                try:
                    kwargs[arg[2:]] = args[index+4]
                except: 
                    kwargs[arg[2:]] = True
        

        ## check to see if the distructive flag was set
        self._destructive = kwargs['distructive'] if 'destructive' in kwargs else False

        if self._model_type == 'screen':
            
            ## qualify the model name with _SCREEN suffix
            self._model_name += '_SCREEN' if self._model_name[-7:] != '_SCREEN' else ''
            

            existing_model = self.check_for_existing_model(self._model_name)

            ## grab the path arguments. Defaults are RAW.ERP. Entity is required. 
            self._database = kwargs['database'] if 'database' in kwargs else 'RAW'
            self._schema = kwargs['schema'] if 'schema' in kwargs else 'ERP'
            self._entity = kwargs['entity'] 

            if self._destructive or not existing_model:
                self.create_new_model(self._model_type, self._model_name, kwargs['entity'], kwargs['schema'],  kwargs['database'])
                self._print_success(self._model_name)
            else:
                self._print_exists(existing_model)

        elif self._model_type == 'audit':
            pass
        elif self._model_type == 'staging_quality':
            pass
        elif self._model_type  == 'partial':
            pass
        elif self._model_type == 'production':
            pass
        else:
            raise ValueError('{} is not a valid model type.'.format(self._model_type))



    def check_for_existing_model(self, model_name):
        ''' 
            INTENT: checks to see if a model of the same name already exists anywhere in the DAG.
            ARGS:   
                - model_name (string) the model name to search for.
            RETURNS: (string) matching path name if the model was found, False if it wasn't
        '''
        ## get the root path
        root_path = self._dbt_root()
        
        ## read the config file and get all the model paths
        model_folders = yaml.load(open(root_path + os.path.sep + 'dbt_project.yml'))['source-paths']
        
        for folder in model_folders:
            path_under_test = root_path + os.path.sep + folder + os.path.sep + model_name + '.sql'

            if os.path.isfile(path_under_test):
                return path_under_test
            
        return False



    def create_new_model(self, model_type, model_name, entity = None, schema = None, database = None):
        '''
            INTENT: creates a new aptly-named file from the appropriate template file.
            ARGS: 
                - model_type (string) determines which template to copy and the naming rules.
                - model_name (string) the unqualified name of the model
            RETURNS: boolean True on success
        '''
        
        ## fill in the template values
        with open(self._dbt_root() + os.path.sep + 'templates' + os.path.sep + model_type + '.template', 'r') as template_text:
            template_text = template_text.read()
            
            ## model_type specific formatting
        if model_type == 'screen':
            template_text = template_text.replace(
                                '<database>', database).replace(
                                    '<schema>', schema).replace(
                                        '<entity>', entity).replace(
                                            '<model_name>', model_name)

        target = open(self._dbt_root() + os.path.sep + self.format_for_folder_name(model_type) + os.path.sep + model_name + '.sql', 'wr')
        
        success = target.write(template_text)
        
        target.close()
        template_text.close()
        return success



    def _dbt_root(self):
        '''
            INTENT: find the dbt project root
            ARGS: none
            RETURNS: (string) the full path of the dbt project root folder
        '''
        def find_dbt_project_yml(p):
            if os.path.isfile(p + '/dbt_project.yml'):
                return p + '/'
            elif p == '/':
                return False
            else:
                return find_dbt_project_path(os.path.split(p)[0])
        
        project_root_path = find_dbt_project_yml(os.path.abspath('.'))

        if not project_root_path:
            raise FileNotFoundError('This is not a DBT project. Please call kdbt_gen from inside a DBT project.')
        else:
            return project_root_path



    def format_for_folder_name(self,name):
        '''
            INTENT: pluralizes as needed for folder stucture
            RETURNS (string) the formatted name
        '''
        if name in ('screen','audit','partial'):
            return name + 's'
        else:
            return name



    def _print_success(self, path):
        '''
            INTENT: prints to the console the success message
            RETURNS: VOID
        '''
        print('Model {} successfully created.'.format(path))




    def _print_exists(self, path):
        '''
            INTENT: prints to the console where the conflicting model exists
            RETURNS VOID
        '''
        print('Sorry! That model already exists at {}'.format(os.path.normpath(path)))

    
    
    def help(self):
        print(
        '''
        usage: kdbt_gen.py <model_type> <model_name> [--option_name option_value]

        model_type: the model template to check and generate. Options are screen, audit, staging_quality, production
        model_name: the file name of the new model
        options:
            database: the source data database
            schema: the source data schema
            entity: the source data entity
            distructive: boolean defaults true to force overwrite of existing models
        '''                
        )
        return

if __name__ == "__main__":
    kdbt_gen(sys.argv)
