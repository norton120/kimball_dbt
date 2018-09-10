#!/usr/bin/python

## kdbt_gen generates stub model files from templates.
## See help method for info

import os
import yaml
import sys

class kdbt_gen:

    def help(self):
        print(
        '''
        usage: kdbt_gen.py <model_type> <model_name> [--option_name option_value]

        model_type: the model template to check and generate. Options are screen, staging_quality, dimension, fact, artifact
        model_name: the file name of the new model
        options:
            database: the source data database. Default RAW
            schema: the source data schema. Default ERP
            entity: the source data entity. Default <model_name>
            record_identifier: the unique identifier (primary key) for the entity. Default id
            destructive: boolean forces overwrite of existing models
        '''
        )
        sys.exit()


    def __init__(self, args):

        ## check for help
        for arg in args:
            if arg[2:].upper() == 'HELP':
                self.help()

        self._model_type = args[1].lower()
        self._model_name = args[2].upper()

        # validate model type
        if self._model_type not in ('production','staging_quality','screen','partial'):
            raise ValueError('{} is not a valid model type.'.format(self._model_type))


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
        self._destructive = kwargs['destructive'] if 'destructive' in kwargs else False

        ## grab the path arguments. Defaults are RAW.ERP. Entity is required.
        self._database = kwargs['database'] if 'database' in kwargs else 'RAW'
        self._schema = kwargs['schema'] if 'schema' in kwargs else 'ERP'
        self._record_identifier = kwargs['record_identifier'] if 'record_identifier' in kwargs else 'id'
        self._entity = kwargs['entity'] if 'entity' in kwargs else self._model_name

        ## qualify the model name with _MODEL suffix
        if self._model_type == 'screen':
            self._model_name += '_SCREEN' if self._model_name[-7:] != '_SCREEN' else ''

        elif self._model_type == 'staging_quality':
            self._model_name += '_STAGING_QUALITY' if self._model_name[-15:] != '_STAGING_QUALITY' else ''

        if self._destructive or not self.check_for_existing_model(self._model_name):
            self.create_new_model(self._model_type,
                                  self._model_name,
                                  self._entity,
                                  self._schema,
                                  self._record_identifier,
                                  self._database)

            self._print_success(self._model_name)
        else:
            self._print_exists(self.check_for_existing_model(self._model_name))




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



    def create_new_model(self, model_type, model_name, entity, schema, record_identifier, database):
        '''
            INTENT: creates a new aptly-named file from the appropriate template file.
            ARGS:
                - model_type (string) determines which template to copy and the naming rules.
                - model_name (string) the unqualified name of the model
                - entity (string) the entity to model
                - database (string) the database to model
                - schema (string) the schema to model
                - record_identifier (string) the unique identifier ie PK for the entity
            RETURNS: boolean True on success
        '''

        ## fill in the template values
        with open(self._dbt_root() + os.path.sep + 'templates' + os.path.sep + model_type + '.template', 'r') as template_text:
            template_text = template_text.read()

            ## model_type specific formatting
        template_text = template_text.replace(
                            '<database>', database).replace(
                                '<schema>', schema).replace(
                                    '<entity>', entity).replace(
                                        '<model_name>', model_name).replace(
                                            '<record_identifier>', record_identifier)

        target = open(self._dbt_root() + os.path.sep + self.format_for_folder_name(model_type) + os.path.sep + model_name + '.sql', 'wr')

        success = target.write(template_text)

        target.close()
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





if __name__ == "__main__":
    kdbt_gen(sys.argv)
