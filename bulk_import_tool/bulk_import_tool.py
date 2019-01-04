import pyodbc
import openpyxl
from tkinter import filedialog

# Tools for the Bulk Import of Natural History Specimens
class import_tools():

    def __init__(self, *args, **kwargs):
        
        # data_filename = filedialog.askopenfilename(title='Open', defaultextension='.xlsx', 
        #                                       filetypes=[('Excel Files', '*.xlsx')])
        data_filename = "C:\\Users\\evharley\\source\\repos\\bulk_import_tool\\bulk_import_tool\\" +\
            "IMM import template - test.xlsx"
        try:
            self.data_file = openpyxl.load_workbook(data_filename)
        except FileNotFoundError:
            return None
        self.ws = self.data_file.active
        connection = pyodbc.connect('DSN=IMMTest; Trusted_Connection=yes;')
        self.cursor = connection.cursor()
        del(data_filename)

    def _find_persons(self):
        # Return all unique persons in the spreadsheet for import
        # Persons to be a dict in format {personName: [ids]}
        person_cols = self._find_relevant_column('Person')
        names = []
        persons = {}
        for row in range (4, self.ws.max_row + 1):
            row_data = self.ws[row]
            for column in person_cols:
                if row_data[column].value is not None:
                    names.extend(self._split_persons(row_data[column].value))
                else:
                    continue
        names = list(set(names))

        for name in names:
            query = "Select person_id from Person where search_name = '{}'".format(name)
            results = self.cursor.execute(query).fetchall()
            persons[name] = []
            if results != []:
                for i in range(len(results)):
                    persons[name].append(results[i][0])
        return persons
        
    def _find_relevant_column(self, method):
        # Return the index of the columns relevant to the _find methods above.
        # Values in a list of indices
        headder_row = self.ws[3]
        relevant_cols = []
        table_id = ''
        if method == 'Person':
            table_id = ['Person.person_id']
        elif method == 'Taxon':
            table_id = ['Taxonomy.taxon_id']
        elif method == 'Sites':
            table_id = ['GeographicSite.latitude', 'GeographicSite.longitude']
        elif method == 'Events':
            table_id = ['CollectionEvent.event_num']

        for col in range(1, len(headder_row)):
            if headder_row[col].value in table_id and col not in relevant_cols:
                relevant_cols.append(col)
        return relevant_cols
        
    def _split_persons(self, person_names):
        # Returns the split value of person names where a dilineator is present
        names = []
        delineators = ",;:|/\\"
        if any(char in person_names for char in delineators):
            person_names = person_names.replace(';', ',').replace(':', ',').replace('|', ',')
            names = [name.strip(' ') for name in person_names.split(',')]
        else:
            names = person_names
        return names

    def _find_taxa(self):
        # Returns the unique taxa for each taxon in the import spreadsheet
        # Taxa to be a dict in format {scientificName: [ids]}
        taxon_cols = self._find_relevant_column('Taxon')
        taxa = {} 
        sns = []
        for row in range (4, self.ws.max_row + 1):
            row_data = self.ws[row]
            for column in taxon_cols:
                if row_data[column].value is not None:
                    sns.append(row_data[column].value)
                else:
                    continue
        sns = list(set(sns))

        for sn in sns:
            taxa[sn] = []
            taxa[sn].extend(self._query_taxa(sn))

        return taxa

    def _query_taxa(self, sn):
        # Returns the taxon ids of a scientific name
        if not sn.endswith('sp.'):
            query = "Select * from ScientificName where scientific_name ='{}'".format(sn)
        else:
            sn = sn[: sn.find(' ')]
            query = "Select taxon_id, term from taxon where term = '{}'".format(sn)
        results = self.cursor.execute(query).fetchall()
        if results != []:
            taxa = [result[0] for result in results]
        else:
            taxa = ["NEW?"]
            
        return taxa
          
        
    def _find_site(self):
        # Returns all existing Geographic Sites with the exact lat long value present in 
        # import spreadsheet
        # Sites to be a dict in format {latlong: [ids]}
        sites = {} 
        return sites

    def _find_events(self):
        # Returns all existing Collection Events with the same Event Number
        # Events to be a dict in format {eventNm: [ids]}
        events = {} 
        return events

    def _generate_sites(self):
        # Generates new sites for import, from unique sites in the import spreasheet
        generated_sites = {}
        return generated_sites

    def _generate_events(self):
        # Generates new collection events for import, from the unique events in the import spreadsheet
        generated_events = {}

    def write_spreadsheet(self):
        # Writes the found and generated data to new tabs in the import spreadsheet
        persons = self._find_persons()
        return 0

    def write_to_db():
        # Writes the data from the import spreadsheet to the database
        return 0

if __name__ == '__main__':
    impt = import_tools()
    if impt is None:
        print("You didn't choose a file")
    else:
        impt.write_spreadsheet()

