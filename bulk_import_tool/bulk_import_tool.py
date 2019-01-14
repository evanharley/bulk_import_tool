import pyodbc
import openpyxl
from tkinter import filedialog

# Tools for the Bulk Import of Natural History Specimens
class import_tools():

    def __init__(self, *args, **kwargs):
        
        # self.data_filename = filedialog.askopenfilename(title='Open', defaultextension='.xlsx', 
        #                                       filetypes=[('Excel Files', '*.xlsx')])
        self.data_filename = "C:\\Users\\evharley\\source\\repos\\bulk_import_tool\\bulk_import_tool\\" +\
            "IMM import template - test.xlsx"
        try:
            self.data_file = openpyxl.load_workbook(self.data_filename)
        except FileNotFoundError:
            return None
        self.ws = self.data_file.active
        # Discipline should be gotten from user at the start of the import
        # so when coding GUI it should be included
        self.discipline = 'inv'
        connection = pyodbc.connect('DSN=IMMTest; Trusted_Connection=yes;')
        self.cursor = connection.cursor()

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
            relevant_cols = [i for i in range(30, 73) if i != 48]
            return relevant_cols
        elif method == 'Events':
            relevant_cols = [i for i in range(6, 30)]
            return relevant_cols

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

    def _generate_sites(self):
        # Generates new sites for import, from unique sites in the import spreasheet
        new_site_id = self._get_max_site_id()
        relevant_cols = self._find_relevant_column('Sites')
        key_row = self.ws[2]
        generated_sites = {}
        for row in range(4, self.ws.max_row + 1):
            new_site_id[1] = str(int(new_site_id[1]) + 1)
            site_id = new_site_id[0] + new_site_id[1]
            if generated_sites == {}:
                generated_sites[site_id] = {}
                for index in relevant_cols:
                    generated_sites[site_id][key_row[index].value] = self.ws[row][index].value
            else:
                site = {}
                for index in relevant_cols:
                    site[key_row[index].value] = self.ws[row][index].value
                if site not in generated_sites.values():
                    generated_sites[site_id] = site

        return generated_sites

    def _get_max_site_id(self):
        prefix_query = "Select geo_site_prefix from NHDisciplineType where discipline_cd = '{}'".format(self.discipline)
        prefix = self.cursor.execute(prefix_query).fetchall()[0][0]
        query = "Select max(convert(int, SUBSTRING(collector_site_id, 3, 100))) from GeographicSite " + \
        "where discipline_cd = '{}' and substring(collector_site_id, 1, 2) = '{}'".format(self.discipline, prefix)
        result = self.cursor.execute(query).fetchone()
        max_site_id = [prefix, str(result[0])]
        return max_site_id

    def _get_max_event_id(self):
        prefix_query = "Select coll_event_prefix from NHDisciplineType where discipline_cd = '{}'".format(self.discipline)
        prefix = self.cursor.execute(prefix_query).fetchall()[0][0]
        query = "Select max(convert(int, SUBSTRING(event_num, 3, 100))) from CollectionEvent " + \
        "where discipline_cd = '{}' and substring(event_num, 1, 2) = '{}'".format(self.discipline, prefix)
        result = self.cursor.execute(query).fetchone()
        max_event_id = [prefix, str(result[0])]
        return max_event_id

    def _generate_events(self):
        # Generates new collection events for import, from the unique events in the import spreadsheet
        generated_events = {}
        new_event_id = self._get_max_event_id()
        relevant_cols = self._find_relevant_column('Events')
        key_row = self.ws[2]
        for row in range(4, self.ws.max_row + 1):
            new_event_id[1] = str(int(new_event_id[1]) + 1)
            event_id = new_event_id[0] + new_event_id[1]
            working_row = self.ws[row]
            if event_id not in generated_events.keys():
                generated_events[event_id] = {key_row[col].value: working_row[col].value 
                                                    for col in relevant_cols if col not in [11, 22]}
                generated_events[event_id]['Collector'] = self._split_persons(working_row[22].value)
            else:
                generated_events[event_id]['Collector'].extend(self._split_persons(working_row[22].value))
                generated_events[event_id]['Collector'] = list(set(generated_events[event_id]['Collector']))
                
        return generated_events

    def _write_persontaxa(self, data, section):
        row = 1
        col = 'A'
        work_sheet = self.data_file[section]
        sheet_ref = sheet_ref = chr(ord(col)) + str(row)
        work_sheet[sheet_ref] = section
        sheet_ref = sheet_ref = chr(ord(col) + 1) + str(row)
        work_sheet[sheet_ref] = section + '_ids'
        row += 1
        for key in data.keys():
            sheet_ref = col + str(row)
            work_sheet[sheet_ref] = key
            for i in range(len(data[key])):
                sheet_ref = chr(ord(col) + 1 + i) + str(row)
                work_sheet[sheet_ref] = data[key][i]
            row += 1
        return 0

    def _write_siteevent(self, data, section):
        row = 1
        col = 1
        worksheet = self.data_file[section]
        if section == 'Event':
            worksheet.cell(row = row, column = col, value = 'Event Number') 
        else:
            worksheet.cell(row = row, column = col, value =  'Geosite_id')
        first_record = data[list(data.keys())[1]]
        keys = [key for key in first_record.keys()]
        for key in data.keys():
            if row == 1:
                worksheet.cell(row = row + 1, column = 1, value = key)
            else:
                worksheet.cell(row = row, column = 1, value = key)
            if row == 1:
                for i in range(len(keys)):
                    worksheet.cell(row = row, column = col + 1 + i, value = keys[i])
                row += 1

            for i in range(len(keys)):
                if data[key][keys[i]] is None:
                    continue
                else:
                    if isinstance(data[key][keys[i]], list):
                        names = '; '.join(data[key][keys[i]])
                        worksheet.cell(row = row, column = col + 1 + i, value = names)
                    else:
                        worksheet.cell(row = row, column = col + 1 + i, value = data[key][keys[i]])
            row += 1
        return 0

    def write_spreadsheet(self):
        # Writes the found and generated data to new tabs in the import spreadsheet
        if self.data_file.sheetnames != ['IMM_template', 'Person', 'Taxon', 'Site', 'Event']:
            self.data_file.create_sheet('Person')
            self.data_file.create_sheet('Taxon')
            self.data_file.create_sheet('Site')
            self.data_file.create_sheet('Event')

        persons = self._find_persons()
        self._write_persontaxa(persons, 'Person')

        taxa = self._find_taxa()
        self._write_persontaxa(taxa, 'Taxon')

        sites = self._generate_sites()
        self._write_siteevent(sites, "Site")

        events = self._generate_events()
        self._write_siteevent(events, 'Event')

        self.data_file.save(self.data_filename)
        return 0 


    def _test_spreadsheet(self):
        key_row = self.ws[3]
        test_results = {}
        disciplines = {
            'bot': 'Botany',
            'ent': 'Entomology',
            'geo': 'Geology',
            'her': 'Herpetology',
            'ich': 'Ichthyology',
            'inv': 'Invertebrate',
            'mam': 'Mammalogy',
            'orn': 'Ornithology',
            'pal': 'Paleontology'
            }
        for key in key_row:
            value = key.value
            query_table = '[' + value[:value.find('.')] + ']'
            query_field = '[' + value[value.find('.') + 1 :] + ']'
            if query_table == '[[DISCIPLINE]]':
                query_table = '[' + disciplines[self.discipline] + 'Item' + ']'
            query = "select {} from {}".format(query_field, query_table)
            try:
                test = self.cursor.execute(query).fetchone()
                test_results[value] = True
            except:
                test_results[value] = False
        return test_results
    
    def _add_ids(self):
        print("TEST")

    def write_to_db():
        # Writes the data from the import spreadsheet to the database
        return 0

if __name__ == '__main__':
    impt = import_tools()
    if impt is None:
        print("You didn't choose a file")
    else:
        impt.write_spreadsheet()

