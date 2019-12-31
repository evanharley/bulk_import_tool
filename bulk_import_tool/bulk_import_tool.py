import pyodbc
import openpyxl
from pubsub import pub
from datetime import datetime

# Tools for the Bulk Import of Natural History Specimens


class ImportTools:
    '''Tool for importing data into Integrated Museum Management. Royal BC Museum's main collection management tool'''
    def __init__(self, *args, **kwargs):
        
        self.data_filename = ''
        self.discipline = ''
        self.area_cd = ''
        self._connection = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                      "Server=RBCMIMMLIVE;"
                      "Database=Mastodon;"
                      "UID=appuser;"
                      "PWD=Museum2019**;")
        self.cursor = self._connection.cursor()
        self.data_file = None
        self.ws = None
        self.keys = None
        self.max_id = self._query_item_id()
        self.max_col = None
        ## self.max_col = 138 actual value
        self.write_status = {'ArchaeologicalSite': False,
                             'ArchaeologicalCollectionEvent': False,
                             'GeographicSite': False, 
                             'CollectionEvent': False,
                             'Taxonomy': False,
                             'Triggers': False}
        self.proc_log = []

    def _get_file(self, filename):
        # Takes file name, opens the excel file as an openpyxl workbook object then sets up some
        # other necessary attributes
        self.data_filename = filename
        try:
            self.data_file = openpyxl.load_workbook(filename)
        except FileNotFoundError:
            return None
        self.ws = self.data_file['IMM_template']
        self.max_col = self._set_max_col()
        self._set_keys()
        

    def _set_max_col(self):
        if self.ws.max_column != 138 or self.ws.max_column != 29:
            if self.ws[3][10].value.startswith('Arch'):
                return 29
            else:
                return 138
        else:
            return self.ws.max_column
        

    def _set_keys(self):
        # gets the spreadsheets column headers for the purpose of having keys for the dicionaries used later
        self.keys = [self.ws[3][col] for col in range(self.max_col) if self.ws[3][col].value is not None]

    def _write_prog(self):
        # Updates the progress log for the file loaded
        prog_log = open('prog_log.log', 'a')
        prog_log.write(f'{self.data_filename}: {self.proc_log[-1]}\n')
        return 0

    def _get_prog_info(self):
        # Reads the progress log for the file loaded
        temp = []
        try:
            prog_log = open('prog_log.log', 'r')
        except FileNotFoundError:
            self.proc_log = []
            return 0
        for row in prog_log:
            if row.startswith(self.data_filename):
                temp = row[row.find(': ') + 1:].strip(' ').split(', ')
        self.proc_log = temp
        return 0

    def _find_persons(self):
        # Return all unique persons in the spreadsheet for import
        # Persons to be a dict in format {personName: [ids]}
        max = self.ws.max_row
        pub.sendMessage('UpdateMessage', message=f'Finding Unique Persons', update_count=2, new_max=max)
        person_cols = self._find_relevant_column('Person')
        names = []
        persons = {}
        for row in range(4, self.ws.max_row + 1):
            row_data = self.ws[row]
            for column in person_cols:
                if row_data[column].value is not None:
                    name = self._split_persons(row_data[column].value)
                    if isinstance(name, list):
                        names.extend(name)
                    else:
                        names.append(name)
            pub.sendMessage('UpdateMessage', message=f'Row {row}: Complete!')

        for i in range(len(names)):
            if ',' in names[i]:
                name = [thing.strip() for thing in names[i].split(',')]
                names[i] = ' '.join(name)
        names = list(set(names))
        mess = 'Querying the database for people'
        pub.sendMessage('UpdateMessage', message=mess, update_count=2, new_max=2*len(names))
        for name in names: 
            mess = f'Querying the database for {name}'
            pub.sendMessage('UpdateMessage', message=mess)
            if "'" in name:
                name = name.replace("'", "''")
            query = self._find_person_query(name, 'Person')
            try:
                results = self.cursor.execute(query).fetchall()
            except:
                print(query)
                print('pause')

            persons[name] = []
            if results != []:
                for i in range(len(results)):
                    persons[name].append(results[i][0])
            else:
                persons[name] = ['NEW?']
            mess = f'{name} not found in database' if persons[name] == ['NEW?'] \
                else f'{name} found. Person_id: {persons[name]}'
            pub.sendMessage('UpdateMessage', message=mess)
        return persons

    def _find_person_query(self, name, type = 'Person' ):
        table = type
        column = f'{type}_id'

        query = f"select {column} from {table} where search_name = '{name}'"
        return query


    def _get_full_disc(self):
        disciplines = {
            'bot': 'Botany',
            'ent': 'Entomology',
            'geo': 'Geology',
            'her': 'Herpetology',
            'ich': 'Ichthyology',
            'inv': 'Invertebrate',
            'mam': 'Mammalogy',
            'orn': 'Ornithology',
            'pal': 'Paleontology',
            'history':'Modern History',
            'archeolg':'Archaeology',
            'ethnolg':'Ethnology',
            }
        return disciplines[self.discipline]

    def _find_relevant_column(self, method):
        # Return the index of the columns relevant to the _find methods above.
        # Values in a list of indices
        relevant_cols = []
        disc = self._get_full_disc()
        table_ids = {'Person': ['Person.search_name'],
                     'Taxon': ['Taxon.term'],
                     'Events': ['CollectionEvent.', 'ArchaeologicalCollectionEvent.'],
                     'Sites': ['GeographicSite.', 'GeoSiteNote'],
                     'Item': ['Item'],
                     'NHItem': ['NaturalHistoryItem.'],
                     'FieldMeasurement': ['FieldMeasurement.'],
                     'DisciplineItem': ['[DISCIPLINE].', disc + 'Item.'],
                     'ImptTaxon': ['Taxonomy.', 'taxon_id'],
                     'Preparation': ['Preparation.'],
                     'ChemicalTreatment': ['ChemicalTreatment.'],
                     'Maker': ['MakerOrganization.org_name'],
                     'Artist': ['Artist.search_name'],
                     'HHItem':['HumanHistoryItem.'],
                     'EthItem': ['EthnologyItem.'],
                     'ArcItem': ['ArchaeologyItem.'],
                     'ArcSite': ['ArchaeologicalSite.'],
                     'Technique': ['Technique.'],
                     'Material': ['Material.'],
                     'MHist': ['ModernHistoryItem.'],
                     'Location': ['Location.location_cd'],
                     'ImptLocation':['Location.location_id']}
        table_id = table_ids[method]


        for col in range(0, len(self.keys)):
            if any(self.keys[col].value.startswith(id) for id in table_id) \
                    and col not in relevant_cols:
                relevant_cols.append(col)

        return relevant_cols
        
    def _split_persons(self, person_names):
        # Returns the split value of person names where a delineator is present
        delineators = ";:|/\\"
        person_names = person_names.replace(',', '~')
        if any(char in person_names for char in delineators):
            for char in delineators:
                if char in person_names:
                    person_names = person_names.replace(char, ',')
            names = [name.strip(' ') for name in person_names.split(',')]
        else:
            names = person_names
        if isinstance(names, list):
            names = [name.replace('~', ',') for name in names]
        else:
            names = names.replace('~', ',')
        return names

    def _find_taxa(self):
        # Returns the unique taxa for each taxon in the import spreadsheet
        # Taxa to be a dict in format {scientificName: [ids]}
        max = self.ws.max_row
        pub.sendMessage('UpdateMessage', message=f'Finding unique Taxa', update_count=2, new_max=max)
        taxon_cols = self._find_relevant_column('Taxon')
        taxa = {} 
        sns = []
        for row in range(4, self.ws.max_row + 1):
            row_data = self.ws[row]
            for column in taxon_cols:
                if row_data[column].value is not None:
                    sns.append(row_data[column].value)
                else:
                    continue
            pub.sendMessage('UpdateMessage', message=f'Row {row}: Complete!')
        sns = list(set(sns))
        mess = 'Querying database for Taxa'
        pub.sendMessage('UpdateMessage', message=mess, update_count=2, new_max=2*len(sns))
        for sn in sns:
            mess = 'Querying database for {sn}'
            pub.sendMessage('UpdateMessage', message=mess)
            taxa[sn] = []
            taxa[sn].extend(self._query_taxa(sn))
            mess = f'{sn} not found in database' if taxa[sn] == ['NEW?'] \
                else f'{sn} found. Taxon_id: {taxa[sn]}'
            pub.sendMessage('UpdateMessage', message=mess)
        return taxa

    def _query_taxa(self, sn):
        # Returns the taxon ids of a scientific name
        if not sn.endswith('sp.') or sn.find(' ') == -1:
            query = f"Select * from ScientificName where scientific_name ='{sn}'"
        else:
            sn = sn[: sn.find(' ')]
            query = f"Select taxon_id, term from taxon where term = '{sn}'"
        results = self.cursor.execute(query).fetchall()
        if results != []:
            taxa = [result[0] for result in results]
        else:
            taxa = ["NEW?"]
            
        return taxa

    def _find_locations(self):
        max = self.ws.max_row
        pub.sendMessage('UpdateMessage', message=f'Finding unique Locations', update_count=2, new_max=max)
        loc_col = self._find_relevant_column('Location')
        loc = {}
        loc_cds = []
        for row in range(4, self.ws.max_row + 1):
            row_data = self.ws[row]
            loc_cds.append(row_data[loc_col])
        loc_cds = list(set(loc_cds))
        mess = 'Querying database for Taxa'
        pub.sendMessage('UpdateMessage', message=mess, update_count=2, new_max=2*len(sns))
        for cd in loc_cds:
            loc[cd] = self._query_loc_values(cd)
            mess = f'{sn} not found in database' if taxa[sn] == ['NEW?'] \
                else f'{sn} found. Taxon_id: {taxa[sn]}'
            pub.sendMessage('UpdateMessage', message=mess)
        return loc

    def _query_loc_id(self, loc_cd):
        query = f"Select location_id from Location where location_cd ='{loc_cd}'"
        results = self.cursor.execute(query).fetchone()
        if result != []:
            return [result[0]]
        else:
            return ['NEW?']

    def _generate_sites(self):
        # Generates new sites for import, from unique sites in the import spreasheet
        area_dict = {'natural': "GeographicSite.collector_site_id",
                     'human': "ArchaeologicalSite.temporary_num"}
        max = self.ws.max_row
        pub.sendMessage('UpdateMessage', message=f'Finding unique Sites', update_count=2, new_max=max)
        site_type = area_dict[self.area_cd]
        new_site_id = self._get_max_site_id()
        relevant_cols = self._find_relevant_column('Sites')
        keys = self.ws[2]
        generated_sites = {}
        for row in range(4, self.ws.max_row + 1):
            if generated_sites == {}:
                new_site_id[1] = str(int(new_site_id[1]) + 1)
                site_id = new_site_id[0] + new_site_id[1]
                generated_sites[site_id] = {}
                for index in relevant_cols:
                    generated_sites[site_id][self.keys[index].value] = self.ws[row][index].value
                generated_sites[site_id][site_type] = site_id
                self.ws.cell(row=row, column=51, value=site_id)
            else:
                site = {}
                unique = 1
                for index in relevant_cols:
                    site[self.keys[index].value] = self.ws[row][index].value
                for item in generated_sites.keys():
                    item_site = generated_sites[item].copy()
                    item_site['GeographicSite.collector_site_id'] = None
                    if site != item_site:
                        continue
                    else:
                        unique = 0
                        matching_id = item
                        break
                    del item_site
                if unique == 1:
                    new_site_id[1] = str(int(new_site_id[1]) + 1)
                    site_id = new_site_id[0] + new_site_id[1]
                    generated_sites[site_id] = site
                    self.ws.cell(row=row, column=51, value=site_id)
                    generated_sites[site_id][site_type] = site_id
                    pub.sendMessage('UpdateMessage', message = f'{site_id} Generated!')
                else:
                    self.ws.cell(row=row, column=51, value=matching_id)
                    pub.sendMessage('UpdateMessage', message= f'Record belongs to {site_id}')
            
        return generated_sites

    def _get_max_site_id(self):
        # Queries the database for the highest collector_site_id for the discipline selected
        # returns the discipline specific prefix and the site_id
        prefix_query = f"Select geo_site_prefix from NHDisciplineType where discipline_cd = '{self.discipline}'"
        prefix = self.cursor.execute(prefix_query).fetchall()[0][0]
        query = "Select max(convert(int, SUBSTRING(collector_site_id, 3, 100))) from GeographicSite " + \
            f"where discipline_cd = '{self.discipline}' and substring(collector_site_id, 1, 2) = '{prefix}'"
        result = self.cursor.execute(query).fetchone()
        if len(str(result)) > 6:
            diff = 6 - len(str(result))
            prefix += ''.join({'0' for i in range(0, diff)})
        max_site_id = [prefix, str(result[0])]
        return max_site_id

    def _get_max_event_id(self):
        # Queries the database for the highest event_num for the discipline selected
        # returns the discipline specific prefix and the event_num
        if self.area_cd == 'natural':
            prefix_query = "Select coll_event_prefix from NHDisciplineType where discipline_cd = " +\
                                f"'{self.discipline}'"
            prefix = self.cursor.execute(prefix_query).fetchall()[0][0]
            query = "Select max(convert(int, SUBSTRING(event_num, 3, 100))) from CollectionEvent " + \
            f"where discipline_cd = '{self.discipline}' and substring(event_num, 1, 2) = '{prefix}'"
        else:
            prefix = 'CE'
            query = "Select max(convert(int, SUBSTRING(event_num, 3, 100))) from ArchaeologicalCollectionEvent "
        result = self.cursor.execute(query).fetchone()
        if len(str(result)) > 6:
            diff = 6 - len(str(result))
            prefix += ''.join({'0' for i in range(0, diff)})
        max_event_id = [prefix, str(result[0])]
        return max_event_id

    def _generate_events(self):
        # Generates new collection events for import, from the unique events in the import spreadsheet
        generated_events = {}
        max = self.ws.max_row
        pub.sendMessage('UpdateMessage', message=f'Finding unique Events', update_count=2, new_max=max)
        new_event_id = self._get_max_event_id()
        relevant_cols = self._find_relevant_column('Events')
        self.keys = self.ws[2]
        for row in range(4, self.ws.max_row + 1):
            working_row = self.ws[row]
            if generated_events == {}:
                new_event_id[1] = str(int(new_event_id[1]) + 1)
                event_id = new_event_id[0] + new_event_id[1]
                generated_events[event_id] = {}
                for index in relevant_cols:
                    generated_events[event_id][self.keys[index].value] = working_row[index].value
                generated_events[event_id]["Event Number"] = event_id
                self.ws.cell(row=row, column=14, value=event_id)
            else:
                event = {}
                unique = 1
                for index in relevant_cols:
                    event[self.keys[index].value] = working_row[index].value
                for item in generated_events.keys():
                    item_event = generated_events[item].copy()
                    item_event['Event Number'] = None
                    if event != item_event:
                        continue
                    else:
                        unique = 0
                        matching_id = item
                        break
                if unique == 1:
                    new_event_id[1] = str(int(new_event_id[1]) + 1)
                    event_id = new_event_id[0] + new_event_id[1]
                    generated_events[event_id] = event
                    self.ws.cell(row=row, column=14, value=event_id)
                    generated_events[event_id]["Event Number"] = event_id
                    pub.sendMessage('UpdateMessage', message = f'{event_id} Generated!')
                else:
                    self.ws.cell(row=row, column=14, value=matching_id)
                    pub.sendMessage('UpdateMessage', message = f'Record belongs to {event_id}')
                
        return generated_events

    def _write_persontaxa(self, data, section):
        max = len(data)
        pub.sendMessage('UpdateMessage', message=f'Writing {section}', update_count=2, new_max=max)
        # Writes persons and taxa to spreadsheet
        row = 1
        col = 'A'
        work_sheet = self.data_file[section]
        sheet_ref = chr(ord(col)) + str(row)
        work_sheet[sheet_ref] = section
        sheet_ref = chr(ord(col) + 1) + str(row)
        work_sheet[sheet_ref] = section + '_ids'
        row += 1
        for key in data.keys():
            pub.sendMessage('UpdateMessage', message = f'Writing {key} to Spreadsheet')
            sheet_ref = col + str(row)
            work_sheet[sheet_ref] = key
            for i in range(len(data[key])):
                sheet_ref = chr(ord(col) + 1 + i) + str(row)
                work_sheet[sheet_ref] = data[key][i]
            row += 1
        return 0

    def _write_siteevent(self, data, section):
        max = len(data)
        pub.sendMessage('UpdateMessage', message=f'Writing {section}', update_count=2, new_max=max)
        # Writes sites and events to spreadsheet
        row = 1
        col = 1
        worksheet = self.data_file[section]
        if section == 'Event':
            worksheet.cell(row=row, column=col, value='Event Number')
        else:
            worksheet.cell(row=row, column=col, value="Collector's Site ID")
        first_record = data[list(data.keys())[1]]
        keys = [key for key in first_record.keys()]
        for key in data.keys():
            pub.sendMessage('UpdateMessage', message = f'Writing {key} to Spreadsheet')
            if row == 1:
                worksheet.cell(row=row + 1, column=1, value=key)
            else:
                worksheet.cell(row=row, column=1, value=key)
            if row == 1:
                for i in range(len(keys)):
                    worksheet.cell(row=row, column=col + 1 + i, value=keys[i])
                row += 1

            for i in range(len(keys)):
                if data[key][keys[i]] is None:
                    continue
                else:
                    if isinstance(data[key][keys[i]], list):
                        names = '; '.join(data[key][keys[i]])
                        worksheet.cell(row=row, column=col + 1 + i, value=names)
                    else:
                        worksheet.cell(row=row, column=col + 1 + i, value=data[key][keys[i]])
            row += 1
        return 0

    def _write_locations(self, data):
        max = len(data)
        pub.sendMessage('UpdateMessage', message=f'Writing Location', update_count=2, new_max=max)
        row = 1
        col = 1
        worksheet = self.data_file['Location']
        for key in data.keys():
            if row == 1:
                worksheet.cell(row=row, column=col, value='Location Code')
                worksheet.cell(row=row, column=col+1, value = 'location_id')
                row += 1
            else:
                worksheet.cell(row=row, column=col, value=key)
                worksheet.cell(row=row, column=col+1, value=data[key])
            row += 1
        return 0


    def write_spreadsheet(self):
        # Writes the found and generated data to new tabs in the import spreadsheet
        if self.area_cd == 'natural':
            missing = {'IMM_template', 'Person', 'Taxon', 'Site', 'Event'} - set(self.data_file.sheetnames)
            tabs = {'Person': [self._find_persons, self._write_persontaxa], 
                    'Taxon': [self._find_taxa, self._write_persontaxa], 
                    'Site': [self._generate_sites, self._write_siteevent], 
                    'Event': [self._generate_events, self._write_siteevent]}
        elif self.discipline == 'history':
            missing = {'IMM_template', 'Location'} - set(self.data_file.sheetnames)
            tabs = {'Location' : [self._find_locations, self._write_locations]}
        else:
            missing = {'IMM_template', 'Person', 'Site', 'Event'} - set(self.data_file.sheetnames)
            tabs = {'Person': [self._find_persons, self._write_persontaxa],  
                    'Site': [self._generate_archsites, self._write_siteevent], 
                    'Event': [self._generate_events, self._write_siteevent]}
        if len(missing) > 0:
            for sheet in missing:
                self.data_file.create_sheet(sheet)
        pub.sendMessage('UpdateMessage', message='Writing Spreadsheet', update_count=1, new_max=4)
        for tab in tabs.keys():
            data = tabs[tab][0]()
            tabs[tab][1](data, tab)
            pub.sendMessage('UpdateMessage', message=f"{tab}s Complete")

        self.data_file.save(self.data_filename)
        self.proc_log.append('Write Spreadsheet')
        self._get_file(self.data_filename)
        return 0 

    def _test_spreadsheet(self):
        # Sanity check for the spreadsheet (not a part of normal operation)
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
        for key in self.keys:
            value = key.value
            query_table = '[' + value[:value.find('.')] + ']'
            query_field = '[' + value[value.find('.') + 1:] + ']'
            if query_table == '[[DISCIPLINE]]':
                query_table = '[' + disciplines[self.discipline] + 'Item' + ']'
            query = f"select {query_field} from {query_table}"
            try:
                test_results = self.cursor.execute(query).fetchone()
                test_results[value] = True
            except:
                test_results[value] = False
        return test_results
    
    def _check_sheets(self):
        # Helper method for the test_spreadsheet method
        # Checks that the required sheets are there
        if self.area_cd == 'natural':
            expected = ('IMM_template', 'Person', 'Taxon', 'Site', 'Event')
        elif self.discipline == 'history':
            expected = ('IMM_template', 'Location')
        else:
            expected = ('IMM_template', 'Person', 'Site', 'Event')
        if set(self.data_file.sheetnames) == expected:
            return True
        else:
            return False

    def _check_persontaxa(self):
        # Helper method for the test_spreadsheet method
        # Checks that persons and taxa look as they should
        for sheet in ['Person', 'Taxon']:
            workingsheet = self.data_file[sheet]
            if workingsheet.max_column > 2:
                return 1, 'This {sheet} sheet is incomplete'
            for row in range(2, workingsheet.max_row + 1):
                if not str(workingsheet.cell(row, 2).value).isnumeric():
                    return 1, 'This {sheet} sheet is incomplete'
        return 0, 'Complete'

    def _add_ids(self):
        # Adds the relevant ids to the spreadsheet for later writing to the database
        if not self._check_sheets:
            return 1, 'This is the wrong spreadsheet'
        if self.discipline != 'history':
            if not self._check_persontaxa:
                return 1, 'Persons/Taxa has not been completed'

        if self.area_cd == 'natural':
            expected = ('IMM_template', 'Person', 'Taxon')
        elif self.discipline == 'history':
            expected = ('IMM_template', 'Location')
        else:
            expected = ('IMM_template', 'Person')
        
        pub.sendMessage('UpdateMessage', message='Adding IDs to Spreadsheet', update_count=1, new_max=4)
        for sheet in expected:
            data = {sheet: {}}
            workingsheet = self.data_file[sheet]
            if sheet in ['Person', 'Taxon']:
                data[sheet] = {workingsheet.cell(i, 1).value: workingsheet.cell(i, 2).value
                               for i in range(2, workingsheet.max_row + 1)}
            else:
                keys = [workingsheet.cell(row=1, column=i).value for i in range(1, workingsheet.max_column + 1)]
                for row in range(2, workingsheet.max_row + 1):
                    id = workingsheet.cell(row=row, column=1).value
                    data[sheet][id] = {keys[i - 1]: workingsheet.cell(row=row, column=i).value
                                       for i in range(1, workingsheet.max_column + 1)}
            if sheet in ['Person', 'Taxon']:
                self._handle_persontaxa(data)
            else:
                self._handle_location(data)
            pub.sendMessage('UpdateMessage', message=f"{sheet} Complete")
        try:    
            self.data_file.save(self.data_filename)
        except PermissionError as e:
            return e, 'Failed! The file is still open'
        self.proc_log.append('IDs added')
        return 0, 'Done'

    def _handle_persontaxa(self, data):
        # Specific logic for writing person_ids and taxon_ids to the spreadsheet
        tab = list(data.keys())[0]
        data = data[tab]
        relevant_cols = self._find_relevant_column(tab)
        i = 1
        for col in relevant_cols:
            col = col + i
            self.ws.insert_cols(col)
            self.ws.cell(row=3, column=col, value=f'{tab.lower()}_id')

            for row in range(4, self.ws.max_row + 1):
                value = self.ws.cell(row=row, column=(col + 1)).value
                if value is None:
                    continue

                if tab == 'Person':
                    values = self._split_persons(value)
                    if isinstance(values, list):
                        value = '; '.join([str(data[thing]) for thing in values])
                    else:
                        value = str(data[value])
                    self.ws.cell(row=row, column=col, value=value)
                else:
                    self.ws.cell(row=row, column=col, value=data[value])
            i += 1
        self._set_keys()
        return 0

    def _handle_location(self, data):
        data = data['Location']
        relevant_col = self._find_relevant_column('Location')
        i = 1
        for col in relevant_col:
            col = col + i
            self.ws.insert_cols(col)
            self.ws.cell(row=3, column=col, value='location_id')

            for row in range(4, self.ws.max_row +1):
                value = self.ws.cell(row=row, column=(col + 1)).value
                if value is None:
                    continue
                self.ws.cell(row=row, column=col, value = data[value])
        self._set_keys()
        return 0



    def _to_prod(self):
        self._connection.close()
        self._connection = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                      "Server=RBCMIMMLIVE;"
                      "Database=Mastodon;"
                      "UID=appuser;"
                      "PWD=Museum2019**;")
        self.cursor = self._connection.cursor()
        return 0, "Database connection changed to Production"

    def _to_test(self):
        self._connection.close()
        self._connection = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                      "Server=RBCMIMMSTAGING;"
                      "Database=Mastodon;"
                      "UID=appuser;"
                      "PWD=Museum2019**;")
        self.cursor = self._connection.cursor()
        return 0, 'Database connection changed to Test'

    def _import_site(self):
        # Logic for importing site data to the database
        # pub methods are for updating the progress bar
        pub.sendMessage('UpdateMessage', message="Writing Sites",
                        update_count=1,
                        new_max=self.data_file['Site'].max_row)
        relevant_cols = self._find_relevant_column('Sites')
        keys = {self.ws[2][col].value: self.keys[col].value 
                for col in relevant_cols if not self.keys[col].value.startswith(
                    'GeoSiteNote')}
        #note_keys = {'Notes (Date)': 'GeoSiteNote.note_date', 
        #             'Notes (Note)': 'GeoSiteNote.note', 
        #             'Notes (Title)': 'GeoSiteNote.title'}
        id_col = 'geo_site_id' if self.area_cd == 'natural' else 'site_id'
        table = 'GeographicSite' if self.area_cd == 'natural' else 'ArchaeologicalSite'
        max_query = f"Select Max(id_col) from {table}"
        max_id = self.cursor.execute(max_query).fetchone()[0]
        working_sheet = self.data_file['Site']
        self._set_identity_insert(table)
        for row in range(2, working_sheet.max_row + 1):
            data = {working_sheet[1][col].value: working_sheet[row][col].value
                    for col in range(1, working_sheet.max_column) 
                    if working_sheet[row][col].value is not None}

            max_id += 1
            insert_keys = 'geo_site_id, discipline_cd, ' if self.area_cd == 'natural' \
                else 'site_id, borden_area, borden_site, temporary_num'
            insert_keys += ', '.join([item.split('.')[1] for item in data.keys()])
            query_part_1 = f"INSERT INTO {table}({insert_keys})"
            if self.area_cd == 'natural':
                query_part_2 = f"VALUES ({max_id}, '{self.discipline}'"
            else:
                query_part_2 = f"VALUES ({max_id}"
            for datum in data.keys():
                if datum in ('Notes (Date)', 'Notes (Note)', 'Notes (Title)'):
                    continue
                if isinstance(data[datum], str):
                    value = f"'{data[datum]}'"
                else:
                    value = data[datum]
                query_part_2 += f", {value}"
            query = query_part_1 + ' \n' + query_part_2 + ')'
            item = data["GeographicSite.collector_site_id"] if self.area_cd == 'natural' \
                else data['ArchaeologicalSite.site_id']
            #if any('Notes (Date)', 'Notes (Note)', 'Notes (Title)' in data.keys()):
            #    note_query = "INSERT INTO GeoSiteNote (geo_site_id, note_date, title, note)"
            #    note_query += "\n VALUES({})".format(max_id, 
            #                                         data['Notes (Date)'],
            #                                         data['Notes (Note)'],
            #                                         data['Notes (Title)'],)
            pub.sendMessage('UpdateMessage', message="{} written to db".format(item))
            
            self.cursor.execute(query)
        self._set_identity_insert(table)
        return 0

    def _import_event(self):
        # Specific logic for importing event data into the database
        pub.sendMessage('UpdateMessage', message="Writing Events",
                        update_count=1,
                        new_max=self.data_file['Event'].max_row)
        relevant_cols = self._find_relevant_column('Events')
        keys = {self.ws[2][col].value: self.keys[col].value 
                for col in relevant_cols}
        id_col = 'coll_event_id' if self.area_cd =='nhist' else 'event_id'
        table = 'CollectionEvent' if self.area_cd == 'natural' else 'ArchaeologicalCollectionEvent'
        max_query = f"Select Max({id_col}) from {table}"
        max_id = self.cursor.execute(max_query).fetchone()[0]
        working_sheet = self.data_file['Event']
        self._set_identity_insert('CollectionEvent')
        for row in range(2, working_sheet.max_row + 1):
            data = {working_sheet[1][col].value: working_sheet[row][col].value
                    for col in range(1, working_sheet.max_column) 
                    if working_sheet[row][col].value is not None}

            max_id += 1
            insert_keys = 'coll_event_id, discipline_cd, ' if self.area_cd == 'natural' \
                else 'event_id, '
            insert_keys += ', '.join([keys[item].split('.')[1] for item in data.keys()])
            query_part_1 = f"INSERT INTO {table}({insert_keys})"
            if self.area_cd == 'natural':
                query_part_2 = f"VALUES ({max_id}, '{self.discipline}'"
            else:
                query_part_2 = f"VALUES ({max_id}"
            for datum in data.keys():
                if isinstance(data[datum], str):
                    value = "'{}'".format(data[datum])
                elif isinstance(data[datum], datetime):
                    value = "'{}'".format(data[datum].strftime('%Y/%m/%d'))
                else:
                    value = data[datum]
                
                query_part_2 += ", {}".format(value)
            query = query_part_1 + ' \n' + query_part_2 + ')'
            item = data["Event Number"]
            pub.sendMessage('UpdateMessage', message="{} written to db".format(item))
            self.cursor.execute(query)
        self._set_identity_insert(table)
        return 0

    def _import_site_event(self):
        # Specific logic for importing the linkage between sites and events
        table = 'GeographicSite_CollectionEvent' if self.area_cd == 'natural' \
            else 'ArchaeologicalSite_Event'
        cols = 'geo_site_id, coll_event_id' if self.area_cd == 'natural' \
            else 'site_id, event_id'
        site_event = []
        for row in range(4, self.ws.max_row + 1):
            site = self.ws[row][51].value
            event = self.ws[row][13].value
            site_event.append(self._query_site_event((site, event)))
        site_event = set(site_event)
        for pair in site_event:
            query = f"Insert into {tables}({cols}) Values ({pair[0]}, {pair[1]})"
            self.cursor.execute(query)

        return 0

    def _query_site_event(self, site_event: tuple):
        # Gets datbase ids for sites and events
        site_table = 'GeographicSite' if self.area_cd == 'natural' else 'ArchaeologicalSite'
        site_param = 'collector_site_id' if self.area_cd == 'natural' else 'temporary_num'
        event_table = 'CollectionEvent' if self.area_cd == 'natural' else 'ArchaeologicalCollectionEvent'
        site_id_col = 'geo_site_id' if self.area_cd == 'natural' else 'site_id'
        event_id_col = 'coll_event_id' if self.area_cd == 'natural' else 'event_id'
        site = site_event[0]
        event = site_event[1]

        site_query = f"Select {site_id_col} from {site_table} where {site_param} = '{site}'"
        event_query = f"Select {event_id_col} from {event_table} where event_num = '{event}'"
        site = self.cursor.execute(site_query).fetchone()[0]
        event = self.cursor.execute(event_query).fetchone()[0]

        return site, event

    def _finalize_query(self, part1, part2, values, update):
        if not update:
            for datum in values.keys():
                if isinstance(values[datum], str):
                    value = f"'{values[datum]}'"
                elif isinstance(values[datum], datetime):
                    value = f"'{values[datum].strftime('%Y/%m/%d')}'"
                else:
                    value = values[datum]
                part2 += f", {value}"
        else:
            for datum in values.keys():
                if isinstance(values[datum], str):
                    values[datum] = f"'{values[datum]}'"
                elif isinstance(values[datum], datetime):
                    values[datum] = f"'{values[datum].strftime('%Y/%m/%d')}'"
                else:
                    continue
            part2 = part2.format(**values)

        query = part1 + '\n' + part2 
        if not update:
            query += ')'
        return query

    def _check_process(self, process, row_num):
        row = self.ws[row_num]
        processes = {'item': 'Item',
                         'nhitem': 'NHItem',
                         'disc_item': 'DisciplineItem',
                         'preparation': 'Preparation',
                         'taxonomy': 'ImptTaxon',
                         'persons': 'Person',
                         'ChemTreat': 'ChemicalTreatment',
                         'FieldMeas': 'FieldMeasurement'}
        relevant_cols = self._find_relevant_column(processes[process])
        values = {self.keys[col].value[self.keys[col].value.find('.') + 1:]: row[col].value 
                for col in relevant_cols if row[col].value is not None} 
        if values == {}:
            return False
        return True


    def _import_specimen(self, update=False):
        # Main method for importing all data which hinges on the item_id
        # This includes all of the item tables (item, naturalhistoryitem, [discipline]item)
        # as well as taxonomy, preparation, collector, determinavit, etc. 
        pub.sendMessage('UpdateMessage', message="Writing Specimens",
                        update_count=1,
                        new_max=self.ws.max_row)
        events = {}
        if self.area_cd == 'natural':
            processes = {'item': self._write_item_query,
                            'nhitem': self._write_nhitem_query,
                            'disc_item': self._write_discipline_item_query,
                            'preparation': self._write_preparation_query,
                            'taxonomy': self._write_taxon_query,
                            'persons': self._prep_persons,
                            'ChemTreat': self._write_chem_query,
                            'FieldMeas': self._write_field_query
                        }
        elif self.discipline != 'history':
            processes = {'item':self._write_item_query,
                            'hhitem': self._write_hhitem_query,
                            'arcitem':self._write_arcitem_query,
                            'persons':self._prep_persons
                            }
        else:
            processes = {'item': self._write_item_query,
                         'hhitem': self._write_chem_query,
                         'location': self._write_loc_query}
        for row in range(4, self.ws.max_row + 1):
            if update is not True:
                self.max_id += 1
            else:
                cat_num = self.ws[row][3].value
                self.max_id = self._get_item_id(cat_num)
            to_do =['item', 'nhitem', 'disc_item', 'preparation', 
                            'ChemTreat','taxonomy', 'FieldMeas'] if  self.area_cd == 'natural' \
                                else ['item', 'hhitem', 'arcitem']
            for process in to_do:
                if not self._check_process(process, row):
                    continue
                query = processes[process](row, update)
                if query == '':
                    continue
                try:
                    self.cursor.execute(query)
                except pyodbc.IntegrityError as e:
                    print('pause')

                except pyodbc.ProgrammingError as e:
                    print('pause')

            event_num = self._query_site_event((self.ws[row][51].value, self.ws[row][13].value))[1]
            person_id = self.ws[row][24].value
            try:
                if event_num not in events.keys() \
                    or person_id not in events[event_num]:
                    if event_num not in events.keys():
                        events[event_num] = []
                    status, stuff = self._import_person(row)
                    if stuff != []:
                        events[stuff[0]].extend(stuff[1])
            except NameError as e:
                print('pause')
            pub.sendMessage('UpdateMessage', message=f'{self.max_id} written to database')
        return 0

    def write_persons_to_db(self):
        pub.sendMessage('UpdateMessage', message="Writing Persons",
                        update_count=1,
                        new_max=self.ws.max_row)
        pub.sendMessage('UpdateMessage', message='Setting Triggers to off',
                        update_count=1, new_max=1)
        self._set_triggers()

        for row in range(4, self.ws.max_row + 1):
            status = self._import_person(row)
            if status != 0:
                return 'ERROR!'

        pub.sendMessage('UpdateMessage', message='Setting Triggers to on',
                        update_count=1, new_max=1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', message='Complete!!')
        self.proc_log.append('Import Complete')
        return 0


    def _query_item_id(self):
        max_query = "Select Max(item_id) from Item"
        max_id = self.cursor.execute(max_query).fetchone()[0]
        return max_id

    def _get_item_id(self, cat_num):
        query = f"select item_id from Item where catalogue_num = '{cat_num}'"
        return self.cursor.execute(query).fetchone()[0]

    def _prep_item(self, row):
        # Helper method for the specimen import method
        relevant_cols = self._find_relevant_column('Item')
        item = {self.keys[col].value[self.keys[col].value.find('.') + 1:]: row[col].value 
                for col in relevant_cols if row[col].value is not None}
        return item

    def _write_insert(self, table, insert_keys):
        formats = {'item': {'table_name': 'Item', 'format': f"{self.max_id}, 'catalog', '{self.area_cd}'"},
                   'nhitem':{'table_name':'NaturalHistoryItem', 'format':f"{self.max_id}, '{self.discipline}'"},
                   'discitem': {'table_name': '{self._get_full_disc()}Item', format: f"VALUES({self.max_id}"}}
        query_part_1 = f"Insert into {formats[table]['table_name']} ({insert_keys})"
        query_part_2 = f"VALUES ({formats[table]['format']}"
        return query_part_1, query_part_2

    def _write_update(self, table, insert_keys):
        formats = {'item': 
                   {'table_name': 'Item', 
                    'format': f"set status_cd = 'catalog', area_cd = '{self.area_cd}', ",
                    'offset': 3},
                   'nhitem':
                   {'table_name':'NaturalHistoryItem', 
                    'format': f"set discipline_cd = '{self.discipline}', ",
                    'offset': 2},
                   'discitem': {'table_name': f'{self._get_full_disc()}Item', 'format':"set ",
                   'offset': 1}}
        query_part_1 = f"Update {formats[table]['table_name']}"
        query_part_2 = formats[table]['format']
        insert_keys = insert_keys.split(', ')[formats[table]['offset']:]
        nums = [j for j in range(len(insert_keys))]
        for i in range(len(insert_keys)):
            if i == 0:
                query_part_2 += f"{insert_keys[i]} = {{{insert_keys[i]}}}"
            else:
                query_part_2 += f", {insert_keys[i]} = {{{insert_keys[i]}}}"
        query_part_2 += f'\nwhere item_id = {self.max_id}'
        return query_part_1, query_part_2

    def _write_item_query(self, row_num, update=False):
        data_row = self.ws[row_num]
        insert_keys = 'item_id, status_cd, area_cd, '
        values = self._prep_item(data_row)
        insert_keys += ', '.join([item for item in values.keys()])
        if not update:
            query_part_1, query_part_2 = self._write_insert('item',insert_keys)
        else:
            query_part_1, query_part_2 = self._write_update('item', insert_keys)
        query = self._finalize_query(query_part_1, query_part_2, values, update)
        return query

    def _prep_nhitem(self, row):
        # Helper method for the specimen import method
        relevant_cols = self._find_relevant_column('NHItem')
        nhitem = {self.keys[col].value[self.keys[col].value.find('.') + 1:]: row[col].value 
                for col in relevant_cols if row[col].value is not None}
        site, event = self._query_site_event((row[51].value, row[13].value))
        nhitem['coll_event_id'] = event
        nhitem['geo_site_id'] = site
        return nhitem

    def _write_nhitem_query(self, row_num, update=False):
        data_row = self.ws[row_num]
        insert_keys = 'item_id, discipline_cd, '
        values = self._prep_nhitem(data_row)
        insert_keys += ', '.join([item for item in values.keys()])
        if not update:
            query_part_1, query_part_2 = self._write_insert('nhitem',insert_keys)
        else:
            query_part_1, query_part_2 = self._write_update('nhitem', insert_keys)
        query = self._finalize_query(query_part_1, query_part_2, values, update)
        return query

    def _prep_discipline_item(self, row):
        # Helper method for the specimen import method
        relevant_cols = self._find_relevant_column('DisciplineItem')
        disc_item = {self.keys[col].value[self.keys[col].value.find('.') + 1:]: row[col].value 
                for col in relevant_cols if row[col].value is not None}
        return disc_item

    def _write_discipline_item_query(self, row_num, update=False):
        data_row = self.ws[row_num]
        insert_keys = 'item_id, '
        values = self._prep_discipline_item(data_row)
        insert_keys += ', '.join([item for item in values.keys()]) 
        if not update:
            query_part_1, query_part_2 = self._write_insert('discitem',insert_keys)
        else:
            query_part_1, query_part_2 = self._write_update('discitem', insert_keys)
        query = self._finalize_query(query_part_1, query_part_2, values, update)
        return query

    def _prep_preparation(self, row):
        # Helper method for the specimen import method
        relevant_cols = self._find_relevant_column('Preparation')
        item = {self.keys[col].value[self.keys[col].value.find('.') + 1:]: row[col].value 
                for col in relevant_cols if row[col].value is not None}
        return item

    def _write_preparation_query(self, row_num, update=False):
        data_row = self.ws[row_num]
        insert_keys = 'item_id, '
        values = self._prep_preparation(data_row)
        insert_keys += ', '.join([item for item in values.keys()])
        query_part_1 = "Insert into Preparation({})".format(insert_keys.strip(', '))
        query_part_2 = "VALUES({0}".format(self.max_id)
        query = self._finalize_query(query_part_1, query_part_2.strip(', '), values, False)
        return query

    def _prep_taxon(self, row):
        # Helper method for the specimen import method
        relevant_cols = self._find_relevant_column('ImptTaxon')
        taxon = {self.keys[col].value[self.keys[col].value.find('.') + 1:]: row[col].value 
                for col in relevant_cols if row[col].value is not None}
        return taxon

    def _write_taxon_query(self, row_num, update = False):
        data_row = self.ws[row_num]
        insert_keys = 'item_id, accepted, cf, aff, '
        values = self._prep_taxon(data_row)
        insert_keys += ', '.join([item for item in values.keys()])
        query_part_1 = "Insert into Taxonomy({})".format(insert_keys.strip(', '))
        query_part_2 = "VALUES({0}, 1, 0, 0".format( self.max_id)
        query = self._finalize_query(query_part_1, query_part_2.strip(', '), values, False)
        return query

    def _prep_chemical_treatment(self, row):

        # Helper method for the specimen import method
        relevant_cols = self._find_relevant_column('ChemicalTreatment')
        chemical_treatment = {self.keys[col].value[self.keys[col].value.find('.') + 1:]: row[col].value 
                for col in relevant_cols if row[col].value is not None}
        return chemical_treatment

    def _write_chem_query(self, row_num, update=False):
        data_row = self.ws[row_num]
        insert_keys = 'item_id, seq_num, '
        values = self._prep_chemical_treatment(data_row)
        insert_keys += ', '.join([item for item in values.keys()])
        query_part_1 = "Insert into ChemicalTreatment('{}')".format(insert_keys.strip(', '))
        query_part_2 = "VALUES({0}, 0, ".format(self.max_id)
        query = self._finalize_query(query_part_1, query_part_2.strip(', '), values, False)
        return query

    def _prep_field_measurement(self, row):
        # Helper method for the specimen import method
        relevant_cols = self._find_relevant_column('FieldMeasurement')
        field_measurement = {self.keys[col].value[self.keys[col].value.find('.') + 1:]: row[col].value 
                for col in relevant_cols if row[col].value is not None}
        return field_measurement

    def _write_field_query(self, row_num, update=False):
        data_row = self.ws[row_num]
        insert_keys = 'item_id, '
        values = self._prep_field_measurement(data_row)
        insert_keys += ', '.join([item for item in values.keys()])
        query_part_1 = "Insert into FieldMeasurement({})".format(insert_keys.strip(', '))
        query_part_2 = "VALUES({0}".format(self.max_id)
        query = self._finalize_query(query_part_1, query_part_2.strip(', '), values, False)
        return query

    def _write_location_query(self, row):
        data_row = self.ws[row]
        insert_keys = 'item_id, location_id, seq_num'
        values = self._prep_location(row)
        query_part_1 = f"Insert into ItemLocation({insert_keys})"
        query_part_2 = f"Values({self.max_id}"
        query = self._finalize_query(query_part_1, query_part_2, values, False)
        return query

    def _prep_location(self, row, update=False):
        col = self._find_relevant_column('ImptLocation')
        if not update:
            location = {'location_id': row[col].value, 'seq_num': 0}
        else:
            location = {'location_id': row[col].value, 'seq_num': self._query_loc_seq_num()}
        return locations
        
    def _query_loc_seq_num(self):
        query = f"Select max(seq_num) from ItemLocation where item_id={self.max_id}"
        result = self.cursor.execute(query).fetchone()
        return result[0] + 1

    def _prep_persons(self, row):
        # Helper method for the specimen import method
        col_names = {25: 'collector',
                81: 'determinavit',
                123: 'preparator'}
        persons = {'collector': [],
                   'determinavit': [],
                   'preparator': []}
        for col_num in col_names.keys():
            key = col_names[col_num]
            if row[col_num -1].value is None:
                continue
            if ';' in row[col_num - 1].value:
                i = 0
                for person in row[col_num - 1].value.split('; '):
                    person_data = {key + '_pid': person}
                    if key == 'collector':
                        coll_event_id = self._query_site_event((row[51].value, row[13].value))[1]
                        if self._person_exists(key, coll_event_id, person):
                            continue
                        person_data['coll_event_id'] = coll_event_id
                        person_data['seq_num'] = self._check_person(key, coll_event_id) + i
                    if key == 'determinavit':
                        if self._person_exists(key, self._query_taxonomy(), person):
                            continue
                        person_data['taxonomy_id'] = self._query_taxonomy()
                        person_data['seq_num'] = self._check_person(key, person_data['taxonomy_id']) + i
                    if key =='preparator':
                        if self._person_exists(key, self.max_id, person):
                            continue
                        person_data['item_id'] = self.max_id
                        person_data['seq_num'] = self._check_person(key, self.max_id) + i
                    persons[key].append(person_data)
                    i += 1
            else:
                person_data = {key + '_pid': row[col_num - 1].value}
                person = row[col_num - 1].value
                if key == 'collector':
                    coll_event_id = self._query_site_event((row[51].value, row[13].value))[1]
                    if self._person_exists(key, coll_event_id, person):
                        continue
                    person_data['coll_event_id'] = coll_event_id
                    person_data['seq_num'] = self._check_person(key, coll_event_id)
                if key == 'determinavit':
                    if self._person_exists(key, self._query_taxonomy(), person):
                        continue
                    person_data['taxonomy_id'] = self._query_taxonomy()
                    person_data['seq_num'] = self._check_person(key, person_data['taxonomy_id'])
                if key =='preparator':
                    if self._person_exists(key, self.max_id, person):
                        continue
                    person_data['item_id'] = self.max_id
                    person_data['seq_num'] = self._check_person(key, self.max_id)
                persons[key].append(person_data)


        return persons

    def _query_taxonomy(self):
        query = "select taxonomy_id from Taxonomy where item_id = {0}".format(self.max_id)
        value = self.cursor.execute(query).fetchone()[0]
        return value

    def _import_person(self, row_num):
        # Helper method for the specimen import method
        # imports person data (collector, determinavit, preparator)
        data_row = self.ws[row_num]
        values = self._prep_persons(data_row)
        for table in values.keys():
            if values[table] is []:
                continue
            for person in values[table]:
                keys = ', '.join(list(person.keys()))
                query_part_1 = 'Insert into {0}({1})'.format(table, keys)
                impt_values = ''
                for key in keys.split(', '):
                    impt_values += str(person[key]) + ', '

                query_part_2 = 'Values ({})'.format(impt_values[:-2])
                query = f'{query_part_2}\n{query_part_2}'
                try:
                    self.cursor.execute(query)
                except pyodbc.IntegrityError as e:
                    if e.args[0]=='23000':
                        return e
                    else:
                        print("This isn't right")
        collectors = [values['collector'][i]['collector_pid'] for i in range(len(values['collector']))]
        if values['collector'] != []:
            collector_info = (values['collector'][0]['coll_event_id'], collectors)
        else:
            collector_info = []

        return 0, collector_info

    def _check_person(self, type, id):
        seq_num = 0
        id_types = {'collector': 'coll_event_id',
                    'determinavit': 'taxonomy_id',
                    'preparator': 'item_id'}
        query = f"Select max(seq_num) from {type} where {id_types[type]} = {id}"
        seq_num = self.cursor.execute(query).fetchone()[0]
        if seq_num is not None:
            return seq_num + 1
        else:
            return 0

    def _person_exists(self, table, id, person):
        id_types = {'collector': ['coll_event_id', 'collector_pid'],
                    'determinavit': ['taxonomy_id', 'determinavit_pid'],
                    'preparator': ['item_id', 'preparator_pid']}
        query = f"select * from {table} where {id_types[table][0]} = {id} and {id_types[table][1]} = {person}"
        results = self.cursor.execute(query).fetchall()
        if results == []:
            return False
        
        return True

    def _set_identity_insert(self, table):
        # Allows/Disallows the inserting into the data tables
        if self.write_status[table] is False:
            query = f'set identity_insert {table} on;'
        else:
            query = f'set identity_insert {table} off;'
        self.cursor.execute(query)
        self.write_status[table] = not self.write_status[table]
        return 0

    def _set_triggers(self):
        # Enables/Disables all triggers on the database
        if self.write_status['Triggers'] is False:
            status = 'DISABLE'
        else:
            status = 'ENABLE'
        query = f'''{status} TRIGGER create_sname ON Taxon;
                   {status} Trigger set_is_component on Component;
                   {status} TRIGGER set_qualified_name ON Taxonomy;
                   {status} TRIGGER clear_item_name ON Taxonomy;
                   {status} TRIGGER update_person_search_name ON Person;
                   {status} TRIGGER update_artist_search_name ON Artist;
                   {status} TRIGGER create_sname on Taxon;
                   {status} TRIGGER create_location_code on Location;'''
        self.cursor.execute(query)
        self.write_status['Triggers'] = not self.write_status['Triggers']
        return 0

    def write_siteevent_to_db(self):
        # Method to write just site and event data to the database (step 1 for some disciplines)
        pub.sendMessage('UpdateMessage', message='Setting Triggers to off',
                        update_count=2, new_max=1)
        self._set_triggers()
        self._import_site()
        self._import_event()
        self.cursor.commit()
        self._import_site_event()
        pub.sendMessage('UpdateMessage', message='Setting Triggers to on',
                        update_count=2, new_max=1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', message='Complete!!')
        self.proc_log.append('Import GeographicSite and CollectionEvent')

    def write_specimen_taxa_persons_to_db(self, update=False):
        # Method to write just specimen related data to the database (setp 2 for the above disciplines)
        pub.sendMessage('UpdateMessage', message='Setting Triggers to off',
                        update_count=2, new_max=1)
        self._set_triggers()
        self._import_specimen(update)
        pub.sendMessage('UpdateMessage', message='Setting Triggers to on',
                        update_count=2, new_max=1)
        self._set_triggers()
        if self.area_cd == 'natural':
            self.cursor.execute("exec BuildAllScientificNames @discipline_cd = '{self.discipline}'")
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', message='Complete!!')
        self.proc_log.append('Import Complete')

    def write_to_db(self):
        # Writes the data from the import spreadsheet to the database
        pub.sendMessage('UpdateMessage', message='Setting Triggers to off',
                        update_count=2, new_max=1)
        step_dict = {'history':[self._set_triggers, self._import_specimen, self._set_triggers],
                 'human': [self._set_triggers, self._import_site, self._import_event, 
                           self.cursor.commit, self._import_site_event, self._import_specimen,
                           self._set_set_triggers, self.cursor.commit],
                 'natural': [self._set_triggers, self._import_site, self._import_event, 
                           self.cursor.commit, self._import_site_event, self._import_specimen,
                           self._set_set_triggers, self.cursor.commit]}
        if self.discipline == 'history':
            steps = step_dict['history']
        else:
            steps = step_dict[self.area_cd]

        for step in steps:
            step()
        if self.area_cd == 'natural':
            self.cursor.execute("exec BuildAllScientificNames @discipline_cd = '{self.discipline}'")
            self.cursor.commit()
        pub.sendMessage('UpdateMessage', message='Complete!!')
        self.proc_log.append('Import Complete')
        return 0

    def update_db(self):
        # Updates the data in the db to match data from the import spreadsheet 
        pub.sendMessage('UpdateMessage', message='Setting Triggers to off',
                        update_count=2, new_max=1)
        self._set_triggers()
        self._import_specimen(update=True)
        pub.sendMessage('UpdateMessage', message='Setting Triggers to on',
                        update_count=2, new_max=1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', message='Complete!!')
        self.proc_log.append('Import Complete')
        return 0

    def write_humanhist_to_db(self):
        return 0
