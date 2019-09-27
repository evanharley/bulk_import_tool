import pyodbc
import openpyxl
from pubsub import pub
from datetime import datetime

# Tools for the Bulk Import of Natural History Specimens


class ImportTools:

    def __init__(self, *args, **kwargs):
        
        # Discipline should be gotten from user at the start of the import
        # so when coding GUI it should be included
        self.data_filename = ''
        self.discipline = ''
        self.area_cd = ''
        self._connection = pyodbc.connect('DSN=IMMProd; Trusted_Connection=yes;')
        self.cursor = self._connection.cursor()
        self.data_file = None
        self.ws = None
        self.keys = None
        self.max_id = self._query_item_id()
        self.max_col = 135
        ## self.max_col = 138 actual value
        self.write_status = {'GeographicSite': False, 
                             'CollectionEvent': False,
                             'Taxonomy': False,
                             'Triggers': False}
        self.proc_log = []

    def _get_file(self, filename):
        self.data_filename = filename
        try:
            self.data_file = openpyxl.load_workbook(filename)
        except FileNotFoundError:
            return None
        self.ws = self.data_file['IMM_template']
        self._set_keys()
        

    def _set_keys(self):
        self.keys = [self.ws[3][col] for col in range(self.max_col) if self.ws[3][col].value is not None]

    def _write_prog(self):
        prog_log = open('prog_log.log', 'a')
        prog_log.write('{0}: {1}\n'.format(self.data_filename, ', '.join(self.proc_log)))
        return 0

    def _get_prog_info(self):
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

        for i in range(len(names)):
            if ',' in names[i]:
                name = [thing.strip() for thing in names[i].split(',')]
                names[i] = ' '.join(name)
        names = list(set(names))

        for name in names:
            query = "Select person_id from Person where search_name = '{}'".format(name)
            results = self.cursor.execute(query).fetchall()
            persons[name] = []
            if results is not []:
                for i in range(len(results)):
                    persons[name].append(results[i][0])
            else:
                persons[name] = ['NEW?']
        return persons

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
            'pal': 'Paleontology'
            }
        return disciplines[self.discipline]

    def _find_relevant_column(self, method):
        # Return the index of the columns relevant to the _find methods above.
        # Values in a list of indices
        relevant_cols = []
        disc = self._get_full_disc()
        table_ids = {'Person': ['Person.search_name'],
                     'Taxon': ['Taxon.term'],
                     'Events': ['CollectionEvent.'],
                     'Sites': ['GeographicSite.', 'GeoSiteNote'],
                     'Item': ['Item'],
                     'NHItem': ['NaturalHistoryItem.'],
                     'FieldMeasurement': ['FieldMeasurement.'],
                     'DisciplineItem': ['[DISCIPLINE].', disc + 'Item.'],
                     'ImptTaxon': ['Taxonomy.', 'taxon_id'],
                     'Preparation': ['Preparation.'],
                     'ChemicalTreatment': ['ChemicalTreatment.']}
        table_id = table_ids[method]


        for col in range(0, len(self.keys)):
            if any(self.keys[col].value.startswith(id) for id in table_id) \
                    and col not in relevant_cols:
                relevant_cols.append(col)

        return relevant_cols
        
    def _split_persons(self, person_names):
        # Returns the split value of person names where a delineator is present
        delineators = ";:|/\\"
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
        for row in range(4, self.ws.max_row + 1):
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
        keys = self.ws[2]
        generated_sites = {}
        for row in range(4, self.ws.max_row + 1):
            new_site_id[1] = str(int(new_site_id[1]) + 1)
            site_id = new_site_id[0] + new_site_id[1]
            if generated_sites == {}:
                generated_sites[site_id] = {}
                for index in relevant_cols:
                    generated_sites[site_id][self.keys[index].value] = self.ws[row][index].value
                generated_sites[site_id]["GeographicSite.collector_site_id"] = site_id
                self.ws.cell(row=row, column=51, value=site_id)
            else:
                site = {}
                difference = 0
                for index in relevant_cols:
                    site[self.keys[index].value] = self.ws[row][index].value
                for item in generated_sites.keys():
                    diff = {key: site[key] for key in generated_sites[item] 
                            if key != "GeographicSite.collector_site_id" and site[key] != generated_sites[item][key]}
                    if len(diff.keys()) > 0:
                        difference += 1
                    else:
                        difference = 0
                        matching_id = item
                        break
                if difference > 0:
                    generated_sites[site_id] = site
                    self.ws.cell(row=row, column=51, value=site_id)
                    generated_sites[site_id]["GeographicSite.collector_site_id"] = site_id
                else:
                    self.ws.cell(row=row, column=51, value=matching_id)
        return generated_sites

    def _get_max_site_id(self):
        # Queries the database for the highest collector_site_id for the discipline selected
        # returns the discipline specific prefix and the site_id
        prefix_query = "Select geo_site_prefix from NHDisciplineType where discipline_cd = '{}'".format(self.discipline)
        prefix = self.cursor.execute(prefix_query).fetchall()[0][0]
        query = "Select max(convert(int, SUBSTRING(collector_site_id, 3, 100))) from GeographicSite " + \
            "where discipline_cd = '{}' and substring(collector_site_id, 1, 2) = '{}'".format(self.discipline, prefix)
        result = self.cursor.execute(query).fetchone()
        if len(str(result)) > 6:
            diff = 6 - len(str(result))
            prefix += ''.join({'0' for i in range(0, diff)})
        max_site_id = [prefix, str(result[0])]
        return max_site_id

    def _get_max_event_id(self):
        # Queries the database for the highest event_num for the discipline selected
        # returns the discipline specific prefix and the event_num
        prefix_query = "Select coll_event_prefix from NHDisciplineType where discipline_cd = " +\
                                "'{}'".format(self.discipline)
        prefix = self.cursor.execute(prefix_query).fetchall()[0][0]
        query = "Select max(convert(int, SUBSTRING(event_num, 3, 100))) from CollectionEvent " + \
            "where discipline_cd = '{}' and substring(event_num, 1, 2) = '{}'".format(self.discipline, prefix)
        result = self.cursor.execute(query).fetchone()
        if len(str(result)) > 6:
            diff = 6 - len(str(result))
            prefix += ''.join({'0' for i in range(0, diff)})
        max_event_id = [prefix, str(result[0])]
        return max_event_id

    def _generate_events(self):
        # Generates new collection events for import, from the unique events in the import spreadsheet
        generated_events = {}
        new_event_id = self._get_max_event_id()
        relevant_cols = self._find_relevant_column('Events')
        self.keys = self.ws[2]
        for row in range(4, self.ws.max_row + 1):
            new_event_id[1] = str(int(new_event_id[1]) + 1)
            event_id = new_event_id[0] + new_event_id[1]
            working_row = self.ws[row]
            if generated_events == {}:
                generated_events[event_id] = {}
                for index in relevant_cols:
                    generated_events[event_id][self.keys[index].value] = working_row[index].value
                generated_events[event_id]["Event Number"] = event_id
                self.ws.cell(row=row, column=14, value=event_id)
            else:
                event = {}
                difference = 0
                for index in relevant_cols:
                    event[self.keys[index].value] = working_row[index].value
                for item in generated_events.keys():
                    diff = {key: event[key] for key in generated_events[item] 
                            if key != "Event Number" and event[key] != generated_events[item][key]}
                    if len(diff.keys()) > 0:
                        difference += 1
                    else:
                        difference = 0
                        matching_id = item
                        break
                if difference > 0:
                    generated_events[event_id] = event
                    self.ws.cell(row=row, column=14, value=event_id)
                    generated_events[event_id]["Event Number"] = event_id
                else:
                    self.ws.cell(row=row, column=14, value=matching_id)
                
        return generated_events

    def _write_persontaxa(self, data, section):
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
            sheet_ref = col + str(row)
            work_sheet[sheet_ref] = key
            for i in range(len(data[key])):
                sheet_ref = chr(ord(col) + 1 + i) + str(row)
                work_sheet[sheet_ref] = data[key][i]
            row += 1
        return 0

    def _write_siteevent(self, data, section):
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

    def write_spreadsheet(self):
        # Writes the found and generated data to new tabs in the import spreadsheet
        missing = {'IMM_template', 'Person', 'Taxon', 'Site', 'Event'} - set(self.data_file.sheetnames)
        if len(missing) > 0:
            for sheet in missing:
                self.data_file.create_sheet(sheet)

        persons = self._find_persons()
        self._write_persontaxa(persons, 'Person')
        pub.sendMessage('UpdateMessage', arg1="Persons Complete")

        taxa = self._find_taxa()
        self._write_persontaxa(taxa, 'Taxon')
        pub.sendMessage('UpdateMessage', arg1="Taxa Complete")

        sites = self._generate_sites()
        self._write_siteevent(sites, "Site")
        pub.sendMessage('UpdateMessage', arg1="Sites Complete")

        events = self._generate_events()
        self._write_siteevent(events, 'Event')
        pub.sendMessage('UpdateMessage', arg1="Events Complete")

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
            query = "select {query_field} from {query_table}"
            try:
                test_results = self.cursor.execute(query).fetchone()
                test_results[value] = True
            except:
                test_results[value] = False
        return test_results
    
    def _check_sheets(self):
        # Helper method for the test_spreadsheet method
        # Checks that the required sheets are there
        if set(self.data_file.sheetnames) == ('IMM_template', 'Person', 'Taxon', 'Site', 'Event'):
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
        if not self._check_persontaxa:
            return 1, 'Persons/Taxa has not been completed'
        
        for sheet in ['Person', 'Taxon', 'Site', 'Event']:
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
            if sheet in ['Site', 'Event']:
                self._handle_siteevent(data)
            pub.sendMessage('UpdateMessage', arg1="{sheet} Complete")
        try:    
            self.data_file.save(self.data_filename)
        except PermissionError as e:
            return e, 'Failed!'
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
            self.ws.cell(row=3, column=col, value='{}_id'.format(tab.lower()))

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

    def _handle_siteevent(self, data):
        # Specific logic for handling writing site and event ids etc to the spreadsheet
        tab = list(data.keys())[0]
        data = data[tab]
        tab = tab + 's'
        relevant_cols = self._find_relevant_column(tab)
        for col in relevant_cols:
            key = self.ws.cell(row=2, column=col + 1).value

            for row in range(4, self.ws.max_row + 1):
                value = self.ws.cell(row=row, column=col + 1).value
                if tab == 'Sites':
                    id = self.ws.cell(row=row, column=52).value
                else:
                    id = self.ws.cell(row=row, column=14).value
        return 0

    def _to_prod(self):
        self._connection.close()
        self._connection = pyodbc.connect('DSN=IMMProd; Trusted_Connection=yes;')
        self.cursor = self._connection.cursor()
        return 0, "Database connection changed to Production"

    def _to_test(self):
        self._connection.close()
        self._connection = pyodbc.connect('DSN=IMMTest; Trusted_Connection=yes;')
        self.cursor = self._connection.cursor()
        return 0, 'Database connection changed to Test'

    def _import_site(self):
        # Logic for importing site data to the database
        # pub methods are for updating the progress bar
        pub.sendMessage('UpdateMessage', arg1="Writing Sites",
                        arg2=1,
                        arg3=self.data_file['Site'].max_row)
        relevant_cols = self._find_relevant_column('Sites')
        keys = {self.ws[2][col].value: self.keys[col].value 
                for col in relevant_cols if not self.keys[col].value.startswith(
                    'GeoSiteNote')}
        #note_keys = {'Notes (Date)': 'GeoSiteNote.note_date', 
        #             'Notes (Note)': 'GeoSiteNote.note', 
        #             'Notes (Title)': 'GeoSiteNote.title'}
        max_query = "Select Max(geo_site_id) from GeographicSite"
        max_id = self.cursor.execute(max_query).fetchone()[0]
        working_sheet = self.data_file['Site']
        self._set_identity_insert('GeographicSite')
        for row in range(2, working_sheet.max_row + 1):
            data = {working_sheet[1][col].value: working_sheet[row][col].value
                    for col in range(1, working_sheet.max_column) 
                    if working_sheet[row][col].value is not None}

            max_id += 1
            insert_keys = 'geo_site_id, discipline_cd, '
            insert_keys += ', '.join([item.split('.')[1] for item in data.keys()])
            query_part_1 = "INSERT INTO GeographicSite({})".format(insert_keys)
            query_part_2 = "VALUES ({}, '{}'".format(max_id, self.discipline)
            for datum in data.keys():
                if datum in ('Notes (Date)', 'Notes (Note)', 'Notes (Title)'):
                    continue
                if isinstance(data[datum], str):
                    value = "'{}'".format(data[datum])
                else:
                    value = data[datum]
                query_part_2 += ", {}".format(value)
            query = query_part_1 + ' \n' + query_part_2 + ')'
            item = data["GeographicSite.collector_site_id"]
            #if any('Notes (Date)', 'Notes (Note)', 'Notes (Title)' in data.keys()):
            #    note_query = "INSERT INTO GeoSiteNote (geo_site_id, note_date, title, note)"
            #    note_query += "\n VALUES({})".format(max_id, 
            #                                         data['Notes (Date)'],
            #                                         data['Notes (Note)'],
            #                                         data['Notes (Title)'],)
            pub.sendMessage('UpdateMessage', arg1="{} written to db".format(item))
            
            self.cursor.execute(query)
        self._set_identity_insert('GeographicSite')
        return 0

    def _import_event(self):
        # Specific logic for importing event data into the database
        pub.sendMessage('UpdateMessage', arg1="Writing Events",
                        arg2=1,
                        arg3=self.data_file['Event'].max_row)
        relevant_cols = self._find_relevant_column('Events')
        keys = {self.ws[2][col].value: self.keys[col].value 
                for col in relevant_cols}
        max_query = "Select Max(coll_event_id) from CollectionEvent"
        max_id = self.cursor.execute(max_query).fetchone()[0]
        working_sheet = self.data_file['Event']
        self._set_identity_insert('CollectionEvent')
        for row in range(2, working_sheet.max_row + 1):
            data = {working_sheet[1][col].value: working_sheet[row][col].value
                    for col in range(1, working_sheet.max_column) 
                    if working_sheet[row][col].value is not None}

            max_id += 1
            insert_keys = 'coll_event_id, discipline_cd, '
            insert_keys += ', '.join([keys[item].split('.')[1] for item in data.keys()])
            query_part_1 = "INSERT INTO CollectionEvent({})".format(insert_keys)
            query_part_2 = "VALUES ({}, '{}'".format(max_id, self.discipline)
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
            pub.sendMessage('UpdateMessage', arg1="{} written to db".format(item))
            self.cursor.execute(query)
        self._set_identity_insert('CollectionEvent')
        return 0

    def _import_site_event(self):
        # Specific logic for importing the linkage between sites and events
        site_event = []
        for row in range(4, self.ws.max_row + 1):
            site = self.ws[row][51].value
            event = self.ws[row][13].value
            site_event.append(self._query_site_event((site, event)))
        query = '''Insert into GeographicSite_CollectionEvent(geo_site_id, coll_event_id)
                    Values ({0}, {1})'''
        site_event = set(site_event)
        for pair in site_event:
            self.cursor.execute(query.format(pair[0], pair[1]))

        return 0

    def _query_site_event(self, site_event: tuple):
        # Gets datbase ids for sites and events
        site = site_event[0]
        event = site_event[1]

        site_query = "Select geo_site_id from GeographicSite where collector_site_id = '{}'"
        event_query = "Select coll_event_id from CollectionEvent where event_num = '{}'"
        site = self.cursor.execute(site_query.format(site)).fetchone()[0]
        event = self.cursor.execute(event_query.format(event)).fetchone()[0]

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
             part2 = part2.format(**values)

        query = part1 + '\n' + part2 
        if not update:
            query += ')'
        return query

    def _import_specimen(self, update=False):
        # Main method for importing all data which hinges on the item_id
        # This includes all of the item tables (item, naturalhistoryitem, [discipline]item)
        # as well as taxonomy, preparation, collector, determinavit, etc. 
        pub.sendMessage('UpdateMessage', arg1="Writing Specimens",
                        arg2=1,
                        arg3=self.ws.max_row)
        events = {}
        for row in range(4, self.ws.max_row + 1):
            if update is not True:
                self.max_id += 1
            else:
                cat_num = self.ws[self.keys['Item.catalogue_num']]
                self.max_id = self._get_item_id(cat_num)
            processes = {'item': self._write_item_query,
                         'nhitem': self._write_nhitem_query,
                         'disc_item': self._write_discipline_item_query,
                         'preparation': self._write_preparation_query,
                         'taxonomy': self._write_taxon_query,
                         'persons': self._prep_persons,
                         'ChemTreat': self._write_chem_query,
                         'FieldMeas': self._write_field_query
                         }
            for process in ['item', 'nhitem', 'disc_item', 'preparation', 
                            'ChemTreat','taxonomy', 'FieldMeas']:
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
            person_id = self.ws[row][25]
            if event_num not in events.keys() or \
                value in events.keys() and person_id not in events[value]:
                if event_num not in events.keys():
                    events[event_num] = []
                status, stuff = self._import_person(row)
                events[stuff[0]].extend(stuff[1])
            pub.sendMessage('UpdateMessage', arg1=f'{self.max_id} written to database')
        return 0

    def write_persons_to_db(self):
        pub.sendMessage('UpdateMessage', arg1="Writing Persons",
                        arg2=1,
                        arg3=self.ws.max_row)
        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to off',
                        arg2=1, arg3=1)
        self._set_triggers()

        for row in range(4, self.ws.max_row + 1):
            status = self._import_person(row)
            if status != 0:
                return 'ERROR!'

        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to on',
                        arg2=1, arg3=1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', arg1='Complete!!')
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
                    'format': f"set discipline_cd = '{self.discipline}'",
                    'offset': 2},
                   'discitem': {'table_name': f'{self._get_full_disc()}Item', format:"set"},
                   'offset': 1}
        query_part_1 = f"Update {formats[table]['table_name']}"
        query_part_2 = formats[table]['format']
        insert_keys = insert_keys.split(', ')[formats[table]['offset']:]
        nums = [j for j in range(len(insert_keys))]
        for i in range(len(insert_keys)):
            if i == 0:
                query_part_2 += f"{insert_keys[i]} = '{{{insert_keys[i]}}}'"
            else:
                query_part_2 += f", {insert_keys[i]} = '{{{insert_keys[i]}}}'"
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
            query_part_1, query_part_2 = self._write_insert('item',insert_keys)
        else:
            query_part_1, query_part_2 = self._write_update('item', insert_keys)
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
        query_part_1 = "Insert into Preparation({})".format(insert_keys)
        query_part_2 = "VALUES({0}".format(self.max_id)
        query = self._finalize_query(query_part_1, query_part_2, values, False)
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
        query_part_1 = "Insert into Taxonomy({})".format(insert_keys)
        query_part_2 = "VALUES({0}, 1, 0, 0".format( self.max_id)
        query = self._finalize_query(query_part_1, query_part_2, values, False)
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
        query_part_1 = "Insert into ChemicalTreatment('{}')".format(insert_keys)
        query_part_2 = "VALUES({0}, 0, ".format(self.max_id)
        query = self._finalize_query(query_part_1, query_part_2, values, False)
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
        query_part_1 = "Insert into FieldMeasurement({})".format(insert_keys)
        query_part_2 = "VALUES({0}".format(self.max_id)
        query = self._finalize_query(query_part_1, query_part_2, values, False)
        return query

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
                for person in row[col_num - 1].split(';'):
                    person_data = {key + '_pid': person}
                    if key == 'collector':
                        person_data['coll_event_id'] = self._query_site_event((row[51].value, row[13].value))[1]
                        person_data['seq_num'] = i                       
                    if key == 'determinavit':
                        person_data['taxonomy_id'] = self._query_taxonomy()
                        person_data['seq_num'] = i
                    if key =='preparator':
                        person_data['seq_num'] = i
                    persons[key].append(person_data)
                    i += 1
            else:
                person_data = {key + '_pid': row[col_num - 1].value}
                if key == 'collector':
                    person_data['coll_event_id'] = self._query_site_event((row[51].value, row[13].value))[1]
                    person_data['seq_num'] = self._check_person(key, person_data['coll_event_id'])
                if key == 'determinavit':
                    person_data['taxonomy_id'] = self._query_taxonomy()
                    person_data['seq_num'] = self._check_person(key, person_data['taxonomy_id'])
                if key =='preparator':
                    person_data['item_id'] = self.max_id
                    person_data['seq_num'] = self._check_person(key, person_data['item_id'])
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
                query = '{0}\n{1}'.format(query_part_1, query_part_2)
                try:
                    self.cursor.execute(query)
                except pyodbc.IntegrityError as e:
                    if e.args[0]=='23000':
                        return e
                    else:
                        print("This isn't right")
        collectors = [values['collector'][i]['collector_pid'] for i in range(len(values['collector']))]
        collector_info = (values['collector'][0]['coll_event_id'], collectors)
        return 0, collector_info

    def _check_person(self, type, id):
        seq_num = 0
        id_types = {'collector': 'coll_event_id',
                    'determinavit': 'taxonomy_id',
                    'preparator': 'item_id'}
        query = "Select max(seq_num) from {0} where {1} = {2}".format(type, id_types[type], id)
        seq_num = self.cursor.execute(query).fetchone()[0]
        if seq_num is not None:
            return seq_num + 1
        else:
            return 0

    def _set_identity_insert(self, table):
        # Allows/Disallows the inserting into the data tables
        if self.write_status[table] is False:
            query = 'set identity_insert {} on;'.format(table)
        else:
            query = 'set identity_insert {} off;'.format(table)
        self.cursor.execute(query)
        self.write_status[table] = not self.write_status[table]
        return 0

    def _set_triggers(self):
        # Enables/Disables all triggers on the database
        if self.write_status['Triggers'] is False:
            status = 'DISABLE'
        else:
            status = 'ENABLE'
        query = '''{0} TRIGGER create_sname ON Taxon;
                   {0} Trigger set_is_component on Component;
                   {0} TRIGGER set_qualified_name ON Taxonomy;
                   {0} TRIGGER clear_item_name ON Taxonomy;
                   {0} TRIGGER update_person_search_name ON Person;
                   {0} TRIGGER update_artist_search_name ON Artist;
                   {0} TRIGGER create_sname on Taxon;
                   {0} TRIGGER create_location_code on Location;'''.format(status)
        self.cursor.execute(query)
        return 0

    def write_siteevent_to_db(self):
        # Method to write just site and event data to the database (step 1 for some disciplines)
        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to off',
                        arg2=1, arg3=1)
        self._set_triggers()
        self._import_site()
        self._import_event()
        self.cursor.commit()
        self._import_site_event()
        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to on',
                        arg2=1, arg3=1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', arg1='Complete!!')
        self.proc_log.append('Import GeographicSite and CollectionEvent')

    def write_specimen_taxa_persons_to_db(self):
        # Method to write just specimen related data to the database (setp 2 for the above disciplines)
        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to off',
                        arg2=1, arg3=1)
        self._set_triggers()
        self._import_specimen()
        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to on',
                        arg2=1, arg3=1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', arg1='Complete!!')
        self.proc_log.append('Import Complete')

    def write_to_db(self):
        # Writes the data from the import spreadsheet to the database
        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to off',
                        arg2=1, arg3=1)
        self._set_triggers()
        self._import_site()
        self._import_event()
        self._import_specimen()
        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to on',
                        arg2=1, arg3=1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', arg1='Complete!!')
        self.proc_log.append('Import Complete')
        return 0

    def update_db(self):
        # Updates the data in the db to match data from the import spreadsheet 
        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to off',
                        arg2=1, arg3=1)
        self._set_triggers()
        self._import_specimen(update=True)
        pub.sendMessage('UpdateMessage', arg1='Setting Triggers to on',
                        arg2=1, arg3=1)
        self._set_triggers()
        self.cursor.commit()
        pub.sendMessage('UpdateMessage', arg1='Complete!!')
        self.proc_log.append('Import Complete')
        return 0
