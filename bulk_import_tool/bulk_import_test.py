import unittest
from datetime import datetime
from bulk_import_tool import import_tools
import openpyxl

class BulkImportTest(unittest.TestCase):
    
    def setUp(self):
        self.impt = import_tools()
        self.maxDiff = None
    
    def test_find_persons(self):
        value = self.impt._find_persons()
        test_values = {'Hugh MacIntosh': [6755], 'Evan Harley': [6168],
                       'Meg Sugrue': ['NEW?'], 'David Stewart': ['NEW?'],
                       'Henry Choong': [2767, 4659], 'Heidi Gartner': [2430, 4829, 5698]}
        self.assertEqual(test_values, value)

    def test_find_relevant_column(self):
        test_methods = ['Person', 'Taxon', 'Sites', 'Events']
        value = []
        test_values = [22, 75, 76, 117]
        test_values2 = [i for i in range(6, 73) if i != 48]
        test_values.extend(test_values2)
        for method in test_methods:
            value.extend(self.impt._find_relevant_column(method))
        self.assertEqual(sorted(test_values), sorted(value))

    def test_split_persons(self):
        value = self.impt._split_persons('Evan Harley, Hugh MacIntosh; Meg Sugrue| Dave Stewart: test')
        self.assertEqual(['Evan Harley', 'Hugh MacIntosh', 'Meg Sugrue', 'Dave Stewart', 'test'], value)

    def test_find_taxa(self):
        value = self.impt._find_taxa()
        test_values = {'Cancer productus': [98672], 'Cancer magister': [98675]}
        self.assertEqual(test_values, value)

    def test_query_taxa(self):
        test_taxa = ['Tryphon', 'Heterocladium macounii Best', 'Trivias', 'Homoglaea hircina', 'Acanthodiaptomus']
        value = []
        test_values = [70215058, 90223816, 'NEW?', 70212504, 70219886, 70219887, 70219888,
                        70219889, 70219890, 70219898, 85836, 80047655]
        for taxon in test_taxa:
            value.extend(self.impt._query_taxa(taxon))
        self.assertEqual(test_values, value)
   
    def test_generate_site(self):
        value = self.impt._generate_sites()
        test_values = {'VS101450':
                        {'Elevation (max)': 15,
                        'Elevation (min)': 14,
                        'Elevation note': None,
                        'Elevation unit': 'm',
                        'Biogeoclimatic': None,
                        'Biozone':None,
                        'Continent': 'North America',
                        'Country': 'Canada',
                        'County': None,
                        'District': None,
                        'Ecoprovince': None,
                        'Fossil Ref. No.': None,
                        'Mine Name': None,
                        'Natural Region': None,
                        'Park/ER/IR': None,
                        'Province/State': 'British Columbia',
                        'Range/Township/Section': None,
                        'Water Body': None,
                        'Description':	'Fannin tower, 2nd floor',
                        'Discipline': None,
                        'Location Name': 'Victoria: RBCM Collections building',
                        'Reference': None, 
                        'Remarks': None,
                        'Notes (Date)': None,
                        'Notes (Note)': None,
                        'Notes (Title)': None,
                        'Accuracy': None,
                        'Approximate': None,
                        'Latitude': 48.419603,
                        'Latitude Stop': None,	
                        'Longitude': -123.3706457,
                        'Longitude Stop': None,
                        'N.A. Datapoint': None,
                        'Non-NTS Map Reference': None,
                        'NTS Map Reference': None,
                        'UTM Datapoint': None,
                        'UTM Easting': None,
                        'UTM Northing': None,
                        'UTM Zone':	None,
                        'Primary drainage':	None,
                        'Secondary drainage': None,
                        'Tertiary drainage': None,
                        }, 'VS101451':
                        {
                        'Elevation (max)': 5,
                        'Elevation (min)': 4,
                        'Elevation note': None,
                        'Elevation unit': 'm',
                        'Biogeoclimatic': None,
                        'Biozone': None,
                        'Continent': 'North America',
                        'Country': 'Canada',
                        'County': None,
                        'District': None,
                        'Ecoprovince': None,
                        'Fossil Ref. No.': None,
                        'Mine Name': None,
                        'Natural Region': None,
                        'Park/ER/IR': None,
                        'Province/State': 'British Columbia',
                        'Range/Township/Section': None,
                        'Water Body': None,
                        'Description':	'Clifford Carl Hall',
                        'Discipline': None,
                        'Location Name': 'Victoria: RBCM Exhibits building',
                        'Reference': None, 
                        'Remarks': None,
                        'Notes (Date)': None,
                        'Notes (Note)': None,
                        'Notes (Title)': None,
                        'Accuracy': None,
                        'Approximate': None,
                        'Latitude': 48.419957,
                        'Latitude Stop': None,	
                        'Longitude': -123.3688604,
                        'Longitude Stop': None,
                        'N.A. Datapoint': None,
                        'Non-NTS Map Reference': None,
                        'NTS Map Reference': None,
                        'UTM Datapoint': None,
                        'UTM Easting': None,
                        'UTM Northing': None,
                        'UTM Zone':	None,
                        'Primary drainage':	None,
                        'Secondary drainage': None,
                        'Tertiary drainage': None,
                        }}

        self.assertEqual(test_values, value)

    def test_get_max_site_id(self):
        value = self.impt._get_max_site_id()
        test_value = ['VS', '101449']
        self.assertEqual(test_value, value)

    def test_get_max_event_id(self):
        value = self.impt._get_max_event_id()
        test_value = ['VE', '17430']
        self.assertEqual(test_value, value)

    def test_add_ids(self):
        self.fail("Not Implemented")

    def test_import_collection_events(self):
        self.fail("Not Implemented")

    def test_import_geographic_sites(self):
        self.fail("Not Implemented")

    def test_import_taxonomy(self):
        self.fail("Not Implemented")

    def test_import_specimen(self):
        self.fail("Not Implemented")

    def test_write_to_test(self):
        self.fail("Not Implemented")

    def test_write_to_prod(self):
        self.fail("Not Implemented")

    def test_generate_event(self):
        value = self.impt._generate_events()
        test_values = {'VE17431':
                        {'Bait': None,
                        'Collection method': 'Hand',
                        'Date': datetime(2018, 12, 31, 0, 0),
                        'Date remarks': None,
                        'Discipline': 'INV',
                        'Field Event Code': 'EV1',
                        'Net/Gear/Trap type': 'trap',
                        'Note': None,
                        'Permit Number': None,
                        'Season': 'Winter',
                        'Start time': None,
                        'Stop time': None,
                        'Time Standard': None,
                        'Trapping/Sampling Duration': None,
                        'Vessel Name': None,
                        'Collector': ['Hugh MacIntosh', 'Evan Harley'],
                        'Air temperature': None,
                        'Air temperature unit': None,
                        'Cloud cover': None,
                        'Weather remarks': None,
                        'Wind direction': None,
                        'Wind speed': None,
                        'Wind speed unit': None,
                        },
                        'VE17432':
                        {'Bait': None,
                        'Collection method': 'Hand',
                        'Date': datetime(2018, 12, 31, 0, 0),
                        'Date remarks': None,
                        'Discipline': 'INV',
                        'Field Event Code': 'EV2',
                        'Net/Gear/Trap type': 'trap',
                        'Note': None,
                        'Permit Number': None,
                        'Season': 'Winter',
                        'Start time': None,
                        'Stop time': None,
                        'Time Standard': None,
                        'Trapping/Sampling Duration': None,
                        'Vessel Name': None,
                        'Collector': ['Meg Sugrue', 'David Stewart'],
                        'Air temperature': None,
                        'Air temperature unit': None,
                        'Cloud cover': None,
                        'Weather remarks': None,
                        'Wind direction': None,
                        'Wind speed': None,
                        'Wind speed unit': None,
                        }
                        }
        self.assertEqual(test_values, value)

    def test_write_spreadsheet(self):
        test_values = {
            'sheet_names':['IMM_template', 'Person', 'Taxon', 'Site', 'Event'],
            'row_count':{'IMM_template': 5, 
                       'Person': 7, 
                       'Taxon':3, 
                       'Site': 3, 
                       'Event': 3}
            }
        values = {}
        self.impt.write_spreadsheet()
        file = openpyxl.load_workbook(self.impt.data_filename)

        values['sheet_names'] = file.sheetnames
        values['row_count'] = {}
        for sheet in file.sheetnames:
            worksheet = file[sheet]
            values['row_count'][sheet] = worksheet.max_row

        self.assertEqual(test_values, values)

    def test_spreadsheet(self):
        values = self.impt._test_spreadsheet()
        key_row = self.impt.ws[3]
        test_values = {key_row[i].value: True for i in range(len(key_row))}
        self.assertEqual(test_values, values)






if __name__ == '__main__':
    unittest.main()
