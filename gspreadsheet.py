
import gspread
import json
import logging
from google.oauth2.service_account import Credentials


SHEETS_API_SCOPE = ['https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive']

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

def _get_json_auth_key(local=False, local_path=None, remote_function=None):
    if local:
        if local_path == '':
            raise ValueError('for local option, local file path is required')
        logging.info('using local creds file')
        with open(local_path, 'r') as local_json:
            return json.loads(local_json.read())
    else:
        logging.info('using remote function creds file')
        return json.loads(remote_function())

class SpreadsheetIntegration:

    main_worksheet = None

    def __init__(self, spreadsheet_id: str, cred_file_path=None, cred_remote_function=None):
        self.spreadsheet_id = spreadsheet_id
        local_usage = True if cred_file_path else False
        self.creds_key_json = _get_json_auth_key(local=local_usage, local_path=cred_file_path, remote_function=cred_remote_function)
        credentials = Credentials.from_service_account_info(
            self.creds_key_json,
            scopes=SHEETS_API_SCOPE
        )
        self.set_credentials(credentials)


    def set_credentials(self, credentials):
        self.gc = gspread.authorize(credentials)

    def set_worksheet(self, worksheet_name):
        self.worksheet = self.gc.open_by_key(self.spreadsheet_id).worksheet(worksheet_name)


    def add_row(self, data, sequence_columns, from_row=''):
        if from_row == '':
            from_row = self._next_available_row(self.worksheet)
        logging.info('adding new row: %s', data)
        self.worksheet.update_cell(from_row, sequence_columns[0], data[0])
        self.worksheet.update_cell(from_row, sequence_columns[1], data[1])
        self.worksheet.update_cell(from_row, sequence_columns[2], data[2])
        self.worksheet.update_cell(from_row, sequence_columns[3], data[3])
        self.worksheet.update_cell(from_row, sequence_columns[4], data[4])

    def find_row_for_allthatmatches(self, key, *args):
        col = args[0][0]
        value_match = args[0][1]
        matching_cells = self.worksheet.findall(key)
        for cell in matching_cells:
            # TODO: check how to sale column (9) condition should be handled (exception function passed as arg?)
            if self.worksheet.cell(cell.row, col).value == str(value_match) and self.worksheet.cell(cell.row, 9).value is None:
                return cell.row

    def update_row(self, row, data, sequence_columns):
        logging.info('updating row: %s, with: %s', row, data)
        self.worksheet.update_cell(row, sequence_columns[0], data[0])
        self.worksheet.update_cell(row, sequence_columns[1], data[1])
        self.worksheet.update_cell(row, sequence_columns[2], data[2])

    def _next_available_row(self, worksheet):
        str_list = list(filter(None, worksheet.col_values(1)))
        return str(len(str_list)+1)