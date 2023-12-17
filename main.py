
import os
import json
import logging
import sys
import stocknotes_extract as extraction
import stocknotes_transform as transformation
from gspreadsheet import SpreadsheetIntegration
from google.cloud import vision
from google.cloud import firestore
from google.cloud import storage

storage_client = storage.Client()
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

breaks = 	vision.TextAnnotation.DetectedBreak.BreakType
paragraphs = []
lines = []

SPREADSHEET_ID = os.getenv('spreadsheet')
WORKSHEET = os.getenv('worksheet')

FII_COLUMN = 1
PURCHASE_DATE_COLUMN = 2
PURCHASE_VALUE_COLUMN = 3
QTY_COLUMN = 4
PURCHASE_TAX_COLUMN = 5
SELL_DATE_COLUMN = 9
SELL_VALUE_COLUMN = 10
SALE_TAX_COLUMN = 11


def extract_from_text(text_content, total_pages):
  if "warren" in text_content:
    return extraction.Warren().extract_stock_data(text_content, process_values=True if total_pages > 1 else False)
  elif "btg pactual" in text_content:
    return extraction.Btg().extract_stock_data(text_content, process_values=True)
  else:
    raise NotImplementedError

def convert_fii_code(elements_to_convert, element_index):
  db = firestore.Client(project='smartfinance-bills-beta')
  users_ref = db.collection(u'stock_config')
  docs = users_ref.stream()
  for doc in docs:
    print(f'{doc.id} => {doc.to_dict()}')
    for e in elements_to_convert:
      print(e[element_index])
    fii_codes = [doc.to_dict()[e[element_index]] for e in elements_to_convert]
  return fii_codes

def stock_notes_etl(data: dict):
  total_pages = len(data['responses'])
  logging.info('Document total pages: %s', total_pages)

  # EXTRACT
  logging.info('processing ETL [extract]')
  page_response_text = ''
  for page in range(0, total_pages):
    if  data['responses'][page].get('fullTextAnnotation'):
      page_response_text += data['responses'][page]['fullTextAnnotation']['text']
    else:
      empty_page=data['responses'][page]
      print(f'could not find full text annotation: {empty_page}')
  logging.debug(page_response_text)

  structured_data, fii_in_original_ticker = extract_from_text(page_response_text, total_pages)

  # TODO: check how to search for registries for stock sale

  # TRANSFORM
  logging.info('processing ETL [transform]')
  # Create objects for Stocks to keep the attributes name, value, count, tax
  # Calculate the tax distribution percentage based in value * count
  stock_note = transformation.transform_transaction_from_dict(structured_data)
  stock_note_rows = transformation.transform_to_rows(stock_note)
  if not fii_in_original_ticker:
    stock_note_rows = transformation.convert_element_in_list(convert_fii_code, stock_note_rows, element_index=0)

  # LOAD
  logging.info('processing ETL [load]')

  # Add data into spreadsheet
  spreadsheets = SpreadsheetIntegration(SPREADSHEET_ID, cred_file_path=os.getenv('CRED_FILE_PATH'))
  spreadsheets.set_worksheet(WORKSHEET)
  for sn in stock_note_rows:
    operation_type = sn[5]
    fii_ticket = sn[0]
    quantity = sn[3]
    if operation_type == 'C':
      spreadsheets.add_row(sn, [FII_COLUMN,PURCHASE_DATE_COLUMN,PURCHASE_VALUE_COLUMN,QTY_COLUMN,PURCHASE_TAX_COLUMN])
    elif operation_type == 'V':
      row = spreadsheets.find_row_for_allthatmatches(fii_ticket, (QTY_COLUMN, quantity))
      if row:
        # removing fii ticket and quantity before update
        sn.pop(3)
        sn.pop(0)
        spreadsheets.update_row(row, sn, [SELL_DATE_COLUMN,SELL_VALUE_COLUMN,SALE_TAX_COLUMN])
      else:
        logging.info('row not found for %s update sales workflow. Proceed with manual update.', fii_ticket)
    else:
      raise ValueError('Operation not defined.')
  logging.info('Stock notes added successfully')


if __name__ == "__main__":

  # https://stackoverflow.com/questions/69361854/serviceunavailable-503-dns-resolution-failed-for-service-firestore-googleapis
  os.environ
  os.environ['GRPC_DNS_RESOLVER'] = 'native'
  os.environ

  if len(sys.argv) > 1:
    logging.info('reading local file')
    file_name = sys.argv[1]
    with open(file_name, 'r') as vision_output:
      vision_data = json.loads(vision_output.read())
  else: # get file from cloud storage
    logging.info('reading remote file')
    sa_bucket = 'sfinstock-notes'
    bucket = storage_client.get_bucket(sa_bucket)

    blob_list = [blob for blob in list(bucket.list_blobs(prefix='output')) if not blob.name.endswith('/')]
    logging.info('Output files:')
    for blob in blob_list:
      logging.info(blob.name)
    # this rule was needed because sometimes the output json file is created under "/output" folder (size here considers folder in position "0", file in position "1").
    # Sometimes the output json file is created under root path (size here considers file in position "0")
    if len(blob_list) > 1:
      output = blob_list[1]
    else:
      output = blob_list[0]
    vision_data = json.loads(output.download_as_string())
  # print('\n--------------------------------------------------------------------------------------\n')
  # print(vision_data['responses'][0]['fullTextAnnotation']['text'])
  # print('\n--------------------------------------------------------------------------------------\n')
  # print(vision_data['responses'][1]['fullTextAnnotation']['text'])
  # print('\n--------------------------------------------------------------------------------------\n')
  stock_notes_etl(vision_data)

