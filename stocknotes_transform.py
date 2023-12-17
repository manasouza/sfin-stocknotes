import decimal
from decimal import Decimal

decimal.getcontext().prec = 2
decimal.getcontext().rounding = decimal.ROUND_HALF_DOWN

class StockNoteTransaction:

  def __init__(self, ticker_name: str, operation: str, quantity: int, value: decimal):
    self.ticker_name = ticker_name
    self.quantity = quantity
    self.value = value
    self.operation = operation

  def set_transaction_date(self, date: str):
    self.transaction_date = date

  def set_tax(self, tax_value: decimal):
    self.tax = tax_value


class StockNote:

  def __init__(self, total_stock_value: decimal, total_tax: decimal, transaction_date: str, transactions: list):
    self.total_value = total_stock_value + total_tax
    self.total_tax = total_tax
    self.transaction_date = transaction_date
    self.transactions = transactions

  def calculate_individual_tax(self, self_value):
    pass


def transform_transaction_from_dict(structured_data: dict):
    stock_transactions = []
    for k in structured_data.keys():

      # if k.startswith('FII'):

      # TODO: FIIs may have different formats amongst stock notes. This method could receive the regex pattern used for FIIs in EXTRACT module, or use comparison to not process 'tax' and 'date' registries
      # if re.match('\w{4}11', k):
      if k != 'tax' and k != 'date':
        for transaction_values in structured_data[k]:
          quantity = transaction_values['quantity']
          value = transaction_values['value']
          operation = transaction_values['operation']
          stock_transactions.append(StockNoteTransaction(k, operation, quantity, Decimal(value.replace(',','.'))))
    # calculation
    total_tax = sum([Decimal(tax.replace(',','.')) for tax in structured_data['tax']])
    total_value = sum([t.value * t.quantity for t in stock_transactions])
    factors = []
    for t in stock_transactions:
      factor = ((t.value*t.quantity) * total_tax)/total_value
      t.set_tax(factor)
      t.set_transaction_date(structured_data['date'])
      factors.append(factor)
    print(sum(factors) == total_tax)
    return StockNote(total_value, total_tax, structured_data['date'], stock_transactions)

def transform_to_rows(stock_note: StockNote):
    transactions = []
    for t in stock_note.transactions:
        transaction_registry = []
        transaction_registry.append(t.ticker_name)
        transaction_registry.append(t.transaction_date)
        transaction_registry.append(str(t.value).replace('.',','))
        transaction_registry.append(t.quantity)
        transaction_registry.append(str(t.tax).replace('.',','))
        transaction_registry.append(t.operation)
        transactions.append(transaction_registry)
    return transactions

def convert_element_in_list(converter_function, elements: list, element_index=0):
  converted_list = []
  if converter_function:
    converted_elements = converter_function(elements, element_index)
  for index, c in enumerate(converted_elements):
    # implementation only for element index == 0 ([1:])
    # TODO: check how to calculate this index if != 0
    iel = elements[index][1:]
    iel.insert(element_index, c)
    converted_list.append(iel)
  return converted_list

