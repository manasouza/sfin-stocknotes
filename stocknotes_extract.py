
import re
import logging


TICKER_PATTERN = '\w{4}11|\w{4}[34]{1}[F]?'
DATE_PATTERN = '(\d{2}\/\d{2}\/20\d{2})'
# VALUE_PATTERN = '^[1-9]\d+,\d{2}$' => does not work for values below 10,00
# VALUE_PATTERN = '\d+,\d{2}$'
VALUE_PATTERN = '^[1-9]\d*,\d{2}$'
# The regex is to support situations like this, that already happened
# 10
# 7
# 10 2 5
# 1
QUANTITY_PATTERN = '^(?!0$)(\d{1,2}(?: \d{1,2})*)$'
TAX_PATTERN = '(^\d{1},\d{2})\sD'
# OPERATION_PATTERN = '1-BOVESPA (\w) VISTA'
OPERATION_PATTERN = '1-BOVESPA (\w).*'

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

class Btg:

    ACCOUNT_1 = '002320286'
    ACCOUNT_2 = '004699022'

    structured_data = {}
    fii_names = []
    fii_quantities = []
    fii_values = []
    fii_taxes = []
    fii_op = []
    fii_date = ''

    def extract_stock_data(self, file_content, data = '', process_values=False):
        """_summary_

        Args:
            file_content (_type_): _description_
            data (str, optional): _description_. Defaults to ''.
            process_values (bool, optional): _description_. Defaults to False.

        Raises:
            ValueError: when not able to extract FIIs

        Returns:
            dict: extracted elements with individual data cascaded
            bool: whether the FII element is in its specific ticket name or not
        """
        current_pattern = None
        exclude_tax_indexes = [2, 6]
        tax_index = 0
        for line in file_content.split('\n'):
            if line == 'Quantidade' or line == 'Preço / Ajuste' or line == 'Data pregão' or line == 'Q Negociação C/V Tipo Mercado':
                logging.debug('############# Start Processing...')
                process_values = True
                current_pattern = None
                continue
            if re.match(TICKER_PATTERN, line):
                logging.debug(line)
                # TODO: for FII ok to get line, but stocks don't, i.e. BBAS3F ON
                data += f'|{line}|'
                self.fii_names.append(line)
            elif process_values and re.match(OPERATION_PATTERN, line):
                op = re.search(OPERATION_PATTERN, line).group(1)
                logging.debug('Operation: %s', op)
                # sometimes the enconding is failing on convert the first 'C' occurence, so adding this rule
                if op != 'C' and op != 'V':
                    op = 'C'
                self.fii_op.append(op)
                import ipdb; ipdb.set_trace()
                data += f'|{op}|'
                current_pattern = OPERATION_PATTERN
            # data da transação
            elif process_values and re.match(DATE_PATTERN, line):
                self.fii_date = re.search(DATE_PATTERN, line).group(1)
                logging.debug('Date: %s', self.fii_date)
                data += f'|{self.fii_date}|'
                current_pattern = DATE_PATTERN
            # valor cota
            elif process_values and re.match(VALUE_PATTERN, line):
                logging.debug('Value: %s', line)
                data += f'|{line}|'
                self.fii_values.append(line)
                current_pattern = VALUE_PATTERN
            # quantidade
            elif process_values and re.match(QUANTITY_PATTERN, line):
                logging.debug('Quantity: %s', line)
                data += f'|{line}|'
                self.fii_quantities.append(int(line))
                current_pattern = QUANTITY_PATTERN
            # taxas
            elif re.match(TAX_PATTERN, line):
                logging.debug('Tax: %s', line)
                line = re.search(TAX_PATTERN, line).group(1)
                data += f'|{line}|'
                if tax_index not in exclude_tax_indexes:
                    self.fii_taxes.append(line)
                tax_index += 1
            elif Btg.ACCOUNT_1 == line or Btg.ACCOUNT_2 == line:
                data += f'|{line}|'
                self.account = line

            if current_pattern and process_values and not re.match(current_pattern, line):
                logging.debug('############# Stop Processing...')
                process_values = False
        if not self.fii_values:
            raise ValueError('Values not filled. Check \'process_values\' rules')
        print(data)
        structured_data = _format_extracted_data(self.fii_date, self.fii_names, self.fii_values, self.fii_quantities, self.fii_taxes, self.fii_op)
        print('### COLLECTED DATA ###')
        logging.info('raw data: %s', data)
        print('--------------------------------------------------------------------------------------')
        logging.info('formatted data: %s', structured_data)
        return structured_data, True

class Warren:

    def extract_stock_data(self, file_content, data = '', process_values=False):
        
        structured_data = {}
        fii_names = []
        fii_quantities = []
        fii_values = []
        fii_taxes = []
        fii_op = []
        fii_date = ''

        # This is useful to analyze the file_content.split('\n') array
        for line in file_content.split('\n'):

            if re.match(OPERATION_PATTERN, line):
                op = re.search(OPERATION_PATTERN, line).group(1)
                if op != 'C' and op != 'V':
                    op = 'C'
                logging.debug('Operação: %s', op)
                fii_op.append(op)
            if line == 'Total Corretagem/Despesas':
                logging.debug('############# Start Processing...')
                process_values = True
                continue
            elif line == 'VALOR/AJUSTE DIC':
                logging.debug('############# Stop Processing...')
                process_values = False
                continue
            if re.search('^FII.*', line):
                logging.debug(line)
                data += f'|{line}|'
                fii_names.append(line)
            # data da transação
            elif re.search('.*(\d{2}\/\d{2}\/20\d{2}).*', line):
                logging.debug('Date: %s', self.fii_date)
                fii_date = re.search('.*(\d{2}\/\d{2}\/20\d{2}).*', line).group(1)
                data += f'|{fii_date}|'
            # valor cota
            elif re.search('^[1-9]\d+[,]\d{2}$', line) and process_values:
                logging.debug('Value: %s', line)
                data += f'|{line}|'
                fii_values.append(line)
            # quantidade
            elif re.search(QUANTITY_PATTERN, line) and process_values:
                qtys = line.split(" ")
                logging.debug('Quantity: %s', line)
                data += ''.join([f'|{q}|' for q in qtys])
                fii_quantities.extend([int(qty) for qty in qtys])
            # taxas
            elif re.search('(^\d{1},\d{2})\sD', line):
                logging.debug('Tax: %s', line)
                line = re.search('(^\d{1},\d{2})\sD', line).group(1)
                data += f'|{line}|'
                fii_taxes.append(line)
        if not fii_values:
            raise ValueError('Values not filled. Check \'process_values\' rules')
        structured_data = _format_extracted_data(fii_date, fii_names, fii_values, fii_quantities, fii_taxes, fii_op)
        print('### COLLECTED DATA ###')
        logging.info('raw data: %s', data)
        print('--------------------------------------------------------------------------------------')
        logging.info('formatted data: %s', structured_data)
        return structured_data, False


def _fill_extracted_data(extracted_data: dict, key, keyvalues={}, unique_value=''):
    """_summary_

    Args:
        extracted_data (dict): _description_
        key (_type_): _description_
        keyvalues (tuple): _description_

    Returns:
        _type_: _description_
    """
    # import ipdb; ipdb.set_trace()

    if extracted_data.get(key) is None:
        if unique_value:
            extracted_data[key] = unique_value
        else:
            extracted_data[key] = []
            extracted_data[key].append(keyvalues)
    else:
        if unique_value:
            extracted_data[key] = unique_value
        else:
            extracted_data[key].append(keyvalues)

    return extracted_data

def _format_extracted_data(fii_date, fii_names, fii_values, fii_quantities, fii_taxes, fii_op):
    extracted_data = {}
    for index in range(0, len(fii_names)):
        name = fii_names[index]
        value = fii_values[index]
        quantity = fii_quantities[index]
        operation = fii_op[index]
        extracted_data = _fill_extracted_data(extracted_data, name, keyvalues={'value':value,'quantity':quantity,'operation':operation})
    _fill_extracted_data(extracted_data, 'tax', unique_value=fii_taxes)
    _fill_extracted_data(extracted_data, 'date', unique_value=fii_date)
    return extracted_data
