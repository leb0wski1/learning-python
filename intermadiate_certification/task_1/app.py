import re

phone_regexp = re.compile(r'(\+7[9]{1}\d{9})')

def phone_number_detect_with_file_read(input_file):

    """Вытащить список номеров телефонов из файла."""
    str_to_process = ""

    with open(input_file, "r") as fd:

        str_to_process = fd.read()

    return phone_regexp.findall(str_to_process)