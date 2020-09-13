" 09/12/2020 newly added comment"
from HW_Assignments.HW1_Template.src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import copy
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    def key_to_template(self, key):
        tmp = {}
        for k in self._data['key_columns']:
            tmp = {k: key[k]}
        return tmp

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break
        return result

    def find_by_primary_key(self, key_fields, field_list=None):

        # can only return 1 or 0 row

        result = {}
        if key_fields is None:
            print("No key values are passed")
            return result

        k_set = set(key_fields)

        for row in self._rows:
            row_set = set(row.values())
            if k_set.issubset(row_set):
                if field_list is None:
                    result.update(row)
                    return result
                else:
                    r = {}
                    for col in field_list:
                        r.update({col: row[col]})
                        result.update(r)
                    return result
        print("passed in key value does not exist")
        pass

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):

        # can return more than 1 row if key isn't a primary key

        result = []
        for row in self._rows:
            if self.matches_template(row, template):
                if field_list is None:
                    result.append(row)
                else:
                    r = {}
                    for col in field_list:
                        r.update({col: row[col]})
                    result.append(r)
        if not result:
            print("Passed in template doesn't match any pair"
                  " (key, value) in table")
        return result

    def delete_by_key(self, key_fields):

        if key_fields is None:
            print("No key values are passed")

        i = 0
        for row in self._rows:
            row_set = set(row.values())
            if set(key_fields).issubset(row_set):
                print("Target row:\n", row)
                row.clear()
                i = i+1
                # if target row is deleted, print empty dict
                print("Target row after deletion: ", row)

        if i == 0:
            print("No such row that matches key_value to delete")
        else:
            return i
        pass

    def delete_by_template(self, template):

        """
        pass by template can delete multiple rows at a time if the dict
        doesn't contain any primary key
        """

        i = 0

        for d in range(len(self._rows)):
            if self.matches_template(self._rows[d], template):
                self._rows[d].clear()
                i = i+1

        if i != 0:
            return i
        else:
            print("No such rows; passed in template doesn't match any "
                  "pair (key, value) in table")
        pass

    def update_by_key(self, key_fields, new_values):

        if new_values is None:
            print("Nothing to update")

        values_set = set(new_values.keys())
        tbl_cols = set(self._rows[0].keys())

        if not values_set.issubset(tbl_cols):
            print("Keys don't match")

        i = 0
        for row in self._rows:
            row_set = set(row.values())
            if set(key_fields).issubset(row_set):
                print(row)
                for k in new_values:
                    for col in row:
                        if k == col:
                            row[col] = new_values[k]
                i = i+1
                print("new_values:")
                print(row)
        if i == 0:
            print("No such row")
        else:
            return i
        pass

    def update_by_template(self, template, new_values):

        if new_values is None:
            raise ValueError("Nothing to update")

        values_set = set(new_values.keys())
        tbl_cols = set(self._rows[0].keys())

        if not values_set.issubset(tbl_cols):
            raise ValueError("Keys don't match")

        i = 0
        for l in range(len(self._rows)):
            if self.matches_template(self._rows[l], template):
                print("Initial value:\n", self._rows[l])
                for k in new_values:
                    for col in self._rows[l]:
                        if k == col:
                            self._rows[l][col] = new_values[k]
                i = i + 1
                print("new_values:")
                print(self._rows[l])

        if i == 0:
            print("No such row")
        else:
            return i
        pass

    def insert(self, new_record):

        if new_record is None:
            raise ValueError("Nothing to insert")

        new_cols = set(new_record.keys())
        tbl_cols = set(self._rows[0].keys())

        if not new_cols.issubset(tbl_cols):
            raise ValueError("Keys don't match")

        for row in self._rows:
            if self.matches_template(row, new_record):
                raise ValueError("Try to insert row that is already exists")

        key_cols = self._data.get("key_columns")

        if key_cols is not None:
            key_cols = set(key_cols)
            if not key_cols.issubset(new_cols):
                raise ValueError("Not a candidate key")

            for k in key_cols:
                if new_record.get(k, None) is None:
                    raise ValueError("Candidate key cannot be NULL")

            key_tmp = self.key_to_template(new_record)

            if self.find_by_template(key_tmp) is not None and len(self.find_by_template(key_cols)) > 0:
                raise ValueError("Row already exist")

        print("Initial last row:")
        print(self._rows[len(self._rows)-1])

        self._rows.append(new_record)
        print("After insertion:")
        print(self._rows[len(self._rows) - 1])
        pass

    def get_rows(self):
        return self._rows
