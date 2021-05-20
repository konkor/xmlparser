#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 20 10:25:52 2021
@author: konkor
"""

import xml.etree.ElementTree as ET
import psycopg2

from psycopg2 import OperationalError

ET.register_namespace("", "http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData")
ET.register_namespace("xn", "http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm")
ET.register_namespace("in", "http://www.3gpp.org/ftp/specs/archive/32_series/32.695#inventoryNrm")

ns = {
    "":"http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData",
    "xn":"http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm",
    "in":"http://www.3gpp.org/ftp/specs/archive/32_series/32.695#inventoryNrm"
    }

inventory_unit = {
    "id":None,
    "inventoryUnitType":None,
    "vendorUnitFamilyType":None,
    "vendorUnitTypeNumber":None,
    "vendorName":None,
    "serialNumber":None,
    "dateOfManufacture":None,
    "unitPosition":None,
    "manufacturerData":None,
    "ParentUnit":None
    }

db_host = "127.0.0.1"
db_port = "5432"
db_name = "xml_parser"
db_user = "postgres"
db_pass = "abc123"

create_inventory_unit_table = """
CREATE TABLE IF NOT EXISTS inventory_units (
  id SERIAL PRIMARY KEY,
  name_id TEXT, 
  inventoryUnitType TEXT,
  vendorUnitFamilyType TEXT,
  vendorUnitTypeNumber TEXT,
  vendorName TEXT,
  serialNumber TEXT,
  dateOfManufacture TEXT,
  unitPosition TEXT,
  manufacturerData TEXT,
  ParentUnit TEXT
);
"""

insert_query = """
INSERT INTO inventory_units (
  name_id, 
  inventoryUnitType,
  vendorUnitFamilyType,
  vendorUnitTypeNumber,
  vendorName,
  serialNumber,
  dateOfManufacture,
  unitPosition,
  manufacturerData,
  ParentUnit
) VALUES %s;
"""

class Database():
    connection = None

    def __init__(self):
        self.create_connection()
        
        #Create inventory_units table
        self.execute_query(create_inventory_unit_table)

    def create_connection(self):
        try:
            self.connection = psycopg2.connect(
                database=db_name,
                user=db_user,
                password=db_pass,
                host=db_host,
                port=db_port,
            )
            print("Connection to PostgreSQL DB successful")
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        return

    def execute_query(self, query, values = ()):
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        try:
            if len(values) == 0:
                cursor.execute(query)
            else:
                cursor.execute(query,values)
            print("Query executed successfully")
        except OperationalError as e:
            print(f"The error '{e}' occurred")
        return


class XMLParser():
    inventory_units = []

    def __init__(self, filename):
        self.database = Database()
        self.tree = ET.parse(filename)
        self.root = self.tree.getroot()
        #ET.dump(self.tree)
        #print (ns)
        
        if self.parse() == True:
            print ("XML file:", filename, "parsed successfully.")

        #print (self.inventory_units)
        for x in self.inventory_units:
            self.add_unit(x)

    def parse(self):
        for u in self.root.findall(".//xn:ManagedElement/in:InventoryUnit", ns):
            self.parse_unit(u)
        return True

    def parse_unit(self, element, parent_id = None):
        unit = inventory_unit.copy()
        unit["id"] = element.attrib["id"];
        if parent_id :
            unit["ParentUnit"] = parent_id;
        for a in element.findall("in:attributes", ns):
            self.parse_attributes(a, unit)
        for a in element.findall("in:InventoryUnit", ns):
            self.parse_unit(a, unit["id"])

        return True

    def parse_attributes(self, element, unit):
        x = element.find("in:inventoryUnitType", ns)
        if x is not None:
            unit["inventoryUnitType"] = x.text

        x = element.find("in:vendorUnitFamilyType", ns)
        if x is not None:
            unit["vendorUnitFamilyType"] = x.text

        x = element.find("in:vendorUnitTypeNumber", ns)
        if x is not None:
            unit["vendorUnitTypeNumber"] = x.text

        x = element.find("in:vendorName", ns)
        if x is not None:
            unit["vendorName"] = x.text

        x = element.find("in:serialNumber", ns)
        if x is not None:
            unit["serialNumber"] = x.text

        x = element.find("in:dateOfManufacture", ns)
        if x is not None:
            unit["dateOfManufacture"] = x.text

        x = element.find("in:unitPosition", ns)
        if x is not None:
            unit["unitPosition"] = x.text

        x = element.find("in:manufacturerData", ns)
        if x is not None:
            unit["manufacturerData"] = x.text

        self.inventory_units.append (unit)

        return unit

    def add_unit(self, unit):
        # if unit["id"] is None:
        #     unit["id"] = "NULL"
        # if unit["inventoryUnitType"] is None:
        #     unit["inventoryUnitType"] = "NULL"
        # if unit["vendorUnitFamilyType"] is None:
        #     unit["vendorUnitFamilyType"] = "NULL"
        # if unit["vendorUnitTypeNumber"] is None:
        #     unit["vendorUnitTypeNumber"] = "NULL"
        # if unit["vendorName"] is None:
        #     unit["vendorName"] = "NULL"
        # if unit["serialNumber"] is None:
        #     unit["serialNumber"] = "NULL"
        # if unit["dateOfManufacture"] is None:
        #     unit["dateOfManufacture"] = "NULL"
        # if unit["unitPosition"] is None:
        #     unit["unitPosition"] = "NULL"
        # if unit["manufacturerData"] is None:
        #     unit["manufacturerData"] = "NULL"
        # if unit["ParentUnit"] is None:
        #     unit["ParentUnit"] = "NULL"
            
        values = [(unit["id"],
                  unit["inventoryUnitType"],
                  unit["vendorUnitFamilyType"],
                  unit["vendorUnitTypeNumber"],
                  unit["vendorName"],
                  unit["serialNumber"],
                  unit["dateOfManufacture"],
                  unit["unitPosition"],
                  unit["manufacturerData"],
                  unit["ParentUnit"]
                 )]
        print (values)
        self.database.execute_query(insert_query, values)
        
            
        return

parser = XMLParser("test.xml")
