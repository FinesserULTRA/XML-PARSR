import xml.etree.ElementTree as ET
import re

tree = ET.parse('Cleaned.xml')

root = tree.getroot()

for child in root:
    print(f"{child.tag}: {child.attrib}")
    for c in child:
        print(f"{c.tag}: {c.text}")