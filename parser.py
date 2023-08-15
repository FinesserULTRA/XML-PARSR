import xml.etree.ElementTree as ET

import mysql.connector

output_file_path = "column_check_result.txt"


def extract_nested_tags_and_data(element):
    nested_data = {}
    for child in element:
        if child.text and child.tag not in nested_data:
            nested_data[child.tag] = child.text.strip()
    return nested_data


def nestedtags():
    nested_tags = []
    cnx = mysql.connector.connect(host='localhost', user='root', database='PARSER')
    cursor = cnx.cursor()
    tree = ET.parse('GFG.xml')
    root = tree.getroot()

    for child in root:
        if child.tag == 'article':
            for nested in child:
                if nested.tag not in nested_tags:
                    nested_tags.append(nested.tag)
                else:
                    pass

    print("updated to ", nested_tags)

    for column in nested_tags:
        cursor.execute(f"SHOW COLUMNS FROM `article` LIKE '{column}'")
        # print(f"SHOW COLUMNS FROM TABLE `article` LIKE '{column}'")
        existing_column = cursor.fetchone()

        if not existing_column:
            cursor.execute(f"ALTER TABLE `article` ADD COLUMN '{column}' varchar(255)")
            print(f"ALTER TABLE `temp` ADD COLUMN {column} varchar(255)")

        cnx.commit()


def parse_large_xml(file_path):
    cnx = mysql.connector.connect(host='localhost', user='root', database='PARSER')
    cursor = cnx.cursor()

    insert_query = ''
    tree = ET.parse(file_path)
    root = tree.getroot()

    # nestedtags()
    data_dict = {}
    val = []
    # for child in root:  # root -> article,book,www wahtv
    # for elem in root.iter(child.tag):  # elem -> author, book, url whatever
    for elem in root.findall('article'):
        nested_data = extract_nested_tags_and_data(elem)
        # print(f"{elem.tag.lower()} Nested Data:")
        for tag, data in nested_data.items():
            data_dict[tag] = data
            val.append(data)

        columns = '`, `'.join(data_dict.keys())
        # for nested_tag in elem:
        #     column_name = nested_tag.tag
        #     print('showing')
        #     cursor.execute(f"SHOW COLUMNS FROM `article` LIKE '%s'" % column_name)
        #     existing_column = cursor.fetchone()

        # if not existing_column:
        #     print('altering')
        #     cursor.execute(f"ALTER TABLE `article` ADD COLUMN {column_name} VARCHAR(255)")
        #     cnx.commit()

        insert_query = f"INSERT INTO `article` (`{columns}`) VALUES {tuple(val)}"
        print(insert_query)
        cursor.execute(insert_query)
        cnx.commit()
        #     # with open(output_file_path, "a") as output_file:
        #     #     output_file.write(insert_query)
        #     #     output_file.write("\n")
        #     #     output_file.write(str(a))
        #     #     output_file.write("\n")
        #     #     a += 1

        val = []
        data_dict = {}


if __name__ == "__main__":
    xml_file_path = "GFG.xml"
    parse_large_xml(xml_file_path)
