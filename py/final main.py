import mysql.connector
import multiprocessing
import time
import xml.etree.ElementTree as ET

start_time = time.perf_counter()

record_types = [
    "article",
    "www",
    "inproceedings",
    "book",
    "phdthesis",
    "proceedings",
    "mastersthesis",
    "incollection",
    "data"
]
num_cpus = multiprocessing.cpu_count()
chunk_size = 50000  # Adjust the chunk size as needed


# Define a function to process individual subroot elements
def process_subroot(subroot_element, table_name):
    tag = {}
    for col in subroot_element:
        tag[col.tag] = col.text

    columns = '`, `'.join(tag.keys())
    values = ', '.join(['%s'] * len(tag))

    query = f"INSERT INTO `{table_name}` (`{columns}`) VALUES ({values})"
    return query, tuple(tag.values())


# Path to your XML file
xml_file_path = "Fixed.xml"


# Database connection function for each process
def get_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        database='PARSER'
    )


# Function for multiprocessing processing
def process_chunk(chunk):
    try:
        local_cnx = get_connection()
        local_cursor = local_cnx.cursor()

        for subroot, table_name in chunk:
            try:
                query, data = process_subroot(subroot, table_name)
                local_cursor.execute(query, data)
                subroot.clear()
            except mysql.connector.Error as err:
                print(err)

        local_cnx.commit()
        local_cursor.close()
        local_cnx.close()
    except mysql.connector.Error as err:
        print("Error:", err)


if __name__ == "__main__":
    # Create a multiprocessing Pool
    with multiprocessing.Pool(processes=4) as pool:
        tree = ET.iterparse(xml_file_path, events=("start", "end"))
        current_chunk = []

        for event, element in tree:
            if event == "end" and element.tag in record_types:
                current_chunk.append((element, element.tag))
                if len(current_chunk) >= chunk_size:
                    pool.apply_async(process_chunk, (current_chunk,))
                    current_chunk = []

        # Process any remaining elements in the last chunk
        if current_chunk:
            pool.apply_async(process_chunk, (current_chunk,))

        pool.close()
        pool.join()

    print("Processing completed.")
    end_time = time.perf_counter()
    total_time = round(end_time - start_time, 2)
    print("Run Time: ", total_time)
