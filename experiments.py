import os
import random
import sqlite3
import struct
from enum import Enum
from typing import List, Optional

BIG_DATA_FILES_DIR_PATH = "Big Data Files"
DB_FILE_PATH = os.path.join(BIG_DATA_FILES_DIR_PATH, "xindex-nocase.db")
LINKS_FILE_PATH = os.path.join(BIG_DATA_FILES_DIR_PATH, "indexbi.bin")


class Namespace(Enum):
    NORMAL = 0
    CATEGORY = 1
    WIKIPEDIA = 2
    PORTAL = 3
    BOOK = 4
    RESERVED = 5
    OTHER = 6


db_conn = sqlite3.connect(DB_FILE_PATH)  # database connection
dc_cur = db_conn.cursor()  # database cursor


def close_connect():
    db_conn.close()


def to_article_title(article_offset: int) -> Optional[str]:
    result = dc_cur.execute("""
    SELECT title FROM pages WHERE offset = ? COLLATE NOCASE LIMIT 1
    """, (str(article_offset),)).fetchall()
    if not result:
        return None
    return result[0][0]


def to_article_offset(article_name: str) -> Optional[int]:
    result = dc_cur.execute("""
    SELECT offset FROM pages WHERE title = ? COLLATE NOCASE LIMIT 1
    """, (article_name,)).fetchall()
    if not result:
        return None
    return result[0][0]


def multiple_offsets_to_titles(article_offsets: List[int]) -> List[str]:
    thing = ", ".join([f"({x})" for x in article_offsets])
    result = dc_cur.execute(f"""
    WITH vals(offs) AS (values (?))
    SELECT title, offset FROM pages
      INNER JOIN vals
    ON pages.offset = vals.offs""", thing).fetchall()
    titles = [x[0] for x in result]
    return titles


# assert to_article_title(to_article_offset("Harry Potter")) == "Harry Potter"


def get_linked_articles(list_of_article_offsets: List[int]) -> List[int]:
    with open(LINKS_FILE_PATH, "rb") as links_file:
        combined_results = set()
        for article_offset in list_of_article_offsets:
            links_file.seek(article_offset)
            headers = links_file.read(4 * 4)
            zero, num_of_links, num_of_bidirectional_links, metadata = \
                struct.unpack("<4i", headers)  # `<` means little endian; `i` means int
            assert zero == 0  # sanity check
            metadata_bits = f"{metadata:b}".zfill(32)
            is_good, is_featured, is_year, is_list = [bool(int(x)) for x in metadata_bits[17:21]]
            namespace_index = int("0b" + metadata_bits[22:24], 2)
            namespace = Namespace(namespace_index)
            is_disambiguation = bool(int(metadata_bits[25]))
            num_of_words_in_title = int("0b" + metadata_bits[26:29], 2)  # maximum 15
            log10_article_length = int("0b" + metadata_bits[29:32], 2)
            linked_article_offsets = struct.unpack(f"<{num_of_links}i", links_file.read(4 * num_of_links))
            combined_results.update(set(linked_article_offsets))
        return list(combined_results)


def do_erdos():
    erdos_number_0 = [to_article_offset("Paul Erdős")]
    print(f"#0 = {len(erdos_number_0)}")
    erdos_number_1 = get_linked_articles(erdos_number_0)
    print(f"#1 = {len(erdos_number_1)}")
    erdos_number_2 = get_linked_articles(erdos_number_1)
    print(f"#2 = {len(erdos_number_2)}")
    erdos_number_3 = get_linked_articles(erdos_number_2)
    print(f"#3 = {len(erdos_number_3)}")
    erdos_number_4 = get_linked_articles(erdos_number_3)
    print(f"#4 = {len(erdos_number_4)}")
    erdos_number_5 = get_linked_articles(erdos_number_4)
    print(f"#5 = {len(erdos_number_5)}")
    print()
    print("--EXAMPLES---")
    for l in [erdos_number_0, erdos_number_1, erdos_number_2, erdos_number_3, erdos_number_4, erdos_number_5]:
        random_article_offset = random.choice(l)
        print(to_article_title(random_article_offset))
    # #0 = 1
    # #1 = 160
    # #2 = 19695
    # #3 = 665810
    # #4 = 3469143
    # #5 = 5181541
    #
    # --EXAMPLES---
    # Paul Erdős
    # Probabilistic method
    # Linkage (mechanical)
    # Archbishop's Garden
    # Talkman
    # Category:Histories of cities in Estonia


def get_num_of_articles():
    with open(LINKS_FILE_PATH, "rb") as links_file:
        _, article_count, _, _ = struct.unpack("<4i", links_file.read(4 * 4))
        return article_count


def calculate_average_links_per_article():
    with open(LINKS_FILE_PATH, "rb") as links_file:
        total_link_count = 0
        total_bidirecitonal_link_count = 0
        version, article_count, _, _ = struct.unpack("<4i", links_file.read(4 * 4))
        print(f"version = {version}, article_count = {article_count}")
        while True:
            headers = links_file.read(4 * 4)
            if headers == b"":  # end of file
                break
            zero, num_of_links, num_of_bidirectional_links, metadata = \
                struct.unpack("<4i", headers)  # `<` means little endian; `i` means int
            total_link_count += num_of_links
            total_bidirecitonal_link_count += num_of_bidirectional_links
            links_file.seek(num_of_links * 4, os.SEEK_CUR)  # go to next article's offset
        average_links_per_article = total_link_count / article_count
        print(f"Average links per article: {average_links_per_article}")


def main():
    calculate_average_links_per_article()


if __name__ == '__main__':
    main()
