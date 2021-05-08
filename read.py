import json
import multiprocessing as mp
import os
import sqlite3
import subprocess

import click

from utils import Mbox, Sqliter, get_logger

log = get_logger(__file__)


def process_chunk(filename):
    chunk_number = filename.replace(".chunks/chunk_", "").replace(".txt", "")
    json_filename = f"jsons/mail_{chunk_number}.jsonl"
    json_file = open(json_filename, "w")
    insert_batch = []

    sql = Sqliter("example.db")
    mbox = Mbox(filename)
    for message in mbox:
        message.json["attachments"] = []
        for attachment in message.attachments:
            attachment.save("attachments")
            message.json["attachments"].append(attachment.save_filename)
        json_file.write(json.dumps(message.json) + "\n")

        insert_batch.append(
            {
                **message.json,
                **{
                    "attachments": ",".join(message.json["attachments"]),
                    "body": "\n__part__\n".join(message.json["body"]),
                    "jsonfile": json_filename,
                    "srcfile": filename,
                },
            }
        )

    if insert_batch:
        sql.insert(table="messages", insert_batch=insert_batch)

    json_file.close()
    con.close()


def split_mbox(mbox_file):
    """
    Split the mbox file into smaller chunks using awk for parallel processing.

    If mbox file is already split, it does not re-split
    """
    mbox_size = os.path.getsize(mbox_file)
    chunk_size = sum(d.stat().st_size for d in os.scandir(".chunks") if d.is_file())

    if abs(mbox_size - chunk_size) / mbox_size > 0.0001:
        subprocess.run(
            [
                "awk",
                """BEGIN{chunk=0} /^From /{msgs++;if(msgs==5000){msgs=0;chunk++}}{print > ".chunks/chunk_" chunk ".txt"}""",
                mbox_file,
            ],
            check=True,
        )


@click.command()
@click.argument("mbox_file")
def read(mbox_file):
    """
    asdf
    """

    os.makedirs("jsons", exist_ok=True)
    os.makedirs("attachments", exist_ok=True)
    os.makedirs(".chunks", exist_ok=True)

    split_mbox(mbox_file)
    if os.path.exists("example.db"):
        os.remove("example.db")

    chunks = [
        os.path.join(".chunks", c)
        for c in os.listdir(".chunks")
        if c.startswith("chunk_")
    ]

    n_workers = mp.cpu_count()
    pool = mp.Pool(n_workers)

    for chunk in chunks:
        results = pool.apply_async(process_chunk, args=(chunk,))
    pool.close()
    pool.join()
    results.get()


if __name__ == "__main__":
    read()
