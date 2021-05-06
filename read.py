import mailbox
import base64
import os
import logging
from binascii import Error as BinasciiError
from dateutil import parser
import multiprocessing as mp
import subprocess
import click
import json
import hashlib
import sqlite3
import copy

logging.basicConfig(
    format="%(asctime)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def get_filename(payload):
    raw_fn = payload.get_filename()
    if (
        raw_fn
        and raw_fn != "Attached Message Part"
        and not raw_fn.endswith(".ics")
        and payload.get("Content-Transfer-Encoding") == "base64"
    ):
        return raw_fn


def get_new_filename(filename, folder, datestring):
    # strip timezone from datestring since it sometimes breaks parsing
    date = parser.parse(datestring.split("+")[0].split("-")[0]).date().isoformat()

    suffix = 1
    new_filename = f"{date}-{filename}"
    while os.path.exists(os.path.join(folder, new_filename)):
        new_filename = f"{date}-{suffix}-{filename}"
        suffix += 1
    return os.path.join(folder, new_filename)


def save_file(filename, attachment, date, message, part):
    new_filename = get_new_filename(filename, "attachments", date)
    # log.info(f"{new_filename}")

    try:
        with open(new_filename, "wb") as fh:
            fh.write(base64.b64decode(attachment))
    except BinasciiError:
        # In some mailboxes (looking at you gmail), txt files claim to be base64 encoded
        # when in fact they are just plain text
        with open(new_filename, "w") as fh:
            fh.write(attachment)

    return new_filename


def save_attachment(message, part):
    if part.get_content_disposition() == "attachment" and get_filename(part):
        return save_file(
            part.get_filename(), part.get_payload(), message["date"], message, part
        )


def get_json(message, part, json_file):
    if (
        part["Content-Type"]
        and "text" in part["Content-Type"]
        and type(part.get_payload()) == str
        and message["date"]
    ):
        output = {
            "from_": message["from"],
            "to_": message["to"],
            "subject": message["subject"],
            "date": parser.parse(
                message["date"].split("+")[0].split("-")[0]
            ).isoformat(),
            "body": part.get_payload(),
            # "attachment": "",
        }
        output["hash"] = hashlib.sha256(json.dumps(output).encode()).hexdigest()[:16]
        return output


def insert(batch, con):
    first = batch[0]
    qs = ["?" for v in first]
    con.executemany(
        f"""
        INSERT INTO messages({",".join(first.keys())})
        VALUES ({",".join(qs)})
        """,
        [tuple(v.values()) for v in batch],
    )
    con.commit()
    log.info(f"Inserted {len(batch)} messages into database.")


def process_chunk(filename, attachment, jsons, database):
    mbox = mailbox.mbox(filename)
    chunk_number = filename.replace(".chunks/chunk_", "").replace(".txt", "")
    os.makedirs("jsons", exist_ok=True)
    json_file = open(f"jsons/mail_{chunk_number}.jsonl", "w")

    con = sqlite3.connect("example.db")
    insert_batch = []

    for message in mbox:
        for part in message.walk():
            output_json = get_json(message, part, json_file)
            if attachment:
                saved_file = save_attachment(message, part)
                if saved_file and message["message-id"]:
                    print("a", message["subject"], message["message-id"])
            if jsons and output_json:
                if message["message-id"]:
                    # if message["subject"] and "LNS" in message["subject"]:
                    # breakpoint()
                    print("j", message["subject"], message["message-id"], output_json)
                json_file.write(json.dumps(output_json) + "\n")
            if database and output_json:
                trimmed = copy.deepcopy(output_json)
                trimmed["body"] = output_json["body"][:255]
                insert_batch.append(trimmed)

    if insert_batch:
        insert(insert_batch, con)
    json_file.close()
    con.close()


def split_mbox(mbox_file):
    """
    Split the mbox file into smaller chunks using awk for parallel processing.

    If mbox file is already split, it does not re-split
    """
    os.makedirs(".chunks", exist_ok=True)
    mbox_size = os.path.getsize(mbox_file)
    chunk_size = sum(d.stat().st_size for d in os.scandir(".chunks") if d.is_file())

    if abs(mbox_size - chunk_size) / mbox_size > 0.0001:
        subprocess.run(["awk", "-f", "awk.txt", mbox_file], check=True)


@click.command()
@click.argument("mbox_file")
@click.option("-a", "--attachments", is_flag=True)
@click.option("-j", "--jsons", is_flag=True)
@click.option("-d", "--database", is_flag=True)
def read(mbox_file, attachments, jsons, database):
    """
    asdf
    """

    split_mbox(mbox_file)
    with sqlite3.connect("example.db") as con:
        con.execute(
            """
CREATE TABLE IF NOT EXISTS messages
(from_ text, to_ text, subject text, date text, body text, hash text)
            """
        )

    chunks = [
        os.path.join(".chunks", c)
        for c in os.listdir(".chunks")
        if c.startswith("chunk_")
    ]

    n_workers = mp.cpu_count()
    pool = mp.Pool(n_workers)

    for chunk in chunks:
        process_chunk(chunk, True, True, True)
        # pool.apply_async(process_chunk, args=(chunk, True, True, True))
    pool.close()
    pool.join()


if __name__ == "__main__":
    read()
