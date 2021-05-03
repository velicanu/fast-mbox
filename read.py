import mailbox
import base64
import os
import logging
from binascii import Error as BinasciiError
from dateutil import parser
import multiprocessing as mp


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
    date = parser.parse(datestring.split("+")[0]).date().isoformat()

    suffix = 1
    new_filename = f"{date}-{filename}"
    while os.path.exists(os.path.join(folder, new_filename)):
        new_filename = f"{date}-{suffix}-{filename}"
        suffix += 1
    return os.path.join(folder, new_filename)


def save_file(filename, attachment, date):
    new_filename = get_new_filename(filename, "attachments", date)
    log.info(f"{new_filename}")

    try:
        with open(new_filename, "wb") as fh:
            fh.write(base64.b64decode(attachment))
    except BinasciiError:
        # In some mailboxes (looking at you gmail), txt files claim to be base64 encoded
        # when in fact they are just plain text
        with open(new_filename, "w") as fh:
            fh.write(attachment)


def process_chunk(filename):
    mbox = mailbox.mbox(filename)
    for message in mbox:
        for part in message.walk():
            if (
                # type(part) != str
                part.get_content_disposition() == "attachment"
                and get_filename(part)
            ):
                save_file(
                    part.get_filename(),
                    part.get_payload(),
                    message["date"],
                )


def read():
    chunks = [
        os.path.join("chunks", c)
        for c in os.listdir("chunks")
        if c.startswith("chunk_")
    ]

    n_workers = mp.cpu_count()
    pool = mp.Pool(n_workers)

    for chunk in chunks:
        # process_chunk(chunk)
        pool.apply_async(process_chunk, args=(chunk,))

    pool.close()
    pool.join()


if __name__ == "__main__":
    read()
