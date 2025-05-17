import base64
import email.parser
import hashlib
import json
import logging
import os
import re
from binascii import Error as BinasciiError

import dateutil.parser


def get_logger(name):
    logging.basicConfig(
        format="%(asctime)s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)


log = get_logger(__file__)


class Mbox:
    """
    Class for iterating through a Mbox file for read-only access.
    """

    def __init__(self, infile):
        self.infile = infile
        self.parser = email.parser.Parser()

    def __iter__(self):
        for raw_msg in self._get_raw_msgs():
            yield Message(
                message=self.parser.parsestr(raw_msg),
                raw_msg=raw_msg,
                srcfilename=self.infile,
            )

    def _get_raw_msgs(self):
        with open(self.infile) as infile:
            raw_msg = ""
            for line in infile:
                if line.startswith("From ") and raw_msg:
                    yield raw_msg
                    raw_msg = ""
                raw_msg += line
            yield raw_msg


class Message:
    """
    Class representing a single messsage within an Mbox file.
    """

    def __init__(self, message, raw_msg, srcfilename):
        self.message = message
        self.raw_msg = raw_msg
        self.srcfilename = srcfilename
        if "date" not in self.message:
            self.message["date"] = re.search(r"^From \d*@xxx (.*)", self.raw_msg)[1]
        self._add_date()
        self._add_base_json()
        self.attachments = []
        self.body = []

        for part in self.message.walk():
            self._add_attachment(part)
            self._add_body(part)

        self.hash = hashlib.sha256(json.dumps(self.json).encode()).hexdigest()[:20]
        self.json["hash"] = self.hash

    def __getitem__(self, name):
        return self.message[name]

    def _add_date(self):
        try:
            self.date = dateutil.parser.parse(self.message["date"])
        except dateutil.parser._parser.ParserError:
            fixed_string = self.message["date"]

            # add -offset to dates that don't have it
            pattern = r"(.*\d{1,2}:\d{2}:\d{2})\s+(\d{4})$"
            if re.match(pattern, self.message["date"]):
                fixed_string = re.sub(pattern, r"\1 -\2", self.message["date"])

            # remove space before offset
            pattern = r"(.*\d{1,2}:\d{2}:\d{2})\s+([+-])\s+(\d{4})$"
            if re.match(pattern, self.message["date"]):
                fixed_string = re.sub(pattern, r"\1 \2\3", self.message["date"])

            self.date = self.message["_date_obj"] = dateutil.parser.parse(
                fixed_string.split("(")[0]
            )

    def _add_base_json(self):
        self.json = {
            "from": self.message["from"],
            "to": self.message["to"],
            "subject": str(self.message["subject"]),
            "date": self.date.isoformat() if self.date else "",
            "attachments": [],
            "body": [],
            "srcfile": self.srcfilename,
        }

    def _add_body(self, part):
        if (
            part["Content-Type"]
            and "text" in part["Content-Type"]
            and isinstance(part.get_payload(), str)
        ):
            body = part.get_payload()
            self.body.append(body)
            self.json["body"].append(body)

    def _add_attachment(self, part):
        if self._has_attachment(part):
            attachment = Attachment(
                filename=part.get_filename(), data=part.get_payload(), date=self.date
            )
            self.attachments.append(attachment)
            self.json["attachments"].append(attachment.original_filename)

    def _has_attachment(self, part):
        if (
            part.get_content_disposition() == "attachment"
            and part.get_filename()
            and part.get_filename() != "Attached Message Part"
            and not part.get_filename().endswith(".ics")
            and part.get("Content-Transfer-Encoding") == "base64"
        ):
            return True
        return False


class Attachment:
    """
    Class representing an email attachment.
    """

    def __init__(self, filename, data, date):
        self.original_filename = filename
        self.clean_filename = re.sub(r"[^a-zA-Z0-9\-\_\.]", "", filename)
        self.data = data
        self.date = date

    def save(self, folder, clean=True):
        """
        Saves the attachment to a file in the folder.

        The filename will be the filename in the attachment, prefixed
        with the message date and an increasing number if already exists.
        If clean is true, any non alphanumeric - _ chars are removed.
        """
        output_root = self.clean_filename if clean else self.original_filename
        self.save_filename = self._get_new_filename(output_root, folder)
        try:
            with open(self.save_filename, "wb") as fh:
                fh.write(base64.b64decode(self.data))
        except BinasciiError:
            # Sometimes txt files claim to be base64 encoded
            # when in fact they are just plain text
            with open(self.save_filename, "w") as fh:
                fh.write(self.data)

    def _get_new_filename(self, output_root, folder):
        suffix = 1
        date_ = self.date.date().isoformat()
        new_filename = f"{date_}-{output_root}"
        while os.path.exists(os.path.join(folder, new_filename)):
            new_filename = f"{date_}-{suffix}-{output_root}"
            suffix += 1
        return os.path.join(folder, new_filename)
