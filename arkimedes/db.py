from pathlib import Path
import re

from sqlalchemy import Column, create_engine, Integer, String, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from arkimedes.config import DB_CONN, DB_TYPE, SQLITE_FILE
from arkimedes.ezid import load_arks_from_ezid_anvl

engine = create_engine(DB_CONN)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class Ark(Base):
    __tablename__ = "arks"

    ark = Column(String(30), primary_key=True)
    target = Column(Text)
    profile = Column(String(8))
    status = Column(String(11))
    owner = Column(String(11))
    ownergroup = Column(String(11))
    created = Column(Integer)
    updated = Column(Integer)
    export = Column(Boolean)
    dc_creator = Column(Text)
    dc_title = Column(Text)
    dc_type = Column(String(20))
    dc_date = Column(Text)
    dc_publisher = Column(Text)
    erc_when = Column(Text)
    erc_what = Column(Text)
    erc_who = Column(Text)
    replaceable = Column(Boolean)

    def from_anvl(self, anvl_string):
        ark_dict = {
            p[0].strip(): p[1].strip()
            for p in [l.split(":", 1) for l in anvl_string.split("\n") if l != ""]
        }

        self.ark = ark_dict.get("success", "")
        self.target = ark_dict.get("_target", "")
        self.profile = ark_dict.get("_profile", "")
        self.status = ark_dict.get("_status", "")
        self.owner = ark_dict.get("_owner", "")
        self.ownergroup = ark_dict.get("_ownergroup", "")
        self.created = int(ark_dict.get("_created", 0))
        self.updated = int(ark_dict.get("_updated", 0))
        if ark_dict.get("_export") == "no":
            self.export = False
        else:
            self.export = True
        self.dc_creator = ark_dict.get("dc.creator", "")
        self.dc_title = ark_dict.get("dc.title", "")
        self.dc_type = ark_dict.get("dc.type", "")
        self.dc_date = ark_dict.get("dc.date", "")
        self.dc_publisher = ark_dict.get("dc.publisher", "")
        self.erc_when = ark_dict.get("erc.when", "")
        self.erc_what = ark_dict.get("erc.what", "")
        self.erc_who = ark_dict.get("erc.who", "")
        self.replaceable = False


def create_db(engine):
    Base.metadata.create_all(engine)


def db_exists():
    global DB_TYPE
    global SQLITE_FILE

    if DB_TYPE == "sqlite":
        return Path(SQLITE_FILE).exists()


def load_batch_download_file_into_db(batch_file):
    arks = load_arks_from_ezid_anvl(batch_file)
    session = Session()

    for a in arks:
        ark_obj = Ark(
            ark=a["ark"],
            target=a["_target"],
            profile=a["_profile"],
            status=a["_status"],
            owner=a["_owner"],
            ownergroup=a["_ownergroup"],
            created=int(a["_created"]),
            updated=int(a["_updated"]),
            export=True if a["_export"] == "yes" else False,
            dc_creator=a["dc.creator"],
            dc_title=a["dc.title"],
            dc_type=a["dc.type"],
            dc_date=a["dc.date"],
            dc_publisher=a["dc.publisher"],
            erc_when=a["erc.when"],
            erc_what=a["erc.what"],
            erc_who=a["erc.who"],
            replaceable=input_is_replaceable(a["dc.title"]),
        )

        session.add(ark_obj)

    session.commit()
    session.close()


def input_is_replaceable(title):
    """Determine of an ARK can be replaced with a new target.

    Occassionally, ARKs have been created for children of a compound object.
    On their own, these ARKs serve no useful purpose. Instead of leaving them,
    we can mark them as replaceable so that we can update already-minted ARKs
    with entirely new metadata and avoid wasting our ARKs.

    Parameters
    ----------
    title : str
        The title of the object in the ARK record. Child objects that don't
        need their own ARKs can be identified by titles like 'Front', 'Back',
        'Page 2', 'p. 7', and even ''.
    
    Returns
    -------
    bool
    """
    bad_title = re.compile(r"(^([Pp](age|\.) \d+|[Ff]ront|[Bb]ack)$|^$)")

    return bool(bad_title.match(title))


def url_is_in_db(resource):
    session = Session()
    # if session.query(Ark.target).filter_by(resource)


def add_to_db(ark_obj):
    session = Session()
    session.add(ark_obj)
    session.commit()
    session.close()


def find_by_ark(ark):
    pass


def find_by_replaceable_arks():
    session = Session()
    results = session.query(Ark.replaceable).filter_by(True)
    session.close()

    return results
