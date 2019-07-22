from inspect import cleandoc
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

    def __repr__(self):
        return f"<Ark(ark={self.ark})>"

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
        self.replaceable = input_is_replaceable(ark_dict["dc.title"])

    def to_anvl(self):
        return cleandoc(
            f""":: {self.ark}
                _created: {self.created}
                _export: {"yes" if self.export else "no"}
                _owner: {self.owner}
                _ownergroup: {self.ownergroup}
                _profile: {self.profile}
                _status: {self.status}
                _target: {self.target}
                _updated: {self.updated}
                dc.creator: {self.dc_creator}
                dc.date: {self.dc_date}
                dc.publisher: {self.dc_publisher}
                dc.title: {self.dc_title}
                dc.type: {self.dc_type}
                erc.what: {self.erc_what}
                erc.when: {self.erc_when}
                erc.who: {self.erc_who}
                iastate.replaceable: {self.replaceable}
        """
        )


def add_to_db(ark_obj):
    session = Session()
    session.add(ark_obj)
    session.commit()
    session.close()


def create_db(engine):
    Base.metadata.create_all(engine)


def db_exists():
    global DB_TYPE
    global SQLITE_FILE

    if DB_TYPE == "sqlite":
        return Path(SQLITE_FILE).exists()


def find(filter_col=None, filter=None, session=None):
    close_session = False

    if session is None:
        session = Session()
        close_session = True

    if filter is not None:
        results = session.query(Ark).filter(filter_col == filter)
    else:
        results = session.query(Ark)

    if close_session:
        session.close()

    return results


def find_all():
    return find()


def find_ark(ark, session=None):
    filter_col = Ark.ark
    return find(filter_col, ark, session)


def find_replaceable():
    filter_col = Ark.replaceable
    filter = True
    return find(filter_col, filter)


def find_url(url, session=None):
    filter_col = Ark.target
    filter = url
    return find(filter_col, filter, session)


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


def sync_db():
    """Syncronize local database with EZID records.

    Add records from EZID to local database and update local records with
    values from EZID where the two are mismatched.
    """
    pass


def update_db_record(ark, ark_dict):

    session = Session()
    record = find_ark(ark, session).first()
    
    for key, value in ark_dict.items():
        key = key.replace("_", "").replace(".", "_")
        setattr(record, key, value)

    session.commit()
    session.close()

def url_is_in_db(url):
    return bool(find_url(url))
