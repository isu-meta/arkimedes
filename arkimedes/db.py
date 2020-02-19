"""Classes and functions fork working with the arkimedes database.

To improve various operations, arkimedes tracks ARK records in a local
database. This allows for flexibility in searching through minted ARKs,
tracking local metadata not stored by EZID, and reduces the need to query
the EZID API to get information about existing records.

Classes and functions in this module:

* Ark()
* Base()
* engine()
* Session()

* add_to_db(ark_obj)
* create_db(engine)
* db_exists()
* dump_db(out_file)
* find(filter_col=None, filter=None, session=None)
* find_all(session=None)
* find_ark(ark, session=None)
* find_replaceable(session=None)
* find_url(url, session=None)
* input_is_replaceable(title)
* load_anvl_file_into_db(batch_file, engine=engine, verbose=True)
* sync_db()
* update_db_record(ark, ark_dict)
* url_is_in_db(url)
"""
from copy import copy
from inspect import cleandoc
from pathlib import Path
import re
import sys

from sqlalchemy import Column, create_engine, Integer, String, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from arkimedes.config import DB_CONN, DB_TYPE, SQLITE_FILE
from arkimedes.ezid import load_anvl_as_dict, load_anvl_as_str

engine = create_engine(DB_CONN)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class Ark(Base):
    """Map ARK records to database rows.

    This class also serves as the schema for the arks table in the database.

    Parameters
    ----------
    ark : str
        Digital object's minted ARK. Primary key for ``arks`` table.
    target : str
        URL the ARK resolves to.
    profile : str
        Preferred metadata profile.
    status : str
        Valid options are: public, reserved, and unavailable.
    owner : str
        Institution owning the ARK.
    ownergroup : str
        Owner group of the ARK.
    created : int
        Unix timestamp of the time the ARK was created.
    updated : int
        Unix timestamp of when the ARK was last changed.
    export : bool
        If the ARK is publicized via external indexing and harvesting
        services.
    dc_creator : str
        Creator of the target resource.
    dc_title : str
        Title of the target resource.
    dc_type : str
        DCMI Type of the target resource.
    dc_date : str
        ISO-8601-formatted date of the target resource's creation.
    dc_publisher : str
        Publishing institution of the target resource.
    erc_when : str
        ISO-8601-formatted date of the target resource's creation.
    erc_what : str
        Title of the target resource.
    erc_who : str
        Creator of the target resource.
    replaceable : bool
        Whether the metadata in a given ARK record can be replaced with
        metadata describing a different resource.

    Attributes
    ----------
    ark : str
        Digital object's minted ARK. Primary key for ``arks`` table.
    target : str
        URL the ARK resolves to.
    profile : str
        Preferred metadata profile.
    status : str
        Valid options are: public, reserved, and unavailable.
    owner : str
        Institution owning the ARK.
    ownergroup : str
        Owner group of the ARK.
    created : int
        Unix timestamp of the time the ARK was created.
    updated : int
        Unix timestamp of when the ARK was last changed.
    export : bool
        If the ARK is publicized via external indexing and harvesting
        services.
    dc_creator : str
        Creator of the target resource.
    dc_title : str
        Title of the target resource.
    dc_type : str
        DCMI Type of the target resource.
    dc_date : str
        ISO-8601-formatted date of the target resource's creation.
    dc_publisher : str
        Publishing institution of the target resource.
    erc_when : str
        ISO-8601-formatted date of the target resource's creation.
    erc_what : str
        Title of the target resource.
    erc_who : str
        Creator of the target resource.
    replaceable : bool
        Whether the metadata in a given ARK record can be replaced with
        metadata describing a different resource.
    """

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
        """How an instance represents itself."""
        return f"<Ark(ark={self.ark})>"

    def from_anvl(self, anvl_string):
        """Populate attributes from a single-record ANVL string.
        
        Parameters
        ----------
        anvl_string : str
            Valid ANVL. See `A Name-Value Language (ANVL) <https://tools.ietf.org/search/draft-kunze-anvl-02>`_
            for more details.

        Returns
        -------
        None
        """
        ark_dict = {
            (p[0].strip() if p[0].strip() != "" else "success"): (
                p[1].strip() if p[0].strip() != "" else p[1][2:].strip()
            )
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
        self.export = 0 if ark_dict.get("_export") == "no" else 1
        self.dc_creator = ark_dict.get("dc.creator", "")
        self.dc_title = ark_dict.get("dc.title", "")
        self.dc_type = ark_dict.get("dc.type", "")
        self.dc_date = ark_dict.get("dc.date", "")
        self.dc_publisher = ark_dict.get("dc.publisher", "")
        self.erc_when = ark_dict.get("erc.when", "")
        self.erc_what = ark_dict.get("erc.what", "")
        self.erc_who = ark_dict.get("erc.who", "")
        self.replaceable = (
            1
            if ark_dict.get("iastate.replaceable") == "True"
            or input_is_replaceable(ark_dict.get("dc.title", "NOT REPLACEABLE"))
            else 0
        )

    def to_anvl(self):
        """Return attributes as an ANVL string.

        Returns
        -------
        str
        """
        anvl = cleandoc(
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

        return f"{anvl}\n\n"


def add_to_db(ark_obj):
    """Add row to the database's arks table.

    Parameters
    ----------
    ark_obj : arkimedes.db.Ark

    Returns
    -------
    None
    """
    session = Session()
    session.add(ark_obj)
    session.commit()
    session.close()


def create_db(engine):
    """Initialize a new database.
    
    Returns
    -------
    None
    """
    Base.metadata.create_all(engine)


def db_exists():
    """Check if database exists.

    Check if the database arkimedes has been initialized. Currently only
    supports checking for SQLite databases.

    Returns
    -------
    bool
    """
    global DB_TYPE
    global SQLITE_FILE

    if DB_TYPE == "sqlite":
        return Path(SQLITE_FILE).exists()


def dump_db(out_file):
    """Dump database contents to an ANVL file.

    Parameters
    ----------
    out_file: str or pathlib.Path
        File to write database contents to.

    Returns
    -------
    None
    """
    arks = find_all()
    with open(out_file, "w", encoding="utf-8") as fh:
        for ark in arks:
            fh.write(ark.to_anvl())


def find(filter_col=None, filter=None, session=None):
    """Find matching rows in the database.
    
    Finds one or more rows that match the ``filter`` term in the
    ``filter_col``. Currently only supports equality match. filter_col and
    filter require each other. ``session`` is always optional and is most
    useful when an edit function needs to update the results of a query.
    If ``filter_col`` and ``filter`` are not provided, returns the entire
    arks table.

    Parameters
    ----------
    filter_col : str or None
        Table column to search. Optional, but required if using filter.
        Defaults to None.
    filter : str or None
        Search term to use. Optional, but required if using filter_col.
        Defaults to None.
    session : sqlalchemy.orm.Session or None
        Session object to conduct query with. Optional. Defaults to None.

    Returns
    -------
    sqlalchemy.orm.Query
    """
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


def find_all(session=None):
    """Alias for find() without arguments.

    Parameters
    ----------
    session : sqlalchemy.orm.Session or None
        Session object to conduct query with. Optional. Defaults to None.
    
    Returns
    -------
    sqlalchemy.orm.Query
    """
    return find(session=session)


def find_ark(ark, session=None):
    """Find record by ARK.

    Parameters
    ----------
    session : sqlalchemy.orm.Session or None
        Session object to conduct query with. Optional. Defaults to None.
    
    Returns
    -------
    sqlalchemy.orm.Query
    """
    filter_col = Ark.ark
    return find(filter_col, ark, session)


def find_replaceable(session=None):
    """Find replaceable ARK records.

    Find rows in the arks table where the replaceable column is True.

    Parameters
    ----------
    session : sqlalchemy.orm.Session or None
        Session object to conduct query with. Optional. Defaults to None.
    
    Returns
    -------
    sqlalchemy.orm.Query
    """
    filter_col = Ark.replaceable
    filter = True
    return find(filter_col, filter)


def find_url(url, session=None):
    """Find record by URL in target column.

    Parameters
    ----------
    session : sqlalchemy.orm.Session or None
        Session object to conduct query with. Optional. Defaults to None.
    
    Returns
    -------
    sqlalchemy.orm.Query
    """
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


def load_anvl_file_into_db(batch_file, engine=engine, verbose=True):
    """Load an ANVL file into database.

    Load an ANVL file that has only fields coresponding the the schema of
    arkimedes.db.Ark into the database. Useful for populating the database
    from ``akimedes.ezid.batch_download`` or restoring from a back-up file.
    Use only for a fresh database as this function does not check to see
    if an record already exists.

    Parameters
    ----------
    batch_file : str or pathlib.Path
        File to load ARK records from.
    engine : sqlalchemy.engine
        Engine to use if the database needs to be created. Defaults to
        ``arkimedes.db.engine.``
    verbose : bool
        If True print the number of the item being added to the database. 

    Returns
    -------
    None
    """
    if not db_exists():
        create_db(engine)

    arks = load_anvl_as_dict(batch_file)
    session = Session()

    for i, a in enumerate(arks):
        if verbose:
            sys.stdout.write(f"\r{i}")
            sys.stdout.flush()

        ark_obj = Ark(
            ark=a.get("ark", ""),
            target=a.get("_target", ""),
            profile=a.get("_profile", ""),
            status=a.get("_status", ""),
            owner=a.get("_owner", ""),
            ownergroup=a.get("_ownergroup", ""),
            created=int(a.get("_created", 0)),
            updated=int(a.get("_updated", 0)),
            export=False if a.get("_export") == "no" else True,
            dc_creator=a.get("dc.creator", ""),
            dc_title=a.get("dc.title", ""),
            dc_type=a.get("dc.type", ""),
            dc_date=a.get("dc.date", ""),
            dc_publisher=a.get("dc.publisher", ""),
            erc_when=a.get("erc.when", ""),
            erc_what=a.get("erc.what", ""),
            erc_who=a.get("erc.who", ""),
            replaceable=True
            if a.get("iastate.replaceable") == "True"
            or input_is_replaceable(a["dc.title"])
            else False,
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
    """Update existing table row.

    Update the row found by ARK with the values in ark_dict.

    Parameters
    ----------
    ark : str
        ARK of record to edit.
    ark_dict : dict
        Keys map to the fields to update. Values are the new values.

    Returns
    -------
    None

    """
    session = Session()
    record = find_ark(ark, session).first()

    for key, value in ark_dict.items():
        key = key.replace("iastate.", "").replace("_", "").replace(".", "_")
        setattr(record, key, value)

    session.commit()
    session.close()


def url_is_in_db(url):
    """Check if a URL is in the arks table.

    Parameters
    ----------
    url : str
        URL to search for.
    
    Returns
    -------
    bool
    """
    return bool(find_url(url).first())
