import os
import logging
from datetime import datetime
from enum import Enum

from sqlalchemy import create_engine, ForeignKey, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.exc import OperationalError

# custom logging level
SUCCESS_JOB = 101
SUCCESS_IP_OPERATION = 102
FAILED_IP_OPERATION = 103
REJECTED_IP_OPERATION = 104

Base = declarative_base()


class DbConnectionError(Exception):
    """DB connection error.
    """
    pass


class DbIpOperationStatus(Enum):
    """IP operation status

    * failed (-3)
    * rejected (-2)
    * deleted (-1)
    * unchanged (0)
    * added (1)
    * updated (2)
    """
    failed = -3
    rejected = -2
    deleted = -1
    unchanged = 0
    added = 1
    updated = 2
    forced = 3


class DbJobRecord(Base):
    """Table definition: jobs
    """
    __tablename__ = 'jobs'
    __table_args__ = {'sqlite_autoincrement': True}

    id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        autoincrement=True
    )
    tuc = Column(
        String(255),
        nullable=False,
        index=True
    )
    start = Column(
        DateTime(),
        nullable=False
    )
    end = Column(
        DateTime(),
        nullable=True
    )
    success = Column(
        Boolean,
        nullable=True,
        index=True
    )
    reason = Column(
        String,
        nullable=True
    )
    pid = Column(
        Integer,
        nullable=False,
    )


class DbIpOperationRecord(Base):
    """Table definition: ip_operations
    """
    __tablename__ = 'ip_operations'

    id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        autoincrement=True
    )
    job_id = Column(
        ForeignKey(DbJobRecord.id),
        nullable=False,
        index=True
    )
    processor = Column(
        String(255),
        nullable=False,
        index=True
    )
    platform_type = Column(
        Integer,
        nullable=False,
        index=True
    )
    ip = Column(
        String(255),
        nullable=False,
    )
    modified = Column(
        DateTime(),
        nullable=False,
    )
    status = Column(
        Integer,
        nullable=False,
        index=True
    )


class DbLpOperationRecord(Base):
    """Table definition: lp_operations
    """
    __tablename__ = 'lp_operations'

    id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        autoincrement=True
    )
    job_id = Column(
        ForeignKey(DbJobRecord.id),
        nullable=False,
        index=True
    )
    processor = Column(
        String(255),
        nullable=False,
        index=True
    )
    modified = Column(
        DateTime(),
        nullable=False,
    )
    status = Column(
        Integer,
        nullable=False,
        index=True
    )


class DbLogger(logging.Handler):
    """A custom handler for DB logging (IF-MNG-DB).

    :param str tuc_name: TUC identifier
    """
    def __init__(self, tuc_name='?'):
        super(DbLogger, self).__init__()

        self._session = None
        self._session_maker = None

        self._tuc_name = tuc_name

        self._ip_operation = {}

        # current / last job id
        self._job_id = None
        self._last_job = {
            'id' : None,
            'start': None,
            'end' : None
        }

        # process start (DbLogger is called in a runner constructor)
        self._start_time = datetime.now()

    def __del__(self):
        """Destructor.
        """
        self._close_all()

    def _close_all(self):
        """Close all sessions.
        """
        if self._session:
            self._session.close()
        if self._session_maker:
            self._session_maker.close_all()
        self._session = self._session_maker = None

    def set_session(self, dbname):
        """Create a new session.

        :param str dbname: database name
        """
        # create session if not already defined
        if not self._session:
            if self._session_maker:
                self._session_maker.close_all()
            engine = create_engine(
                'sqlite:///{}'.format(dbname)
                # , echo=True # display also SQL statements
            )
            Base.metadata.bind = engine
            self._session_maker = sessionmaker(engine)
            self._session = self._session_maker()

        # create tables
        try:
            Base.metadata.create_all(Base.metadata.bind)
        except OperationalError as e:
            self._close_all()
            raise DbConnectionError('{}'.format(e))

    def set_ip_operation(self, identifier, ip, status, timestamp, platform_type):
        """Set IP operation.

        :param str identifier: processor identifier
        :param str ip: image product
        :param DbIpOperationStatus status: status
        :param datetime typestamp: timestamp
        :param int platform_type: platform type identifier
        """
        self._ip_operation = {
            'identifier' : identifier,
            'ip' : ip,
            'status' : status,
            'timestamp': timestamp,
            'platform_type': platform_type
        }

    def emit(self, record):
        """Format the record and store in DB log tables.

        Overrides the logging.Handler.emit function.

        :param record: record to emit
        """
        if not self._session_maker or not self._session:
            return

        timestamp = datetime.now()
        message = record.getMessage()
        if record.levelno in (logging.CRITICAL, SUCCESS_JOB):
            # job finished
            self._session.query(DbJobRecord.id).\
                filter(DbJobRecord.id == self._job_id).\
                update({DbJobRecord.end: timestamp,
                        DbJobRecord.success: True if record.levelno == SUCCESS_JOB else False,
                        DbJobRecord.reason: message})

            self._session.commit()

        if record.levelno in (SUCCESS_IP_OPERATION, FAILED_IP_OPERATION, REJECTED_IP_OPERATION):
            # ip_operation finished
            db_ip_operation = DbIpOperationRecord(
                processor=self._ip_operation['identifier'],
                ip=self._ip_operation['ip'],
                modified=timestamp if not self._ip_operation['timestamp'] else self._ip_operation['timestamp'],
                status=self._ip_operation.get('status'),
                job_id=self._job_id,
                platform_type=self._ip_operation.get('platform_type')
            )
            self._session.add(db_ip_operation)
            self._ip_operation = dict()

        self._session.commit()

    def job_id(self):
        """Get current job id.

        :return int: id
        """
        if not self._job_id:
            qry = self._session.query(
                func.max(DbJobRecord.id).label("max")
            )
            res = qry.one()
            self._job_id = res.max + 1 if res.max else 1

        return self._job_id

    def _last_job_id(self, processor=None):
        """Get last job id.

        :param str processor: filter by processor or None

        :return int: job id
        """
        query = self._session.query(DbJobRecord.id, DbJobRecord.start, DbJobRecord.end). \
            filter(DbJobRecord.tuc == self._tuc_name). \
            filter(DbJobRecord.success == True)
        if processor:
            query = query.join(DbIpOperationRecord). \
                filter(DbIpOperationRecord.processor == processor)

        return query.order_by(DbJobRecord.start.desc()).first()

    def last_job_id(self, processor=None):
        """Get last job id.

        :param str processor: filter by processor or None

        :return int: job id
        """
        if not self._last_job['id']:
            last_job = self._last_job_id(processor)
            if last_job:
                self._last_job['id'] = last_job[0]
                self._last_job['start'] = last_job[1]
                self._last_job['end'] = last_job[2]

        return self._last_job['id']

    def processed_ips_last(self, processor):
        """Get processed image products for defined processor from last found
        job.

        If last job is not found than current job is used.
        
        :param str processor: processor identifier
        
        :return list: list of tuples (ip, platform_type, status)

        """
        result = []
        try:
            last_job = self._last_job_id(processor)[0] # id
        except TypeError:
            # no previous job found (tests?, try current)
            last_job = self._job_id

        query = self._session.query(
            DbIpOperationRecord.ip,
            DbIpOperationRecord.platform_type,
            DbIpOperationRecord.status). \
            filter(DbIpOperationRecord.processor == processor). \
            filter(DbIpOperationRecord.job_id == last_job)

        for rec in query.all():
            result.append((rec[0], rec[1], DbIpOperationStatus(rec[2])))

        return result
        
    def processed_ips(self, processor, prev=False, platform_type=None):
        """Get processed image products for defined processor.

        :param str processor: processor identifier
        :param bool prev: True for previous job otherwise current
        :param int platform_type: platform type filter (see processors.QCPlatformType)

        :return list: list of tuples (ip, status)
        """
        result = []
        if prev:
            # limit to previous job
            job_id = self._last_job['id']
            if not job_id:
                return result
        else:
            # limit to current job
            job_id = self._job_id

        query = self._session.query(
            DbIpOperationRecord.ip,
            DbIpOperationRecord.status). \
            filter(DbIpOperationRecord.processor == processor). \
            filter(DbIpOperationRecord.job_id == job_id)

        if platform_type is not None:
            query = query.filter(
                DbIpOperationRecord.platform_type == platform_type.value
            )

        for rec in query.all():
            result.append((rec[0], DbIpOperationStatus(rec[1])))

        return result

    def processed_ip_status(self, processor, ip):
        """Get processed image product status of defined processor from
        previous job.

        :param str processor: processor identifier
        :param str ip: image product

        :return DbIpOperationStatus: status or None (on failure)
        """
        status_id = self._session.query(DbIpOperationRecord.status). \
            filter(DbIpOperationRecord.job_id == self._last_job['id']). \
            filter(DbIpOperationRecord.processor == processor). \
            filter(DbIpOperationRecord.ip == ip).first()

        if status_id:
            return DbIpOperationStatus(status_id[0])

        return None

    def job_started(self):
        """Indicates that job is running.
        """
        dbjob = DbJobRecord(
            tuc=self._tuc_name,
            start=self._start_time,
            pid=os.getpid()
        )
        self._session.add(dbjob)

    def delete_job(self, job_id):
        """Delete job records.

        :param int job_id: job id to be deleted
        """
        # delete from jobs
        self._session.query(DbJobRecord.id).\
            filter(DbJobRecord.id == job_id).delete()
        # delete from ip_operations
        self._session.query(DbIpOperationRecord.job_id).\
            filter(DbIpOperationRecord.job_id == job_id).delete()

        self._session.commit()
