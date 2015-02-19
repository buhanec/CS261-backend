from system2.plugins import Plugin, StoragePlugin
import sqlalchemy as sa
import sqlalchemy.types as st
import datetime


class SqlStorage(StoragePlugin, Plugin):
    """ SQL Alchemy thing """

    _name = 'SQL Storage'

    def __init__(self, db='mysql+mysqldb://CS261:password@127.0.0.1/CS261'):
        super(SqlStorage, self).__init__()

        self.engine = sa.create_engine(
            db,
            echo=False,
            echo_pool=False,
            encoding='utf-8',
            pool_recycle=3600,
        )
        self.engine.echo = False
        self.factory = sa.scoped_session(sa.sessionmaker(bind=self.engine))
        metadata = sa.MetaData(self.engine)
        tables = {
            'traders': sa.Table(
                'traders', metadata,
                sa.Column('trader', st.String(254), nullable=False),
                sa.Column('activity', st.DateTime(timezone=True),
                          default=datetime.datetime.utcnow, nullable=False)
            ),
            'trades': sa.Table(
                'trades', metadata,
                sa.Column('time', st.DateTime(timezone=True),
                          default=datetime.datetime.utcnow, nullable=False),
                sa.Column('buyer', st.String(254), nullable=False),
                sa.Column('seller', st.String(254), nullable=False),
                sa.Column('price', st.Numeric(8, 2), nullable=False),
                sa.Column('size', st.BigInteger(), nullable=False),
                sa.Column('currency', st.Enum(('GBX')), nullable=False),
                sa.Column('symbol', st.CHAR(6), nullable=False),
                sa.Column('sector', st.String(255), nullable=False),
                sa.Column('bid', st.Numeric(8, 2), nullable=False),
                sa.Column('ask', st.Numeric(8, 2), nullable=False)
            ),
            'recipients': sa.Table(
                'recipients', metadata,
                sa.Column('group', st.BigInteger(), nullable=False),
                sa.Column('recipient', st.String(254), nullable=False)
            ),
            'comms': sa.Table(
                'comms', metadata,
                sa.Column('time', st.DateTime(timezone=True),
                          default=datetime.datetime.utcnow, nullable=False),
                sa.Column('sender', st.String(254), nullable=False),
                sa.Column('group', st.BigInteger(), nullable=False)
            )
        }

        for table in tables:
            if not self.engine.dialect.has_table(self.engine.connect(), table):
                tables[table].create(checkfirst=True)

        self.status = Plugin.STATUS_INIT
        self.logger.info('[SqlStorage] init')

    def worker(self):
        session = self.factory()
        while not self._terminate.isSet():
            data = self._q.get()
            if data is None:  # flush blocked threads
                self._q.task_done()
                break
            self.burst_store(data)
            self._q.task_done()
        session.remove()

    def burst_store(self, storage):
        """ Performs queries to insert data """
        # session.query(...)
        # session.add(...)
        # session.commit()

    def unload(self):
        super(SqlStorage, self).unload()
        self.engine.close()
        self.logger.info("[SqlStorage] unload")
