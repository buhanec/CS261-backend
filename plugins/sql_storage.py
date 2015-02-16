from system2.plugins import Plugin, StoragePlugin
import sqlalchemy as sa
import sqlalchemy.types as st
import datetime


class SqlStorage(Plugin, StoragePlugin):
    """ SQL Alchemy thing """

    _name = 'Network Capture'

    def __init__(self, db='mysql+mysqldb://CS261:password@127.0.0.1/CS261'):
        super(SqlStorage, self).__init__()

        engine = sa.create_engine(
            db,
            echo=False,
            echo_pool=False,
            encoding='utf-8',
            pool_recycle=3600,
        )
        engine.echo = False
        self.factory = sa.scoped_session(sa.sessionmaker(bind=engine))
        metadata = sa.MetaData(engine)
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
            if not engine.dialect.has_table(engine.connect(), table):
                tables[table].create(checkfirst=True)

        print('[SqlStorage] init')

    def store(self, storage):
        session = self.factory()
        # session.query(...)
        # session.add(...)
        # session.commit()
        session.remove()
