from system2.plugins import Plugin, StoragePlugin, QueryPlugin
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
import sqlalchemy.types as st
import datetime
import time


class SqlStorage(StoragePlugin, QueryPlugin, Plugin):
    """ SQL Alchemy thing """

    _name = 'SQLStore'

    def __init__(self, db='mysql+mysqldb://CS261:password@127.0.0.1/CS261'):
        super(SqlStorage, self).__init__()
        self.engine = sa.create_engine(
            db,
            echo=False,
            echo_pool=False,
            encoding='utf-8',
            pool_recycle=3600
        )
        self.engine.echo = False
        self.factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(self.factory)
        metadata = sa.MetaData(self.engine)
        c = sa.Column
        t = sa.Table
        # Types used in tables for easier tweaking
        price = st.Numeric(8, 2)
        email = st.String(254)
        date = st.DateTime(timezone=True)
        symbol = st.CHAR(7)
        sector = st.String(255)
        volume = st.BigInteger()
        day = st.SmallInteger()
        firm = st.String(30)
        currency = st.Enum(('GBX'))
        percent = st.Numeric(3, 2)
        spread = st.Numeric(3, 2)
        perf = st.Numeric(3, 2)
        num_dev = st.Numberic(2, 2)
        trade_id = st.String(12)
        group_id = st.BigInteger()
        # All the tables
        self.tables = {
            'live_trade': t(
                'live_trade', metadata,
                c('time', date, nullable=False),
                c('buyer', email, nullable=False),
                c('seller', email, nullable=False),
                c('price', price, nullable=False),
                c('size', volume, nullable=False),
                c('currency', currency, nullable=False),
                c('symbol', symbol, nullable=False),
                c('sector', sector, nullable=False),
                c('bid', price, nullable=False),
                c('ask', price, nullable=False),
                c('day', day, nullable=False),
                c('trade_id', trade_id, nullable=False, primary_key=True),
                c('buyer_firm', firm),
                c('seller_firm', firm)
            ),
            'live_comms': t(
                'live_comms', metadata,
                c('time', date, nullable=False),
                c('sender', email, nullable=False),
                c('group', group_id, nullable=False),
                c('day', day, nullable=False)
            ),
            'past_trade': t(
                'past_trade', metadata,
                c('time', date, nullable=False),
                c('buyer', email, nullable=False),
                c('seller', email, nullable=False),
                c('price', price, nullable=False),
                c('size', volume, nullable=False),
                c('currency', currency, nullable=False),
                c('symbol', symbol, nullable=False),
                c('sector', sector, nullable=False),
                c('bid', price, nullable=False),
                c('ask', price, nullable=False),
                c('day', day, nullable=False),
                c('trade_id', trade_id, nullable=False, primary_key=True),
                c('buyer_firm', firm),
                c('seller_firm', firm)
            ),
            'past_comms': t(
                'past_comms', metadata,
                c('time', date, nullable=False),
                c('sender', email, nullable=False),
                c('group', group_id, nullable=False),
                c('day', day, nullable=False)
            ),
            'recipients': t(
                'recipients', metadata,
                c('group', group_id, nullable=False),
                c('recipient', email, nullable=False)
            ),
            'averages_trade': t(
                'averages_trade', metadata,
                c('symbol', symbol, nullable=False, primary_key=True),
                c('sector', sector, nullable=False),
                c('one_day_close', price, nullable=False),
                c('five_day_close', price, nullable=False),
                c('twenty_day_close', price, nullable=False),
                c('one_day_price_mean', price, nullable=False),
                c('one_day_price_sd', price, nullable=False),
                c('five_day_price_mean', price, nullable=False),
                c('five_day_price_sd', price, nullable=False),
                c('twenty_day_price_mean', price, nullable=False),
                c('twenty_day_price_sd', price, nullable=False),
                c('one_day_volume_mean', price, nullable=False),
                c('one_day_volume_sd', price, nullable=False),
                c('five_day_volume_mean', price, nullable=False),
                c('five_day_volume_sd', price, nullable=False),
                c('twenty_day_volume_mean', price, nullable=False),
                c('twenty_day_volume_sd', price, nullable=False),
                c('one_day_spread_mean', spread, nullable=False),
                c('one_day_spread_sd', spread, nullable=False),
                c('five_day_spread_mean', spread, nullable=False),
                c('five_day_spread_sd', spread, nullable=False),
                c('twenty_day_spread_mean', spread, nullable=False),
                c('twenty_day_spread_sd', spread, nullable=False),
                c('one_day_stock_perf', perf, nullable=False),
                c('one_day_sector_perf', perf, nullable=False),
                c('five_day_stock_perf', perf, nullable=False),
                c('five_day_sector_perf', perf, nullable=False),
                c('twenty_day_stock_perf', perf, nullable=False),
                c('twenty_day_sector_perf', perf, nullable=False),
                c('one_day_price_input', num_dev, nullable=False),
                c('five_day_price_input', num_dev, nullable=False),
                c('twenty_day_price_input', num_dev, nullable=False),
                c('one_day_spread_input', num_dev, nullable=False),
                c('five_day_spread_input', num_dev, nullable=False),
                c('twenty_day_spread_input', num_dev, nullable=False),
                c('one_day_volume_input', num_dev, nullable=False),
                c('five_day_volume_input', num_dev, nullable=False),
                c('twenty_day_volume_input', num_dev, nullable=False),
                c('one_day_perf_input', percent, nullable=False),
                c('five_day_perf_input', percent, nullable=False),
                c('twenty_day_perf_input', percent, nullable=False),
                c('holdings_input', percent, nullable=False),
            ),
            'portfolio_holdings': t(
                'portfolio_holdings', metadata,
                c('firm', firm, nullable=False),
                c('symbol', symbol, nullable=False),
                c('holdings', volume, nullable=False),
                c('holdings_perc', percent, nullable=False)
            ),
            'suspicious_trade': t(
                'suspicious_trade', metadata,
                c('off_market_trans_trade_id', trade_id, nullable=False),
                c('wash_trade_trade_id', trade_id, nullable=False),
                c('one_day_price_trade_id', trade_id, nullable=False),
                c('five_day_price_trade_id', trade_id, nullable=False),
                c('twenty_day_price_trade_id', trade_id, nullable=False),
                c('one_day_spread_trade_id', trade_id, nullable=False),
                c('five_day_spread_trade_id', trade_id, nullable=False),
                c('twenty_day_spread_trade_id', trade_id, nullable=False),
                c('one_day_volume_trade_id', trade_id, nullable=False),
                c('five_day_volume_trade_id', trade_id, nullable=False),
                c('twenty_day_volume_trade_id', trade_id, nullable=False),
                c('one_day_perf_trade_id', trade_id, nullable=False),
                c('five_day_perf_trade_id', trade_id, nullable=False),
                c('twenty_day_perf_trade_id', trade_id, nullable=False),
                c('portfolio_firm', firm, nullable=False),
                c('portfolio_symbol', symbol, nullable=False),
                c('trader_activity_trade_id', trade_id, nullable=False),
            )
        }

        for table in self.tables:
            if not self.engine.dialect.has_table(self.engine.connect(), table):
                self.tables[table].create(checkfirst=True)

        self.status = Plugin.STATUS_INIT
        self.logger.info('[SqlStorage] init')

    def worker(self):
        while not hasattr(self, "Session"):
            time.sleep(0.1)
        session = self.Session()
        # while true # - should we let it clean up?
        while not self._terminate.isSet():
            data = self._q.get()
            if data is None:  # flush blocked threads
                self._q.task_done()
                break
            self.burst_store(data, session)
            self._q.task_done()
        self.Session.remove()

    def store_trades(self, data):
        """ Stores trades """

    def store_comms(self, data):
        """ Stores comms """
        time = datetime.strptime(data[0], '%Y-%m-%d %H:%M:%S.%f')
        sender = data[1]
        sender_firm = data[1].rsplit('@', 1)[0]
        recipients = [(r, r.rsplit('@', 1)[0]) for r in data[2].split(';')]
        # SELECT MAX(Id) FROM Customers
        #

    def store(self, data):
        """ Performs queries to insert data """
        if len(data) == 10:
            self.store_tradse(data)
        elif len(data) == 3:
            self.store_comms(data)

    def unload(self):
        super(SqlStorage, self).unload()
        self.logger.info("[SqlStorage] unload")
