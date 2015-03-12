from system2.plugins import Plugin, StoragePlugin, QueryPlugin
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
import sqlalchemy.types as st
from datetime import datetime, timedelta
from pprint import pformat as pf
import time
import threading
import traceback


def str2dt(str_):
    return datetime.strptime(str_, '%Y-%m-%d %H:%M:%S.%f')


class SqlStorage(StoragePlugin, QueryPlugin, Plugin):
    """ SQL Alchemy thing """

    _name = 'SQLStore'

    def __init__(self, db='mysql+mysqldb://CS261:password@127.0.0.1/CS261'):
        super(SqlStorage, self).__init__()
        # Engine and session factory
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

        def c(*args, **kwargs):
            return sa.Column(*args, nullable=False, **kwargs)
        t = sa.Table
        # Types used in tables for easier tweaking
        price = st.Numeric(8, 2)
        email = st.String(254)
        date = st.DateTime(timezone=True)  # loses microseconds because mysql
        symbol = st.CHAR(7)
        sector = st.String(255)
        volume = st.BigInteger()
        day = st.Integer()
        live = st.SmallInteger()
        firm = st.String(30)
        currency = st.Enum(('GBX'))
        percent = st.Numeric(3, 2)
        spread = st.Numeric(3, 2)
        perf = st.Numeric(3, 2)
        num_dev = st.Numeric(2, 2)
        trade_id = st.BigInteger()
        comm_id = st.BigInteger()
        # All the tables
        self.tables = {
            'trades': t(
                'trades', metadata,
                c('time', date),
                c('buyer', email),
                c('seller', email),
                c('price', price),
                c('size', volume),
                c('currency', currency),
                c('symbol', symbol),
                c('sector', sector),
                c('bid', price),
                c('ask', price),
                c('day', day, default=0),
                c('live', live, default=0),
                c('trade_id', trade_id, primary_key=True, autoincrement=True),
                c('buyer_firm', firm),
                c('seller_firm', firm)
            ),
            'comms': t(
                'comms', metadata,
                c('time', date),
                c('sender', email),
                c('comm_id', comm_id, primary_key=True, autoincrement=True),
                c('day', day, default=0),
                c('live', live, default=0)
            ),
            'recipients': t(
                'recipients', metadata,
                c('comm_id', comm_id),
                c('recipient', email)
            ),
            'averages_trade': t(
                'averages_trade', metadata,
                c('symbol', symbol, primary_key=True),
                c('sector', sector),
                c('one_day_close', price),
                c('five_day_close', price),
                c('twenty_day_close', price),
                c('one_day_price_mean', price),
                c('one_day_price_sd', price),
                c('five_day_price_mean', price),
                c('five_day_price_sd', price),
                c('twenty_day_price_mean', price),
                c('twenty_day_price_sd', price),
                c('one_day_volume_mean', price),
                c('one_day_volume_sd', price),
                c('five_day_volume_mean', price),
                c('five_day_volume_sd', price),
                c('twenty_day_volume_mean', price),
                c('twenty_day_volume_sd', price),
                c('one_day_spread_mean', spread),
                c('one_day_spread_sd', spread),
                c('five_day_spread_mean', spread),
                c('five_day_spread_sd', spread),
                c('twenty_day_spread_mean', spread),
                c('twenty_day_spread_sd', spread),
                c('one_day_stock_perf', perf),
                c('one_day_sector_perf', perf),
                c('five_day_stock_perf', perf),
                c('five_day_sector_perf', perf),
                c('twenty_day_stock_perf', perf),
                c('twenty_day_sector_perf', perf),
                c('one_day_price_input', num_dev),
                c('five_day_price_input', num_dev),
                c('twenty_day_price_input', num_dev),
                c('one_day_spread_input', num_dev),
                c('five_day_spread_input', num_dev),
                c('twenty_day_spread_input', num_dev),
                c('one_day_volume_input', num_dev),
                c('five_day_volume_input', num_dev),
                c('twenty_day_volume_input', num_dev),
                c('one_day_perf_input', percent),
                c('five_day_perf_input', percent),
                c('twenty_day_perf_input', percent),
                c('holdings_input', percent),
            ),
            'portfolio_holdings': t(
                'portfolio_holdings', metadata,
                c('firm', firm),
                c('symbol', symbol),
                c('holdings', volume),
                c('holdings_perc', percent)
            ),
            'suspicious_trade': t(
                'suspicious_trade', metadata,
                c('off_market_trans_trade_id', trade_id),
                c('wash_trade_trade_id', trade_id),
                c('one_day_price_trade_id', trade_id),
                c('five_day_price_trade_id', trade_id),
                c('twenty_day_price_trade_id', trade_id),
                c('one_day_spread_trade_id', trade_id),
                c('five_day_spread_trade_id', trade_id),
                c('twenty_day_spread_trade_id', trade_id),
                c('one_day_volume_trade_id', trade_id),
                c('five_day_volume_trade_id', trade_id),
                c('twenty_day_volume_trade_id', trade_id),
                c('one_day_perf_trade_id', trade_id),
                c('five_day_perf_trade_id', trade_id),
                c('twenty_day_perf_trade_id', trade_id),
                c('portfolio_firm', firm),
                c('portfolio_symbol', symbol),
                c('trader_activity_trade_id', trade_id),
            )
        }
        # Create tables
        metadata.create_all(self.engine)
        # Create views - format this with timer variable
        session = self.Session()
        try:
            session.execute("CREATE OR REPLACE VIEW live_tradess AS SELECT *\
                            FROM trades WHERE live < 10")
            session.execute("CREATE OR REPLACE VIEW past_tradess AS SELECT *\
                            FROM trades WHERE live >= 10 AND day < 20")
            session.execute("CREATE OR REPLACE VIEW live_comms AS SELECT *\
                            FROM comms WHERE live < 10")
            session.execute("CREATE OR REPLACE VIEW past_comms AS SELECT *\
                            FROM comms WHERE live >= 10 AND day < 20")
            session.commit()
        except:
            session.rollback()
            raise
        self.tables['live_trades'] = t('live_trades', metadata, autoload=True)
        self.tables['past_trades'] = t('past_trades', metadata, autoload=True)
        self.tables['live_comms'] = t('live_comms', metadata, autoload=True)
        self.tables['past_comms'] = t('past_comms', metadata, autoload=True)
        # Interval-based workers
        now = datetime.now()
        day = now.replace(hour=0, minute=10, second=0, microsecond=0)
        # "Minute" worker
        self._min_interval_n = 1
        self._min_interval_m = 10
        self._min_interval = timedelta(minutes=self._min_interval_n)
        self._min_time = now + self._min_interval
        timeout = (self._min_time - now).total_seconds()
        self._min_timer = threading.Timer(timeout, self.worker_minute)
        self._min_timer.start()
        # "Day" worker
        self._day_interval_n = 1
        self._day_interval_m = 20
        self._day_interval = timedelta(days=self._day_interval_n)
        self._day_time = day + self._day_interval
        timeout = (self._day_time - now).total_seconds()
        self._day_timer = threading.Timer(timeout, self.worker_day)
        self._day_timer.start()
        # expose session for QueryPlugin
        self._session = session
        # Done with init
        self.status = Plugin.STATUS_INIT
        self.logger.info('[SqlStorage] init')

    def store_trades(self, data, session):
        """ Stores trades """
        try:
            ins = self.tables['trades'].insert()
            try:
                ins.execute(*[{
                    'time': str2dt(d[0]),
                    'buyer': d[1],
                    'seller': d[2],
                    'price': d[3],
                    'size': d[4],
                    'currency': d[5],
                    'symbol': d[6],
                    'sector': d[7],
                    'bid': d[8],
                    'ask': d[9],
                    'buyer_firm': d[1].rsplit('@', 1)[1],
                    'seller_firm': d[2].rsplit('@', 1)[1]
                } for d in data])
                # } for d in data if len(d) == 10])
            except ValueError:
                self.logger.warn('[SqlStorage] Trade ValueError: %s', pf(data))
                traceback.print_exc()
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()

    def store_comms(self, data, session):
        """ Stores comms """
        try:
            g_ins = self.tables['recipients'].insert()
            c_ins = self.tables['comms'].insert()
            for d in data:
                try:
                    res = c_ins.execute({
                        'time': str2dt(d[0]),
                        'sender': d[1],
                    })
                except ValueError:
                    pass
                else:
                    id_ = res.inserted_primary_key
                    g_ins.execute(*[
                        {'comm_id': id_, 'recipient': r}
                        for r in d[2].split(';')
                    ])
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()

    def burst_store(self, data, session):
        """ Stores multiple entry data into storage """
        if len(data) > 0:
            if len(data[0]) == 3:
                self.store_comms(data, session)
            elif len(data[0]) == 10:
                self.store_trades(data, session)

    def worker(self):
        while not hasattr(self, "Session"):
            time.sleep(0.1)
        session = self.Session()
        while not self._terminate.isSet():
            data = self._q.get()
            if data is None:
                self._q.task_done()
                break
            self.burst_store(data, session)
            self._q.task_done()
        self.Session.remove()

    def worker_minute(self):
        self.logger.info('[SqlStorage] Running worker_minute')
        # Schedule next run
        self._min_time = self._min_time + self._min_interval
        timeout = (self._min_time - datetime.now()).total_seconds()
        self._min_timer = threading.Timer(timeout, self.worker_minute)
        self._min_timer.start()
        # Aliases for ease of use
        session = self.Session()
        comms = self.tables['comms']
        live_comms = self.tables['live_comms']
        past_comms = self.tables['past_comms']
        trades = self.tables['trades']
        live_tradess = self.tables['live_tradess']
        past_tradess = self.tables['past_tradess']
        # Increase live age
        try:
            c_up = comms.update().\
                values(live=comms.c.live + self._min_interval_n).\
                where(comms.c.live < self._min_interval_m)
            t_up = trades.update().\
                values(live=trades.c.live + self._min_interval_n).\
                where(trades.c.live < self._min_interval_m)
            c_up.execute()
            t_up.execute()
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()
        # Pre-algorithm updates
        try:
            queries = [
                # Preperations

                # Algo

            ]

            for q in queries:
                pass
                #session.execute(q)
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()
        # Perform algorithms and store ids
        try:
            queries = []
            for q in queries:
                session.execute(q)
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()
        self.Session.remove()
        self.logger.info('[SqlStorage] Finished worker_minute')

    def worker_day(self):
        self.logger.info('[SqlStorage] Running worker_day')
        # Schedule next run
        self._day_time = self._day_time + self._day_interval
        timeout = (self._day_time - datetime.now()).total_seconds()
        self._day_timer = threading.Timer(timeout, self.worker_day)
        self._day_timer.start()
        # Aliases for ease of use
        session = self.Session()
        comms = self.tables['comms']
        live_comms = self.tables['live_comms']
        past_comms = self.tables['past_comms']
        trades = self.tables['trades']
        live_tradess = self.tables['live_tradess']
        past_tradess = self.tables['past_tradess']
        # Increase day age
        try:
            c_up = comms.update().\
                values(day=comms.c.day + self._day_interval_n).\
                where(comms.c.day < self._day_interval_m)
            t_up = trades.update().\
                values(day=trades.c.day + self._day_interval_n).\
                where(trades.c.day < self._day_interval_m)
            c_up.execute()
            t_up.execute()
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()
        # Pre-algorithm updates
        try:
            queries = [

            ]
            for q in queries:
                session.execute(q)
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()
        # Perform algorithms and store ids
        try:
            queries = []
            for q in queries:
                session.execute(q)
            session.commit()
        except:
            session.rollback()
            traceback.print_exc()
        self.Session.remove()
        self.logger.info('[SqlStorage] Finished worker_day')

    def unload(self):
        super(SqlStorage, self).unload()
        self._min_timer.cancel()
        self._day_timer.cancel()
        self._min_timer.join()
        self._day_timer.join()
        self.Session.remove()
        self.logger.info("[SqlStorage] unload")

    # QueryPlugin

    def trades(self, query, number):
        try:
            trades = self.tables['trades']
            query = self._session.query(trades).order_by(trades.c.time.desc())\
                        .limit(number)
            self._session.commit()
            return [list(r) for r in self._session.execute(query)]
        except:
            self._session.rollback()
            raise

    def comms(self, query, number):
        try:
            comms = self.tables['comms']
            query = self._session.query(comms).order_by(comms.c.time.desc())\
                        .limit(number)
            self._session.commit()
            return [list(r) for r in self._session.execute(query)]
        except:
            self._session.rollback()
            raise

    def alerts(self, query, number):
        try:
            alerts = self.tables['suspicious_trade']
            query = self._session.query(alerts).order_by(alerts.c.time.desc())\
                        .limit(number)
            self._session.commit()
            return [list(r) for r in self._session.execute(query)]
        except:
            self._session.rollback()
            raise

    def stock(self, symbol):
        pass

    def trader(self, symbol):
        pass

    def alert(self, alertid):
        pass

    def plost_data(self, column1, column2):
        pass
