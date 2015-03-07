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
        c = sa.Column
        t = sa.Table
        # Types used in tables for easier tweaking
        price = st.Numeric(8, 2)
        email = st.String(254)
        date = st.DateTime(timezone=True)
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
                c('day', day, nullable=False, default=0),
                c('live', live, nullable=False, default=0),
                c('trade_id', trade_id, nullable=False, primary_key=True,
                  autoincrement=True),
                c('buyer_firm', firm),
                c('seller_firm', firm)
            ),
            'comms': t(
                'comms', metadata,
                c('time', date, nullable=False),
                c('sender', email, nullable=False),
                c('comm_id', comm_id, nullable=False, primary_key=True,
                  autoincrement=True),
                c('day', day, nullable=False, default=0),
                c('live', live, nullable=False, default=0)
            ),
            'recipients': t(
                'recipients', metadata,
                c('comm_id', comm_id, nullable=False),
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
        # Create tables
        metadata.create_all(self.engine)
        # Create views
        session = self.Session()
        try:
            session.execute("CREATE OR REPLACE VIEW live_trades AS SELECT *\
                            FROM trades WHERE live < 10")
            session.execute("CREATE OR REPLACE VIEW past_trades AS SELECT *\
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
        self.Session.remove()
        # Set times for workers
        now = datetime.now()
        self._timer_minute = now + timedelta(minutes=1)
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if now < day:
            self._timer_day = day.replace(minute=10)
        else:
            self._timer_day = day + timedelta(days=1, minutes=10)
        self._worker_minute = threading.Thread(target=self.worker_minute)
        self._worker_minute.start()
        self._worker_day = threading.Thread(target=self.worker_day)
        self._worker_day.start()
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
                } for d in data if len(d) == 10])  # much slower, but less lost
            except ValueError:
                self.logger.warn('[SqlStorage] Trade ValueError: %s', pf(data))
                traceback.print_exc()
            except IndexError:
                self.logger.warn('[SqlStorage] Trade IndexError: %s', pf(data))
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
            try:
                for d in data:
                    if len(d) == 3:
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
            except ValueError:
                self.logger.warn('[SqlStorage] Comm ValueError: %s', pf(data))
                traceback.print_exc()
            except IndexError:
                self.logger.warn('[SqlStorage] Comm IndexError: %s', pf(data))
                traceback.print_exc()
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
        session = self.Session()
        comms = self.tables['comms']
        trades = self.tables['trades']
        while not self._terminate.isSet():
            time.sleep(5)
            # Increase live age
            try:
                c_up = comms.update().values(live=comms.c.live + 1).where(
                    comms.c.live < 10)
                t_up = trades.update().values(live=trades.c.live + 1).where(
                    trades.c.live < 10)
                c_up.execute()
                t_up.execute()
                session.commit()
            except:
                session.rollback()
                traceback.print_exc()
            # Pre-algorithm updates
            try:
                queries = []
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

    def worker_day(self):
        pass

    def unload(self):
        super(SqlStorage, self).unload()
        self._worker_minute.join()
        self._worker_day.join()
        self.logger.info("[SqlStorage] unload")
