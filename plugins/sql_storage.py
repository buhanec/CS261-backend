from system2.plugins import Plugin, StoragePlugin, QueryPlugin
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
import sqlalchemy.types as st
from datetime import datetime, timedelta
import time
import threading
import traceback
from threading import Semaphore


def str2dt(str_):
    return datetime.strptime(str_, '%Y-%m-%d %H:%M:%S.%f')


class SqlStorage(StoragePlugin, QueryPlugin, Plugin):
    """ SQL Alchemy thing """

    _name = 'SQLStore'

    def __init__(self,
                 db='mysql+mysqldb://CS261:password@127.0.0.1/CS261',
                 unloader=None):
        super(SqlStorage, self).__init__(unloader=unloader)
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

        def n(*args, **kwargs):
            return sa.Column(*args, nullable=True, **kwargs)
        t = sa.Table
        # Types used in tables for easier tweaking
        price = st.Numeric(8, 2)
        email = st.String(254)
        date = st.DateTime(timezone=True)  # loses microseconds because mysql
        symbol = st.CHAR(6)
        sector = st.String(255)
        volume = st.BigInteger()
        day = st.Integer()
        live = st.SmallInteger()
        firm = st.String(30)
        currency = st.Enum('GBX')
        pkey = st.BigInteger()
        alert_type = st.Enum('Volume', 'Price')
        # All the tables
        self.tables = {
            'trades': t(
                'trades', metadata,
                c('id', pkey, primary_key=True, autoincrement=True),
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
                c('buyer_firm', firm),
                c('seller_firm', firm),
                c('day', day, default=0),
                c('live', live, default=0),
            ),
            'comms': t(
                'comms', metadata,
                c('id', pkey, primary_key=True, autoincrement=True),
                c('time', date),
                c('sender', email),
                c('day', day, default=0),
                c('live', live, default=0)
            ),
            'recipients': t(
                'recipients', metadata,
                c('comm_id', pkey),
                # c('comm_id', pkey, sa.ForeignKey('comms.id')),
                c('recipient', email)
            ),
            'stocks': t(
                'stocks', metadata,
                c('symbol', symbol, primary_key=True),
                c('avg_price', st.Float()),
                c('avg_size', st.Float()),
                c('std_price', st.Float(), default=0),
                c('std_size', st.Float(), default=0),
                c('count', volume, default=1)
            ),
            'alerts': t(
                'alerts', metadata,
                c('id', pkey, primary_key=True, autoincrement=True),
                c('trade_id', pkey),
                c('severity', st.Float()),
                c('alert_type', alert_type),
                n('comms', pkey),
                n('trades', pkey)
            ),
            'comm_clusters': t(
                'comm_clusters', metadata,
                c('id', pkey),
                c('trade_id', pkey)
            ),
            'trade_clusters': t(
                'trade_clusters', metadata,
                c('id', pkey),
                c('cluster_id', pkey)
            )
        }
        # Create tables
        metadata.create_all(self.engine)
        # Interval-based workers
        now = datetime.now()
        day = now.replace(hour=0, minute=10, second=0, microsecond=0)
        # How2threadsafe
        self._YOU_SHALL_NOT_PASS = Semaphore(1)
        # "Minute" worker
        self._min_count_sem = Semaphore(1)
        self._min_count_m = 100
        self._min_count = self._min_count_m
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
        self._session = self.Session()
        # Done with init
        self.status = Plugin.STATUS_INIT
        self.logger.info('[SqlStorage] init')

    def store_trades(self, data, session):
        """ Stores trades """
        self._YOU_SHALL_NOT_PASS.acquire()
        try:
            ins = self.tables['trades'].insert()
            for d in data:
                result = ins.execute({
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
                })
                tradeid = result.inserted_primary_key[0]
                fixthis = '\
SET @symbol = \''+d[6]+'\'; \
SET @price = '+d[3]+'; \
SET @size = '+d[4]+'; \
INSERT INTO `stocks` (`symbol`, `avg_price`, `avg_size`, `std_price`, `std_size`, `count`) \
VALUES (@symbol, @price, @size, 0, 0, 1) \
ON DUPLICATE KEY UPDATE \
`count` = `count` + 1, \
`avg_price` = (SELECT AVG(`price`) FROM `trades` \
               WHERE `symbol` = @symbol), \
`std_price` = (SELECT STD(`price`) FROM `trades` \
               WHERE `symbol` = @symbol), \
`avg_size` = (SELECT AVG(`size`) FROM `trades` \
              WHERE `symbol` = @symbol), \
`std_size` = (SELECT STD(`size`) FROM `trades` \
              WHERE `symbol` = @symbol);'
                session.execute(fixthis)
                session.commit()
                fixme = 'SELECT * FROM `stocks` WHERE `symbol` =  \''+d[6]+'\'';
                result = session.execute(fixme)
                session.commit()
                r = [r for r in result][0]
                price = float(d[3])
                size = float(d[4])
                if r[5] > 10:
                    dprice = abs(price - r[1])/r[3]
                    dvolume = abs(size - r[2])/r[4]
                    threshold = 3
                    if dprice > threshold:
                        session.execute('INSERT INTO `alerts` (`trade_id`, `severity`, `alert_type`, `comms`, `trades`) VALUES ('+str(tradeid)+', '+str(dprice)+', \'Price\', null, null)')
                    if dvolume > threshold:
                        session.execute('INSERT INTO `alerts` (`trade_id`, `severity`, `alert_type`, `comms`, `trades`) VALUES ('+str(tradeid)+', '+str(dvolume)+', \'Volume\', null, null)')
        except:
            raise
            session.rollback()
            traceback.print_exc()
        finally:
            self._YOU_SHALL_NOT_PASS.release()

    def store_comms(self, data, session):
        """ Stores comms """
        self._YOU_SHALL_NOT_PASS.acquire()
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
        finally:
            self._YOU_SHALL_NOT_PASS.release()

    def burst_store(self, data, session):
        """ Stores multiple entry data into storage """
        if len(data) > 0:
            if len(data[0]) == 3:
                self.store_comms(data, session)
            elif len(data[0]) == 10:
                self.store_trades(data, session)
        self._min_count_sem.acquire()
        self._min_count = self._min_count - 1
        if self._min_count == 0:
            try:
                self._min_timer.cancel()
                self._min_timer.join()
            except:
                pass
            self.worker_minute()
        self._min_count_sem.release()

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
        self._YOU_SHALL_NOT_PASS.acquire()
        # Schedule next run
        self._min_time = self._min_time + self._min_interval
        timeout = (self._min_time - datetime.now()).total_seconds()
        self._min_timer = threading.Timer(timeout, self.worker_minute)
        self._min_timer.start()
        # Aliases for ease of use
        session = self.Session()
        comms = self.tables['comms']
        trades = self.tables['trades']
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
        self._YOU_SHALL_NOT_PASS.release()
        self.logger.info('[SqlStorage] Finished worker_minute')

    def worker_day(self):
        self.logger.info('[SqlStorage] Running worker_day')
        self._YOU_SHALL_NOT_PASS.acquire()
        # Schedule next run
        self._day_time = self._day_time + self._day_interval
        timeout = (self._day_time - datetime.now()).total_seconds()
        self._day_timer = threading.Timer(timeout, self.worker_day)
        self._day_timer.start()
        # Aliases for ease of use
        session = self.Session()
        comms = self.tables['comms']
        trades = self.tables['trades']
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
        self.logger.info('[SqlStorage] Finished worker_day')
        self._YOU_SHALL_NOT_PASS.release()

    def unload(self):
        super(SqlStorage, self).unload()
        try:
            self._min_timer.cancel()
            self._min_timer.join()
        except:
            pass
        try:
            self._day_timer.cancel()
            self._day_timer.join()
        except:
            pass
        self.Session.remove()
        self.logger.info("[SqlStorage] unload")

    # QueryPlugin

    def trade(self, id_):
        try:
            trades = self.tables['trades']
            query = self._session.query(trades).filter(trades.c.id == id_)
            self._session.commit()
            return [list(r)[:-2] for r in self._session.execute(query)][0]
        except:
            self._session.rollback()
            raise

    def trades(self, query, number):
        try:
            trades = self.tables['trades']
            query = self._session.query(trades).order_by(trades.c.time.desc())\
                        .limit(number)
            self._session.commit()
            return [list(r)[:-2] for r in self._session.execute(query)]
        except:
            self._session.rollback()
            raise

    def recipients(self, comm_id):
        try:
            recipients = self.tables['recipients']
            query = self._session.query(recipients)\
                        .filter(recipients.c.comm_id == comm_id)
            self._session.commit()
            return [r[1] for r in self._session.execute(query)]
        except:
            self._session.rollback()
            raise

    def comms(self, query, number):
        try:
            comms = self.tables['comms']
            query = self._session.query(comms).order_by(comms.c.time.desc())\
                        .limit(number)
            self._session.commit()
            l = [list(r)[:-2] + [self.recipients(list(r)[0])]
                 for r in self._session.execute(query)]
            return l
        except:
            self._session.rollback()
            raise

    def alerts(self, query, number):
        try:
            alerts = self.tables['alerts']
            query = self._session.query(alerts).order_by(alerts.c.id.desc())\
                        .limit(number)
            self._session.commit()
            # return [list(r)[-2] for r in self._session.execute(query)]
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
