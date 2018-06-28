from sqlalchemy import create_engine


class DB(object):
    def __init__(self, host='localhost', db='soundcomparisons', user='soundcomparisons', password='pwd'):
        self.engine = create_engine('mysql+pymysql://%s:%s@%s/%s?charset=utf8mb4' % (user, password, host, db))

    def __call__(self, *args, **kw):
        return self.engine.execute(*args, **kw)
