from sqlalchemy.sql import func


class identity_agg(object):

    def __call__(self, x):
        return x

    def __nonzero__(self):
        return False

identity_agg = identity_agg()

avg = func.avg

sum = func.sum
