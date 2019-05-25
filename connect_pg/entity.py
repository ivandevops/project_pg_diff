class TableColumn:
    def __init__(self,name = None, type = None, varchar_len = None, double_len = None, collate = None, is_nullable = None, default = None):
        self.name = name
        self.type = type
        self.varchar_len = varchar_len
        self.double_len = double_len
        self.collate = collate
        self.is_nullable = is_nullable
        self.default = default

    def name(self):
        return self.name

    def type(self):
        return self.type

    def varchar_len(self):
        return self.varchar_len
    
    def double_len(self):
        return self.double_len

    def collate(self):
        return self.collate
    
    def is_nullable(self):
        return self.is_nullable

    def default(self):
        return self.default


class TableConstraint:
    def __init__(self,name = None,ck = None,uk = None,pk = None,fk = None):
        self.name = name
        self.ck = ck
        self.uk = uk
        self.pk = pk
        self.fk = fk

    def name(self):
        return self.name
    
    def ck(self):
        return self.ck

    def uk(self):
        return self.uk

    def pk(self):
        return self.pk
    
    def fk(self):
        return self.fk


class TableRemark:
    def __init__(self, name=None, desc=None):
        self.name = name
        self.desc = desc
    
    def name(self):
        return self.name

    def desc(self):
        return self.desc


class Sequence:
    def __init__(self, name = None, max = None , min = None, now = None , increment = None, start = None ):
        self.name = name
        self.max = max
        self.min = min
        self.now = now
        self.increment = increment
        self.start = start

    def name(self):
        return self.name

    def max(self):
        return self.max

    def min(self):
        return self.min

    def now(self):
        return self.now

    def increment(self):
        return self.increment

    def start(self):
        return self.start


class index:
    def __init__(self, index_name = None, fetch_table_name = None):
        self.name = index_name
        self.fetch_table_name = fetch_table_name






