class TableColumn:
    def __init__(self,name = None,type = None,varchar_len = None,double_len = None,collate = None,is_nullable = None,default = None):
        self.name = name
        self.type = type
        self.varchar_len = varchar_len
        self.double_len = double_len
        self.collate = collate
        self.is_nullable = is_nullable
        self.default = default

    @property
    def name(self):
        return self.name

    @property
    def type(self):
        return self.type

    @property
    def varchar_len(self):
        return self.varchar_len

    @property
    def double_len(self):
        return self.double_len

    @property
    def collate(self):
        return self.collate

    @property
    def is_nullable(self):
        return self.is_nullable

    @property
    def default(self):
        return self.default


class TableConstraint:
    def __init__(self,name = None,ck = None,uk = None,pk = None,fk = None):
        self.name = name
        self.ck = ck
        self.uk = uk
        self.pk = pk
        self.fk = fk

    @property
    def name(self):
        return self.name

    @property
    def ck(self):
        return self.ck

    @property
    def uk(self):
        return self.uk

    @property
    def pk(self):
        return self.pk

    @property
    def fk(self):
        return self.fk


class TableRemark:
    def __init__(self, name=None, desc=None):
        self.name = name
        self.desc = desc

    @property
    def name(self):
        return self.name

    @property
    def desc(self):
        return self.desc




