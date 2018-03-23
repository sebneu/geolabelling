from collections import defaultdict
import datetime

from rdflib.namespace import XSD


def profile(data_columns):

    columns = [Column(vals['values']['exact']) for vals in data_columns]
    types =  [c.label for c in columns]
    return types


class ColumnLabel:
    NUMBER = XSD.number
    ANY = XSD.anyAtomicType
    BINARY = XSD.binary
    DATETIME = XSD.datetime

    EMPTY = XSD.anyAtomicType
    

def isfloat(value):
  try:
    float(value)
    return True
  except:
    return False


def isdatetime(datestring):
    try:
        d = datetime.datetime.strptime(datestring, '%Y-%m-%dT%H:%M:%S')
        if d:
            return True
        else:
            return False
    except Exception as e:
        return False


def get_label(v):
    if v.isnumeric() or isfloat(v):
        return ColumnLabel.NUMBER
    if isfloat(v):
        return ColumnLabel.NUMBER
    if isdatetime(v):
        return ColumnLabel.DATETIME
    if not v:
        return ColumnLabel.EMPTY
    return ColumnLabel.ANY


class Column:
    def __init__(self, values):
        self.values = values
        self._classify()

    def _classify(self):
        self.char_dist = defaultdict(int)
        labels = defaultdict(int)
        lengths = defaultdict(int)
        tokens = defaultdict(int)

        for value in self.values:
            for c in value:
                self.char_dist[c] += 1

            labels[get_label(value)] += 1
            lengths[len(value)] += 1
            tok = len(value.split(' '))
            length = '4+' if tok >= 4 else str(tok)
            tokens[length] += 1

        # do not consider empty cells
        if ColumnLabel.EMPTY in labels:
            del labels[ColumnLabel.EMPTY]

        if len(labels) == 1 and ColumnLabel.NUMBER in labels:
            self.label = ColumnLabel.NUMBER

        elif len(labels) == 1 and ColumnLabel.DATETIME in labels:
            self.label = ColumnLabel.DATETIME

        elif len(labels) == 1 and len(set(self.values)) == 2 and len(tokens) == 1 and '1' in tokens:
            self.label = ColumnLabel.BINARY

        else:
            self.label = ColumnLabel.ANY

        #self.length = max(tokens.iteritems(), key=operator.itemgetter(1))[0]
