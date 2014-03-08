#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict
import datetime
from decimal import Decimal, InvalidOperation
import doctest
import re
import sqlalchemy as sa
import dateutil.parser
metadata = sa.MetaData()

import math

def precision_and_scale(x):
    """
    From a float, decide what precision and scale are needed to represent it.
   
    >>> precision_and_scale(54.2)
    (3, 1)
    >>> precision_and_scale(9)
    (1, 0)
    
    Thanks to Mark Ransom, 
    http://stackoverflow.com/questions/3018758/determine-precision-and-scale-of-particular-number-in-python
    """
    max_digits = 14
    int_part = int(abs(x))
    magnitude = 1 if int_part == 0 else int(math.log10(int_part)) + 1
    if magnitude >= max_digits:
        return (magnitude, 0)
    frac_part = abs(x) - int_part
    multiplier = 10 ** (max_digits - magnitude)
    frac_digits = multiplier + int(multiplier * frac_part + 0.5)
    while frac_digits % 10 == 0:
        frac_digits /= 10
    scale = int(math.log10(frac_digits))
    return (magnitude + scale, scale)


complex_enough_to_be_date = re.compile(r"[\\\-\. /]")
def coerce_to_specific(datum):
    """
    Coerces datum to the most specific data type possible
    Order of preference: datetime, integer, decimal, float, string
    
    >>> coerce_to_specific(7.2)
    Decimal('7.2')
    >>> coerce_to_specific("Jan 17 2012")
    datetime.datetime(2012, 1, 17, 0, 0)
    >>> coerce_to_specific("something else")
    'something else'
    """
    try:
        if complex_enough_to_be_date.search(datum):
            return dateutil.parser.parse(datum)
    except TypeError:
        pass
    try:
        return int(str(datum))
    except ValueError:
        pass
    try: 
        return Decimal(str(datum))
    except InvalidOperation:
        pass
    try:
        return float(str(datum))
    except ValueError:
        pass
    return str(datum)

def best_coercable(data):
    """
    Given an iterable of scalar data, returns the datum representing the most specific
    data type the list overall can be coerced into, preferring datetimes,
    then integers, then decimals, then floats, then strings.  
    
    >>> best_coercable((6, '2', 9))
    6
    >>> best_coercable((Decimal('6.1'), 2, 9))
    Decimal('6.1')
    >>> best_coercable(('2014 jun 7', '2011 may 2'))
    datetime.datetime(2014, 6, 7, 0, 0)
    >>> best_coercable((7, 21.4, 'ruining everything'))
    'ruining everything'
    """
    preference = (datetime.datetime, int, Decimal, float, str)
    worst_pref = 0 
    (worst_prec, worst_scale) = (0, 0)
    worst = ''
    for datum in data:
        coerced = coerce_to_specific(datum)
        pref = preference.index(type(coerced))
        if pref > worst_pref:
            worst_pref = pref
            worst = coerced
        elif pref == worst_pref:
            if isinstance(coerced, Decimal):
                (prec, scale) = precision_and_scale(coerced)
                worst = Decimal("%s.%s" * ('9' * worst_prec, '9' * worst_scale))
                (worst_prec, worst_scale) = (max(prec, worst_prec), max(scale, worst_scale))
            elif isinstance(coerced, float):
                worst = max(coerced, worst)
            else:  # int, str
                if len(str(coerced)) > len(str(worst)):
                    worst = coerced
    return worst
            
    
complex_enough_to_be_date = re.compile(r"[\\\-\. /]")
def sqla_datatype_for(datum):
    """
    >>> sqla_datatype_for(7.2)
    DECIMAL(precision=2, scale=1)
    >>> sqla_datatype_for("Jan 17 2012")
    <class 'sqlalchemy.sql.sqltypes.DATETIME'>
    >>> sqla_datatype_for("something else")
    String(length=14)
    """
    try:
        if complex_enough_to_be_date.search(datum):
            result = dateutil.parser.parse(datum)
            return sa.DATETIME
    except TypeError:
        pass
    try:
        (prec, scale) = precision_and_scale(datum)
        return sa.DECIMAL(prec, scale)
    except TypeError:
        return sa.String(len(datum))
        
''' 
def least_picky_of(dtype1, dtype2):
    """
    >>> least_picky_of(sa.String(24), sa.String(12))
    String(length=24)
    >>> least_picky_of(sa.Decimal
    """
    import pdb; pdb.set_trace()
    if isinstance(dtype1, sa.String) or isinstance(dtype2, String):
        return sa.String(length=max(len(str(dtyp
    if isinstance(dtype1, sa.String):
        if isinstance(dtype2, sa.String):
            return sa.String(length=max(dtype1.length, dtype2.length))
        
        return sa.String(length=max(dtype1.length, len(str(dtype2))))
    elif isinstance(dtype2, sa.String):
        return sa.String(length=max(dtype2.length, len(str(dtype1))))
    import pdb; pdb.set_trace()
    elif isinstance(dtype1, sa.DECIMAL):
        if isinstance(dtype2, sa.DECIMAL):
            return sa.DECIMAL
                        
    print(dtype1)
''' 
    
class DDL(object):
    """
    >>> data = [{"name": "Lancelot", "kg": 83, "dob": "9 jan 461"}]
    >>> data.append({"name": "Gawain", "kg": 69.4})
    >>> table_def = DDL(data, "knights")
    
    """
    
    def __init__(self, data, table_name=None):
        if not hasattr(data, 'append'): # not a list
            self.data = [data,]
        else:
            self.data = data
        self.table_name = table_name or 'generated_table'
        
    def _generate(self):
        column_types = OrderedDict() 
        for row in self.data:
            for (k, v) in row.items():
                k = k.lower()   # case-sensitive column names are evil
                if k not in column_types:
                    column_types[k] = str
                column_types[k] = least_picky_of(column_types[k], sqla_datatype_for(v))
                
        self.sqla_def = sa.Table(self.table_name, metadata) 
        
        
    def _emit(self, dialect):
        self.sqla_def = sa.Table(self.table_name, metadata) 
        
       
    
if __name__ == '__main__':
    doctest.testmod()