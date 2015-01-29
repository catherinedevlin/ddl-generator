#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Various functions for examining data types.
"""
import datetime
from decimal import Decimal, InvalidOperation
import doctest
import math
import re
import sqlalchemy as sa
import dateutil.parser

def is_scalar(x):
    return hasattr(x, 'lower') or not hasattr(x, '__iter__')

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
    if isinstance(x, Decimal):
        precision = len(x.as_tuple().digits)
        scale = -1 * x.as_tuple().exponent
        if scale < 0:
            precision -= scale
            scale = 0
        return (precision, scale)
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

_complex_enough_to_be_date = re.compile(r"[\-\. /]")
_digits_only = re.compile(r"^\d+$")
def coerce_to_specific(datum):
    """
    Coerces datum to the most specific data type possible
    Order of preference: datetime, boolean, integer, decimal, float, string

    >>> coerce_to_specific('-000000001854.60')
    Decimal('-1854.60')
    >>> coerce_to_specific(7.2)
    Decimal('7.2')
    >>> coerce_to_specific("Jan 17 2012")
    datetime.datetime(2012, 1, 17, 0, 0)
    >>> coerce_to_specific("something else")
    'something else'
    >>> coerce_to_specific("20141010")
    datetime.datetime(2014, 10, 10, 0, 0)
    >>> coerce_to_specific("001210107")
    1210107
    >>> coerce_to_specific("010")
    10
    """
    if datum is None:
        return None 
    try:
        result = dateutil.parser.parse(datum)
        # but even if this does not raise an exception, may
        # not be a date -- dateutil's parser is very aggressive
        # check for nonsense unprintable date
        str(result) 
        # most false date hits will be interpreted as times today
        # or as unlikely far-future or far-past years
        clean_datum = datum.strip().lstrip('-').lstrip('0').rstrip('.')
        if len(_complex_enough_to_be_date.findall(clean_datum)) < 2:
            digits = _digits_only.search(clean_datum)
            if (not digits) or (len(digits.group(0)) not in 
                                (4, 6, 8, 12, 14, 17)):
                raise Exception("false date hit for %s" % datum)
            if result.date() == datetime.datetime.now().date():
                raise Exception("false date hit (%s) for %s" % (
                    str(result), datum))
            if not (1700 < result.year < 2150):
                raise Exception("false date hit (%s) for %s" % (
                    str(result), datum)) 
        return result
    except Exception as e:
        pass
    if str(datum).strip().lower() in ('0', 'false', 'f', 'n', 'no'):
        return False
    elif str(datum).strip().lower() in ('1', 'true', 't', 'y', 'yes'):
        return True
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

def _places_b4_and_after_decimal(d):
    """
    >>> _places_b4_and_after_decimal(Decimal('54.212'))
    (2, 3)
    """
    tup = d.as_tuple()
    return (len(tup.digits) + tup.exponent, max(-1*tup.exponent, 0))

def worst_decimal(d1, d2):
    """
    Given two Decimals, return a 9-filled decimal representing both enough > 0 digits
    and enough < 0 digits (scale) to accomodate numbers like either.

    >>> worst_decimal(Decimal('762.1'), Decimal('-1.983'))
    Decimal('999.999')
    """
    (d1b4, d1after) = _places_b4_and_after_decimal(d1)
    (d2b4, d2after) = _places_b4_and_after_decimal(d2)
    return Decimal('9' * max(d1b4, d2b4) + '.' + '9' * max(d1after, d2after))

def set_worst(old_worst, new_worst):
    """
    Pad new_worst with zeroes to prevent it being shorter than old_worst.
    
    >>> set_worst(311920, '48-49')
    '48-490'
    >>> set_worst(98, -2)
    -20
    """
    
    if isinstance(new_worst, bool):
        return new_worst
    # Negative numbers confuse the length calculation. 
    negative = ( (hasattr(old_worst, '__neg__') and old_worst < 0) or
                 (hasattr(new_worst, '__neg__') and new_worst < 0) )
    try:
        old_worst = abs(old_worst)
        new_worst = abs(new_worst)
    except TypeError:
        pass
   
    # now go by length 
    new_len = len(str(new_worst))
    old_len = len(str(old_worst))
    if new_len < old_len:
        new_type = type(new_worst)
        new_worst = str(new_worst).ljust(old_len, '0')
        new_worst = new_type(new_worst)
        
    # now put the removed negative back
    if negative:
        try:
            new_worst = -1 * abs(new_worst)
        except:
            pass
        
    return new_worst
    
def best_representative(d1, d2):
    """
    Given two objects each coerced to the most specific type possible, return the one
    of the least restrictive type.

    >>> best_representative(Decimal('-37.5'), Decimal('0.9999'))
    Decimal('-99.9999')
    >>> best_representative(None, Decimal('6.1'))
    Decimal('6.1')
    >>> best_representative(311920, '48-49')
    '48-490'
    >>> best_representative(6, 'foo')
    'foo'
    >>> best_representative(Decimal('4.95'), Decimal('6.1'))
    Decimal('9.99')
    >>> best_representative(Decimal('-1.9'), Decimal('6.1'))
    Decimal('-9.9')
    """
  
    if hasattr(d2, 'strip') and not d2.strip():
        return d1
    if d1 is None:
        return d2
    elif d2 is None:
        return d1
    preference = (datetime.datetime, bool, int, Decimal, float, str)
    worst_pref = 0
    worst = ''
    for coerced in (d1, d2):
        pref = preference.index(type(coerced))
        if pref > worst_pref:
            worst_pref = pref
            worst = set_worst(worst, coerced)
        elif pref == worst_pref:
            if isinstance(coerced, Decimal):
                worst = set_worst(worst, worst_decimal(coerced, worst))
            elif isinstance(coerced, float):
                worst = set_worst(worst, max(coerced, worst))
            else:  # int, str
                if len(str(coerced)) > len(str(worst)):
                    worst = set_worst(worst, coerced)
    return worst

def best_coercable(data):
    """
    Given an iterable of scalar data, returns the datum representing the most specific
    data type the list overall can be coerced into, preferring datetimes, then bools,
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
    preference = (datetime.datetime, bool, int, Decimal, float, str)
    worst_pref = 0
    worst = ''
    for datum in data:
        coerced = coerce_to_specific(datum)
        pref = preference.index(type(coerced))
        if pref > worst_pref:
            worst_pref = pref
            worst = coerced
        elif pref == worst_pref:
            if isinstance(coerced, Decimal):
                worst = worst_decimal(coerced, worst)
            elif isinstance(coerced, float):
                worst = max(coerced, worst)
            else:  # int, str
                if len(str(coerced)) > len(str(worst)):
                    worst = coerced
    return worst

def sqla_datatype_for(datum):
    """
    Given a scalar Python value, picks an appropriate SQLAlchemy data type.

    >>> sqla_datatype_for(7.2)
    DECIMAL(precision=2, scale=1)
    >>> sqla_datatype_for("Jan 17 2012")
    <class 'sqlalchemy.sql.sqltypes.DATETIME'>
    >>> sqla_datatype_for("something else")
    Unicode(length=14)
    """
    try:
        if len(_complex_enough_to_be_date.findall(datum)) > 1:
            dateutil.parser.parse(datum)
            return sa.DATETIME
    except (TypeError, ValueError):
        pass
    try:
        (prec, scale) = precision_and_scale(datum)
        return sa.DECIMAL(prec, scale)
    except TypeError:
        return sa.Unicode(len(datum))

if __name__ == '__main__':
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
