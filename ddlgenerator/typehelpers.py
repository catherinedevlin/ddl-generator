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
def coerce_to_specific(datum):
    """
    Coerces datum to the most specific data type possible
    Order of preference: datetime, boolean, integer, decimal, float, string

    >>> coerce_to_specific(7.2)
    Decimal('7.2')
    >>> coerce_to_specific("Jan 17 2012")
    datetime.datetime(2012, 1, 17, 0, 0)
    >>> coerce_to_specific("something else")
    'something else'
    """
    try:
        if len(_complex_enough_to_be_date.findall(datum)) > 1:
            return dateutil.parser.parse(datum)
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

def best_representative(d1, d2):
    """
    Given two objects each coerced to the most specific type possible, return the one
    of the least restrictive type.

    >>> best_representative(6, 'foo')
    'foo'
    >>> best_representative(Decimal('4.95'), Decimal('6.1'))
    Decimal('9.99')
    """
    preference = (datetime.datetime, bool, int, Decimal, float, str)
    worst_pref = 0
    worst = ''
    for coerced in (d1, d2):
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