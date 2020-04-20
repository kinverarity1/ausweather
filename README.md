# ausweather

Download Australian weather data from the [Bureau of
Meteorology](http://www.bom.gov.au/climate/data/) via
[SILO](https://www.longpaddock.qld.gov.au/silo/) using Python

## Installation

Install from the command line:

```python
python -m pip install -U ausweather
```

## Example of how to use

In a Python interpreter session:

```python
>>> import ausweather
```

To use this package to download annual rainfall data for Kent Town, first you
need to find the station number using the BoM [Weather Station
Directory](http://www.bom.gov.au/climate/data/stations/). Then you can use the
``fetch_bom_station_from_silo(station_number, email_address)`` function to
return a dictionary:

```python
>>> data = ausweather.fetch_bom_station_from_silo(23090, 'kinverarity@hotmail.com')
station #: 23090 name: ADELAIDE (KENT TOWN) title: 23090 ADELAIDE (KENT TOWN) (fetched from SILO on 2020-03-04 16:23:26.395696)
>>> data.keys()
dict_keys(['silo_returned', 'station_no', 'station_name', 'title', 'df', 'annual', 'srn'])
```

The data is stored in this dictionary under the key "df":

```python
>>> data['df'].info()
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 25677 entries, 1 to 25677
Data columns (total 28 columns):
Date       25677 non-null datetime64[ns]
Day        25677 non-null int32
Date2      25677 non-null object
T.Max      25677 non-null float64
Smx        25677 non-null int32
T.Min      25677 non-null float64
Smn        25677 non-null int32
Rain       25677 non-null float64
Srn        25677 non-null int32
Evap       25677 non-null float64
Sev        25677 non-null object
Radn       25677 non-null float64
Ssl        25677 non-null int32
VP         25677 non-null float64
Svp        25677 non-null int32
RHmaxT     25677 non-null float64
RHminT     25677 non-null float64
FAO56      25677 non-null float64
Mlake      25677 non-null float64
Mpot       25677 non-null float64
Mact       25677 non-null float64
Mwet       25677 non-null float64
Span       25677 non-null float64
Ssp        25677 non-null int32
EvSp       25677 non-null float64
Ses        25677 non-null int32
MSLPres    25677 non-null float64
Sp         25677 non-null int32
dtypes: datetime64[ns](1), float64(16), int32(9), object(2)
memory usage: 4.6+ MB
```

To see annual rainfall, you can group-by the ``dt.year`` accessor of the "Date"
column:

```python
>>> df = data["df"]
>>> df.groupby(df.Date.dt.year).Rain.sum()
Date
1950    426.9
1951    677.9
1952    584.9
1953    601.0
1954    439.6
        ...  
2016    820.8
2017    536.2
2018    429.8
2019    376.3
2020    101.6
Name: Rain, Length: 71, dtype: float64
```

## License

Released under the [MIT License](LICENSE.md).

## Version history

### Version 0.2.1 (3 Mar 2020)
- Fix bug for whitespace in BoM station name

### Version 0.2.0 (3 Mar 2020)
- Update, many changes.

### Version 0.1.0 (11 Feb 2020)
- Initial release