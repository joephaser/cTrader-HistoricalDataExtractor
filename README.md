# cTrader Historical Data Extractor

This project is a cTrader cBot for extracting historical OHLCV (Open, High, Low, Close, Volume) and spread data from cTrader charts and saving it to a CSV file. The bot is designed for traders and developers who need to analyze or export historical market data for backtesting, research, or other purposes.

## Features
- Exports bar data (OHLCV and spread) to a CSV file in your Documents folder
- Automatically names the CSV file based on instrument, timeframe, and date range
- Writes a header row if the file is new
- Appends new bar data as it becomes available
- Supports multiple timeframes and instruments

## How It Works
- On each new bar, the bot writes the previous bar's data to a CSV file.
- The CSV file is named as: `<Instrument>_<TimeFrame>_<StartDate>_<EndDate>.csv`
- The file is saved in your Windows Documents folder.
- The following columns are included: `DateTime, Instrument, Granularity, Open, High, Low, Close, Volume, Spread`

## Usage
1. Open the project in cTrader Automate (cAlgo).
2. Build and attach the `HistoricalDataExtractor` cBot to a chart.
3. Set the desired parameters (optional).
4. Start the cBot. The CSV file will be created/updated in your Documents folder.

## Parameters
- **Message**: A custom message printed to the cBot log when started (default: "Historical Data Extractor").

## File Example
```
DateTime,Instrument,Granularity,Open,High,Low,Close,Volume,Spread
2025-08-01 00:00:00.000,EURUSD,M1,1.1000,1.1010,1.0990,1.1005,100,0.0002
...
```

## Requirements
- cTrader platform (with cAlgo API)
- .NET 6.0 or compatible

## License
MIT

## Author
[joephaser](https://github.com/joephaser)
