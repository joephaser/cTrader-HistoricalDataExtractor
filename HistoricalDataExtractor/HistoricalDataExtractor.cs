using System;
using cAlgo.API;
using cAlgo.API.Internals;

namespace cAlgo.Robots
{
    [Robot(AccessRights = AccessRights.None, AddIndicators = true)]
    public class HistoricalDataExtractor : Robot
    {
        public string Message { get; set; }

        private string _csvFilePath;
        private bool _csvHeaderWritten = false;

        // Field for tracking last bar index and bar-close spread
        private int _lastBarIndex;
        private double _barCloseSpread;

        protected override void OnStart()
        {
            // To learn more about cTrader Algo visit our Help Center:
            // https://help.ctrader.com/ctrader-algo/

            Print(Message);

            // Set CSV file path in the bot's data folder
            var startDate = Bars.Count > 0 ? Bars[0].OpenTime.ToString("yyyyMMdd") : Server.Time.ToString("yyyyMMdd");
            var endDate = Bars.Count > 0 ? Bars[Bars.Count - 1].OpenTime.ToString("yyyyMMdd") : Server.Time.ToString("yyyyMMdd");
            var granularity = GetShortTimeFrame(TimeFrame);
            var instrument = SymbolName;
            var fileName = string.Format("{0}_{1}_{2}_{3}.csv", instrument, granularity, startDate, endDate);
            _csvFilePath = System.IO.Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), fileName);
            _csvHeaderWritten = System.IO.File.Exists(_csvFilePath) && System.IO.File.ReadAllText(_csvFilePath).Contains("DateTime,Instrument,Granularity,Open,High,Low,Close,Volume,Spread");

            // Initialize bar index and bar-close spread
            _lastBarIndex = Bars.Count - 1;
            _barCloseSpread = Symbol.Spread * 10000;
        }

        protected override void OnTick()
        {
            // Bar-level spread and OHLC logging
            var currentIndex = Bars.Count - 1;
            var currentSpread = Symbol.Spread * 10000;
            var granularity = GetShortTimeFrame(TimeFrame);
            var instrument = SymbolName;
            if (currentIndex != _lastBarIndex)
            {
                // New bar: write previous bar data
                var bar = Bars[_lastBarIndex];
                var dateTime = bar.OpenTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
                var volume = bar.TickVolume;
                // Prepare CSV line: DateTime, Instrument, Granularity, Open, High, Low, Close, Volume, Spread
                var csvLine = string.Format("{0},{1},{2},{3:F5},{4:F5},{5:F5},{6:F5},{7},{8:F1}",
                    dateTime,
                    instrument,
                    granularity,
                    bar.Open, bar.High, bar.Low, bar.Close,
                    volume,
                    _barCloseSpread);
                // Write header if not already written
                if (!_csvHeaderWritten)
                {
                    System.IO.File.AppendAllText(_csvFilePath, "DateTime,Instrument,Granularity,Open,High,Low,Close,Volume,Spread\r\n");
                    _csvHeaderWritten = true;
                }
                System.IO.File.AppendAllText(_csvFilePath, csvLine + "\r\n");
                // Reset bar-close spread for new bar
                _barCloseSpread = currentSpread;
                _lastBarIndex = currentIndex;
            }
            else
            {
                // Same bar: update bar-close spread
                _barCloseSpread = currentSpread;
            }
        }

        protected override void OnStop()
        {
            // Handle cBot stop here
        }

        // Helper to convert TimeFrame to short format (e.g. M1, M5, H1)
        private string GetShortTimeFrame(TimeFrame tf)
        {
            if (tf == TimeFrame.Minute) return "M1";
            if (tf == TimeFrame.Minute5) return "M5";
            if (tf == TimeFrame.Minute15) return "M15";
            if (tf == TimeFrame.Minute30) return "M30";
            if (tf == TimeFrame.Hour) return "H1";
            if (tf == TimeFrame.Hour4) return "H4";
            if (tf == TimeFrame.Daily) return "D1";
            if (tf == TimeFrame.Weekly) return "W1";
            if (tf == TimeFrame.Monthly) return "MN";
            // fallback to string
            return tf.ToString();
        }
    }
}