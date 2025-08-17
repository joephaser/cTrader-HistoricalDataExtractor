using System;
using System.IO;
using System.Text; // Buffering tick-level writes
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.IO.Compression;
using cAlgo.API;
using cAlgo.API.Internals;

namespace cAlgo.Robots
{
    // File writes require FileSystem (or FullAccess). Using FileSystem keeps scope minimal.
    [Robot(AccessRights = AccessRights.FullAccess, AddIndicators = true)]
    public class HistoricalDataExtractor : Robot
    {
    // General
    [Parameter("Message", Group = "General", DefaultValue = "")] 
    public string Message { get; set; }

    // User-configurable parameters
    [Parameter("Flush Max Buffer Chars", DefaultValue = 32768, MinValue = 512, Step = 512, Group = "Flush Settings")]
    public int FlushMaxBufferChars { get; set; }

    [Parameter("Flush Interval (Seconds)", DefaultValue = 2, MinValue = 0, Step = 1, Group = "Flush Settings")]
    public int FlushIntervalSeconds { get; set; }

    [Parameter("Flush Every Tick", DefaultValue = false, Group = "Flush Settings")]
    public bool FlushEveryTick { get; set; }

    [Parameter("Output Subfolder", DefaultValue = "cTraderTicks", Group = "File")]
    public string OutputSubfolder { get; set; }
    [Parameter("Rollover Mode", DefaultValue = RolloverMode.Daily, Group = "File")]
    public RolloverMode FileRolloverMode { get; set; }
    [Parameter("Compression", DefaultValue = CompressionModeOption.None, Group = "File")]
    public CompressionModeOption CompressionModeParam { get; set; }

    [Parameter("Async Writes", DefaultValue = true, Group = "Performance")]
    public bool AsyncWrites { get; set; }

    [Parameter("Price Change Filter", DefaultValue = PriceChangeFilterMode.AllTicks, Group = "Filtering")]
    public PriceChangeFilterMode PriceChangeFilter { get; set; }

    // Tick-level file & buffering (daily rotation)
    private string _tickCsvFilePath;
    private bool _tickCsvHeaderWritten = false;
    private StringBuilder _tickBuffer = new StringBuilder(32_768);
    private DateTime _lastTickFlush = DateTime.MinValue;
    private int _effectiveFlushChars;
    private int _effectiveFlushIntervalSeconds;
    private DateTime _currentFileDate = DateTime.MinValue; // date for current daily file
    private DateTime _sessionStart;

    // Filtering state
    private bool _havePrevPrices = false;
    private double _prevBid;
    private double _prevAsk;

    // Async write infrastructure
    private readonly ConcurrentQueue<string> _writeQueue = new ConcurrentQueue<string>();
    private CancellationTokenSource _writerCts;
    private Task _writerTask;
    private volatile bool _writerRunning;

    // Compression streams
    private FileStream _fileStream;
    private GZipStream _gzipStream; // when compression enabled

        protected override void OnStart()
        {
            // To learn more about cTrader Algo visit our Help Center:
            // https://help.ctrader.com/ctrader-algo/

            if (!string.IsNullOrWhiteSpace(Message))
                Print(Message);

            // Sanitize / apply effective flush settings
            _effectiveFlushChars = Math.Max(FlushMaxBufferChars, 512); // enforce minimum
            _effectiveFlushIntervalSeconds = Math.Max(FlushIntervalSeconds, 0); // 0 => disabled time-based flush
            _tickBuffer = new StringBuilder(Math.Min(_effectiveFlushChars, 131072)); // cap initial capacity

            _sessionStart = Server.Time;
            // Open initial file (daily or session)
            if (FileRolloverMode == RolloverMode.Daily)
                OpenNewFile(Server.Time.Date);
            else
                OpenNewSessionFile();

            // Start a 1-second timer if we need time-based flushing independent of tick arrival
            if (_effectiveFlushIntervalSeconds > 0)
                Timer.Start(1);

            // Start writer if async
            if (AsyncWrites)
            {
                _writerCts = new CancellationTokenSource();
                _writerRunning = true;
                _writerTask = Task.Run(() => WriterLoop(_writerCts.Token));
            }
        }

        protected override void OnTick()
        {
            // Tick-level bid/ask logging (each tick only)
            var instrument = SymbolName;
            var granularity = GetShortTimeFrame(TimeFrame); // still useful for naming context
            var tickTime = Server.Time; // current tick time
            // Rotate file only if daily mode
            if (FileRolloverMode == RolloverMode.Daily && tickTime.Date != _currentFileDate)
                OpenNewFile(tickTime.Date);
            var bid = Symbol.Bid; // Best bid price
            var ask = Symbol.Ask; // Best ask price
            // Compute spread manually; Symbol.Spread may be zero/unsupported in some contexts
            var spreadPips = (ask - bid) / Symbol.PipSize; // in pips
            var volume = Bars.TickVolumes.LastValue; // Current bar tick volume (proxy for activity)
            var tickTimeStr = tickTime.ToString("yyyy-MM-dd HH:mm:ss.fff");

            // Price change filtering
            bool bidChanged = !_havePrevPrices || bid != _prevBid;
            bool askChanged = !_havePrevPrices || ask != _prevAsk;
            bool spreadChanged = !_havePrevPrices || (ask - bid) != (_prevAsk - _prevBid);
            bool shouldLog = PriceChangeFilter switch
            {
                PriceChangeFilterMode.AllTicks => true,
                PriceChangeFilterMode.AnySideChange => bidChanged || askChanged,
                PriceChangeFilterMode.BidOnly => bidChanged,
                PriceChangeFilterMode.AskOnly => askChanged,
                PriceChangeFilterMode.SpreadChange => spreadChanged,
                _ => true
            };
            if (!_havePrevPrices)
            {
                _prevBid = bid; _prevAsk = ask; _havePrevPrices = true; // ensure first tick logged
                shouldLog = true;
            }
            if (shouldLog)
            {
                _prevBid = bid; _prevAsk = ask;
            }
            else
            {
                // Still evaluate flush timers even if no logging
                if (FlushEveryTick)
                    FlushTickBuffer();
                return;
            }

            // Dynamic formatting based on symbol digits for bid/ask
            var priceFormat = "F" + Symbol.Digits;
            var spreadFormat = "F2"; // Slightly higher precision for spread
            _tickBuffer
                .Append(tickTimeStr).Append(',')
                .Append(instrument).Append(',')
                .Append(granularity).Append(',')
                .Append(bid.ToString(priceFormat)).Append(',')
                .Append(ask.ToString(priceFormat)).Append(',')
                .Append(spreadPips.ToString(spreadFormat)).Append(',')
                .Append(volume)
                .Append("\r\n");

            // Flush conditions: explicit each tick OR size threshold OR time interval (if > 0)
            if (FlushEveryTick ||
                _tickBuffer.Length >= _effectiveFlushChars ||
                (_effectiveFlushIntervalSeconds > 0 && (tickTime - _lastTickFlush).TotalSeconds >= _effectiveFlushIntervalSeconds))
                FlushTickBuffer();
        }

        protected override void OnTimer()
        {
            // Ensure periodic flush even during quiet markets
            if (_effectiveFlushIntervalSeconds > 0 &&
                (Server.Time - _lastTickFlush).TotalSeconds >= _effectiveFlushIntervalSeconds &&
                _tickBuffer.Length > 0)
            {
                FlushTickBuffer();
            }
        }

        protected override void OnStop()
        {
            // Final flush of tick buffer
            FlushTickBuffer();
            // Drain queue if async
            if (AsyncWrites)
            {
                _writerRunning = false;
                _writerCts.Cancel();
                try { _writerTask?.Wait(3000); } catch { }
            }
            CloseCurrentStreams();
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

        // Flush buffered tick lines to disk
        private void FlushTickBuffer()
        {
            if (_tickBuffer.Length == 0) return;
            try
            {
                var snapshot = _tickBuffer.ToString();
                _tickBuffer.Clear();

                if (!_tickCsvHeaderWritten)
                {
                    // Column order: DateTime,Instrument,Granularity,Bid,Ask,Spread(pips),Volume
                    snapshot = "DateTime,Instrument,Granularity,Bid,Ask,Spread(pips),Volume\r\n" + snapshot;
                    _tickCsvHeaderWritten = true;
                }

                if (AsyncWrites)
                {
                    _writeQueue.Enqueue(snapshot);
                }
                else
                {
                    WriteChunk(snapshot);
                }
                _lastTickFlush = Server.Time;
            }
            catch (Exception ex)
            {
                Print($"Error flushing tick buffer: {ex.Message}");
            }
        }

        private void OpenNewFile(DateTime date)
        {
            FlushTickBuffer(); // flush buffer to old file first
            CloseCurrentStreams();
            _currentFileDate = date.Date;
            var instrument = SymbolName;
            var granularity = GetShortTimeFrame(TimeFrame);
            var fileName = string.Format("{0}_{1}_{2:yyyyMMdd}_ticks.csv", instrument, granularity, _currentFileDate);
            InitializeFile(fileName);
        }

        private void OpenNewSessionFile()
        {
            FlushTickBuffer();
            CloseCurrentStreams();
            var instrument = SymbolName;
            var granularity = GetShortTimeFrame(TimeFrame);
            var fileName = string.Format("{0}_{1}_{2:yyyyMMdd_HHmmss}_session_ticks.csv", instrument, granularity, _sessionStart);
            InitializeFile(fileName);
        }

        private void InitializeFile(string baseFileName)
        {
            var baseFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), OutputSubfolder ?? "cTraderTicks");
            try { Directory.CreateDirectory(baseFolder); } catch (Exception ex) { Print($"Failed to create directory '{baseFolder}': {ex.Message}"); }

            if (CompressionModeParam == CompressionModeOption.GZip)
                baseFileName += ".gz";

            _tickCsvFilePath = Path.Combine(baseFolder, baseFileName);
            _tickCsvHeaderWritten = File.Exists(_tickCsvFilePath); // if exists assume header present
            try
            {
                if (CompressionModeParam == CompressionModeOption.GZip)
                {
                    _fileStream = new FileStream(_tickCsvFilePath, FileMode.Append, FileAccess.Write, FileShare.Read);
                    _gzipStream = new GZipStream(_fileStream, CompressionLevel.SmallestSize, leaveOpen: true);
                }
            }
            catch (Exception ex)
            {
                Print($"Failed to open file '{_tickCsvFilePath}': {ex.Message}");
            }
        }

        private void CloseCurrentStreams()
        {
            try { _gzipStream?.Dispose(); } catch { }
            try { _fileStream?.Dispose(); } catch { }
            _gzipStream = null; _fileStream = null;
        }

        private void WriteChunk(string chunk)
        {
            if (string.IsNullOrEmpty(chunk)) return;
            try
            {
                if (CompressionModeParam == CompressionModeOption.GZip)
                {
                    if (_gzipStream == null)
                    {
                        // reopen (e.g., after rotation)
                        var fs = new FileStream(_tickCsvFilePath, FileMode.Append, FileAccess.Write, FileShare.Read);
                        _fileStream = fs;
                        _gzipStream = new GZipStream(_fileStream, CompressionLevel.SmallestSize, leaveOpen: true);
                    }
                    var bytes = Encoding.UTF8.GetBytes(chunk);
                    _gzipStream.Write(bytes, 0, bytes.Length);
                    _gzipStream.Flush();
                    _fileStream.Flush();
                }
                else
                {
                    File.AppendAllText(_tickCsvFilePath, chunk, Encoding.UTF8);
                }
            }
            catch (Exception ex)
            {
                Print($"WriteChunk error: {ex.Message}");
            }
        }

        private void WriterLoop(CancellationToken ct)
        {
            while (_writerRunning && !ct.IsCancellationRequested)
            {
                if (_writeQueue.TryDequeue(out var chunk))
                {
                    WriteChunk(chunk);
                }
                else
                {
                    Thread.Sleep(10);
                }
            }
            // Drain remaining
            while (_writeQueue.TryDequeue(out var remaining))
            {
                WriteChunk(remaining);
            }
        }

        // Enums for parameters
        public enum RolloverMode { Daily, Session }
        public enum CompressionModeOption { None, GZip }
        public enum PriceChangeFilterMode { AllTicks, AnySideChange, BidOnly, AskOnly, SpreadChange }
    }
}