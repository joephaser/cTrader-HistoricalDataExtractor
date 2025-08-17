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

    // User-configurable parameters
    // FlushMaxBufferChars is now fixed and not configurable

    // Removed: Flush Interval parameter
    public bool FlushEveryTick = false;

    [Parameter("Output Subfolder", DefaultValue = "cTraderTicks", Group = "File")]
    public string OutputSubfolder { get; set; }
    private readonly CompressionModeOption CompressionModeParam = CompressionModeOption.GZip;
    [Parameter("Use cAlgo Data Folder", DefaultValue = true, Group = "File")]
    public bool UseDataFolder { get; set; }
    [Parameter("Server Time UTC Offset (Hours)", DefaultValue = 0, MinValue = -24, MaxValue = 24, Step = 1, Group = "File")]
    public int ServerTimeUtcOffsetHours { get; set; }
    public bool AsyncWrites = true;

    // All ticks will be recorded (price change filter removed)

    // Tick-level file & buffering (daily rotation)
    private string _tickCsvFilePath;
    private bool _tickCsvHeaderWritten = false;
    private StringBuilder _tickBuffer = new StringBuilder(327680);
    private DateTime _lastTickFlush = DateTime.MinValue;
    private int _effectiveFlushChars;
    private int _effectiveFlushIntervalSeconds;
    // Single file mode (no rollover)
    private bool _fileOpened = false;
    private DateTime? _firstTickUtc = null;
    private DateTime _lastTickUtc = DateTime.MinValue;
    private string _outputFolder;

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

            // Removed: user-specified file name prefix

            // Sanitize / apply effective flush settings
            _effectiveFlushChars = 327680; // fixed value
            _effectiveFlushIntervalSeconds = 5; // fixed to 5 seconds
            _tickBuffer = new StringBuilder(_effectiveFlushChars); // fixed initial capacity

            OpenOutputFile();

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
            var serverNow = Server.Time; // broker server time
            var tickTimeUtc = GetUtc(serverNow);
            // No rollover; track range
            if (!_firstTickUtc.HasValue) _firstTickUtc = tickTimeUtc;
            _lastTickUtc = tickTimeUtc;
            var bid = Symbol.Bid; // Best bid price
            var ask = Symbol.Ask; // Best ask price
            // Compute spread manually; Symbol.Spread may be zero/unsupported in some contexts
            var spreadPips = (ask - bid) / Symbol.PipSize; // in pips
            var volume = Bars.TickVolumes.LastValue; // Current bar tick volume (proxy for activity)
            var tickTimeStr = tickTimeUtc.ToString("yyyy-MM-dd HH:mm:ss.fff");

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
                (_effectiveFlushIntervalSeconds > 0 && (serverNow - _lastTickFlush).TotalSeconds >= _effectiveFlushIntervalSeconds))
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
            TryFinalizeFileName();
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
                    // Column order: DateTimeUTC,Instrument,Granularity,Bid,Ask,Spread(pips),Volume
                    snapshot = "DateTimeUTC,Instrument,Granularity,Bid,Ask,Spread(pips),Volume\r\n" + snapshot;
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

        private void OpenOutputFile()
        {
            if (_fileOpened) return;
            var instrument = SymbolName;
            var granularity = GetShortTimeFrame(TimeFrame);
            // Always use dynamic naming (legacy behavior) with provisional RUNNING tag until stop
            var provisionalStamp = GetUtc(Server.Time).ToString("yyyyMMdd_HHmmss");
            var prefix = $"{instrument}_{granularity}";
            var fileName = $"{prefix}_{provisionalStamp}_to_RUNNING_ticks.csv"; // finalized later
            InitializeFile(fileName);
            _fileOpened = true;
        }

    // Convert server (broker) time to UTC using configured offset
    private DateTime GetUtc(DateTime serverTime) => serverTime - TimeSpan.FromHours(ServerTimeUtcOffsetHours);

        private void InitializeFile(string baseFileName)
        {
            var documents = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
            string baseFolder;
            if (UseDataFolder)
            {
                // Typical platform data path under Documents\cAlgo\Data\cBots\HistoricalDataExtractor
                var dataRoot = Path.Combine(documents, "cAlgo", "Data", "cBots", "HistoricalDataExtractor");
                baseFolder = string.IsNullOrWhiteSpace(OutputSubfolder) ? dataRoot : Path.Combine(dataRoot, OutputSubfolder);
            }
            else
            {
                baseFolder = Path.Combine(documents, OutputSubfolder ?? "cTraderTicks");
            }
            try { Directory.CreateDirectory(baseFolder); } catch (Exception ex) { Print($"Failed to create directory '{baseFolder}': {ex.Message}"); }

            if (CompressionModeParam == CompressionModeOption.GZip)
                baseFileName += ".gz";

            _tickCsvFilePath = Path.Combine(baseFolder, baseFileName);
            _outputFolder = baseFolder;
            var fileExists = File.Exists(_tickCsvFilePath);
            _tickCsvHeaderWritten = fileExists; // if exists assume header present
            try
            {
                if (CompressionModeParam == CompressionModeOption.GZip)
                {
                    _fileStream = new FileStream(_tickCsvFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
                    _gzipStream = new GZipStream(_fileStream, CompressionLevel.SmallestSize, leaveOpen: true);
                }
                else if (!fileExists)
                {
                    // Create empty file early so user can see it immediately
                    using (var fs = new FileStream(_tickCsvFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite)) { }
                }

                // Write header immediately for visibility (and avoid waiting for first flush)
                if (!_tickCsvHeaderWritten)
                {
                    var header = "DateTimeUTC,Instrument,Granularity,Bid,Ask,Spread(pips),Volume\r\n";
                    if (CompressionModeParam == CompressionModeOption.GZip)
                    {
                        var bytes = Encoding.UTF8.GetBytes(header);
                        _gzipStream.Write(bytes, 0, bytes.Length);
                        _gzipStream.Flush();
                        _fileStream.Flush();
                    }
                    else
                    {
                        File.AppendAllText(_tickCsvFilePath, header, Encoding.UTF8);
                    }
                    _tickCsvHeaderWritten = true;
                }
            }
            catch (Exception ex)
            {
                Print($"Failed to open file '{_tickCsvFilePath}': {ex.Message}");
            }
        }

        private void CloseCurrentStreams()
        {
            try
            {
                _gzipStream?.Dispose();
            }
            catch { }
            _gzipStream = null;
            try
            {
                _fileStream?.Dispose();
            }
            catch { }
            _fileStream = null;
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
                        var fs = new FileStream(_tickCsvFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
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
            // Drain remaining queue after cancellation
            while (_writeQueue.TryDequeue(out var remaining))
            {
                WriteChunk(remaining);
            }
        }

        private void TryFinalizeFileName()
        {
            // No longer skip renaming for custom file name (option removed)
            if (string.IsNullOrEmpty(_tickCsvFilePath) || !_firstTickUtc.HasValue || _lastTickUtc == DateTime.MinValue)
                return;
            try
            {
                var instrument = SymbolName;
                var granularity = GetShortTimeFrame(TimeFrame);
                var first = _firstTickUtc.Value;
                var last = _lastTickUtc;
                var prefix = $"{instrument}_{granularity}";
                var newName = string.Format("{0}_{1}_to_{2}_ticks.csv", prefix,
                    first.ToString("yyyyMMdd_HHmmss"), last.ToString("yyyyMMdd_HHmmss"));
                if (CompressionModeParam == CompressionModeOption.GZip) newName += ".gz";
                var finalPath = Path.Combine(_outputFolder ?? Path.GetDirectoryName(_tickCsvFilePath)!, newName);
                if (File.Exists(finalPath))
                {
                    int i = 1;
                    string candidate;
                    do
                    {
                        candidate = Path.Combine(_outputFolder ?? Path.GetDirectoryName(_tickCsvFilePath)!,
                            Path.GetFileNameWithoutExtension(newName) + "_" + i + Path.GetExtension(newName));
                        i++;
                    } while (File.Exists(candidate) && i < 1000);
                    finalPath = candidate;
                }
                if (!string.Equals(finalPath, _tickCsvFilePath, StringComparison.OrdinalIgnoreCase))
                {
                    File.Move(_tickCsvFilePath, finalPath, overwrite: false);
                    _tickCsvFilePath = finalPath;
                }
            }
            catch (Exception ex)
            {
                Print($"Filename finalize failed: {ex.Message}");
            }
        }

        // Enums for parameters
    public enum CompressionModeOption { None, GZip }
    }
}