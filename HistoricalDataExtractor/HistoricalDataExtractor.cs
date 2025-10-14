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

    [Parameter("Output Subfolder", DefaultValue = "Extracted", Group = "File")]
    public string OutputSubfolder { get; set; }
    private readonly CompressionModeOption CompressionModeParam = CompressionModeOption.GZip;
    [Parameter("Server Time UTC Offset (Hours)", DefaultValue = 0, MinValue = -24, MaxValue = 24, Step = 1, Group = "File")]
    public int ServerTimeUtcOffsetHours { get; set; }
    public bool AsyncWrites = true;

    // Bar data will be recorded (OHLC candles)

    // Bar-level file & buffering - optimized for large datasets
    private string _barCsvFilePath;
    private bool _barCsvHeaderWritten = false;
    private StringBuilder _barBuffer = new StringBuilder(1048576); // 1MB initial capacity (increased from 327KB)
    private DateTime _lastBarFlush = DateTime.MinValue;
    private int _effectiveFlushChars;
    private int _effectiveFlushIntervalSeconds;
    // Single file mode (no rollover)
    private bool _fileOpened = false;
    private DateTime? _firstBarUtc = null;
    private DateTime _lastBarUtc = DateTime.MinValue;
    private string _outputFolder;
    private int _lastProcessedBarIndex = -1;

    // Async write infrastructure - optimized queue
    private readonly ConcurrentQueue<string> _writeQueue = new ConcurrentQueue<string>();
    private CancellationTokenSource _writerCts;
    private Task _writerTask;
    private volatile bool _writerRunning;
    private readonly object _flushLock = new object(); // Prevent concurrent flush operations

    // Compression streams
    private FileStream _fileStream;
    private GZipStream _gzipStream; // when compression enabled
    private StreamWriter _streamWriter; // for writing to compressed or uncompressed stream

        protected override void OnStart()
        {
            // To learn more about cTrader Algo visit our Help Center:
            // https://help.ctrader.com/ctrader-algo/

            // Removed: user-specified file name prefix

            // Optimized buffer settings for large data handling
            _effectiveFlushChars = 1048576; // 1MB buffer (increased from 327KB for better performance)
            _effectiveFlushIntervalSeconds = 10; // 10 seconds (increased from 5 for fewer I/O operations)
            _barBuffer = new StringBuilder(_effectiveFlushChars, _effectiveFlushChars * 2); // Set max capacity to 2MB

            OpenOutputFile();

            // Initialize last processed bar index
            _lastProcessedBarIndex = Bars.Count - 1;

            // Start a 1-second timer for periodic flushing
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

        protected override void OnBar()
        {
            // Process completed bars (OHLC data)
            ProcessCompletedBars();
        }

        protected override void OnTick()
        {
            // Check for new bars on each tick (backup to OnBar)
            if (Bars.Count > _lastProcessedBarIndex + 1)
            {
                ProcessCompletedBars();
            }
        }

        private void ProcessCompletedBars()
        {
            // Process all completed bars since last check
            int currentBarCount = Bars.Count;
            
            // Process all bars from last processed to current (excluding the current incomplete bar)
            for (int i = _lastProcessedBarIndex + 1; i < currentBarCount - 1; i++)
            {
                if (i < 0) continue;
                
                var instrument = SymbolName;
                var granularity = GetShortTimeFrame(TimeFrame);
                var barTimeUtc = GetUtc(Bars.OpenTimes[i]);
                
                // Track time range
                if (!_firstBarUtc.HasValue) _firstBarUtc = barTimeUtc;
                _lastBarUtc = barTimeUtc;
                
                // OHLC data
                var open = Bars.OpenPrices[i];
                var high = Bars.HighPrices[i];
                var low = Bars.LowPrices[i];
                var close = Bars.ClosePrices[i];
                var volume = Bars.TickVolumes[i];
                
                // Calculate spread from close price (approximation)
                var bid = close;
                var ask = close + (Symbol.Spread * Symbol.PipSize);
                var spreadPips = Symbol.Spread;
                
                var barTimeStr = barTimeUtc.ToString("yyyy-MM-dd HH:mm:ss");

                // Dynamic formatting based on symbol digits
                var priceFormat = "F" + Symbol.Digits;
                var spreadFormat = "F2";
                
                _barBuffer
                    .Append(barTimeStr).Append(',')
                    .Append(instrument).Append(',')
                    .Append(granularity).Append(',')
                    .Append(open.ToString(priceFormat)).Append(',')
                    .Append(high.ToString(priceFormat)).Append(',')
                    .Append(low.ToString(priceFormat)).Append(',')
                    .Append(close.ToString(priceFormat)).Append(',')
                    .Append(spreadPips.ToString(spreadFormat)).Append(',')
                    .Append(volume)
                    .Append("\r\n");
            }
            
            // Update last processed index to the last complete bar
            if (currentBarCount > 1)
            {
                _lastProcessedBarIndex = currentBarCount - 2;
            }

            // Flush conditions: size threshold OR time interval
            var serverNow = Server.Time;
            if (_barBuffer.Length >= _effectiveFlushChars ||
                (_effectiveFlushIntervalSeconds > 0 && (serverNow - _lastBarFlush).TotalSeconds >= _effectiveFlushIntervalSeconds))
                FlushBarBuffer();
        }

        protected override void OnTimer()
        {
            // Ensure periodic flush even during quiet markets
            if (_effectiveFlushIntervalSeconds > 0 &&
                (Server.Time - _lastBarFlush).TotalSeconds >= _effectiveFlushIntervalSeconds &&
                _barBuffer.Length > 0)
            {
                FlushBarBuffer();
            }
        }

        protected override void OnStop()
        {
            // Process any remaining bars
            ProcessCompletedBars();
            
            // Final flush of bar buffer
            FlushBarBuffer();
            
            // Drain queue if async
            if (AsyncWrites)
            {
                _writerRunning = false;
                _writerCts?.Cancel();
                try { _writerTask?.Wait(5000); } catch { }
            }
            // Ensure all pending writes complete before closing
            Thread.Sleep(100);
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

        // Flush buffered bar lines to disk - thread-safe
        private void FlushBarBuffer()
        {
            if (_barBuffer.Length == 0) return;
            
            // Prevent concurrent flush operations
            lock (_flushLock)
            {
                if (_barBuffer.Length == 0) return; // Double-check after acquiring lock
                
                try
                {
                    var snapshot = _barBuffer.ToString();
                    _barBuffer.Clear();

                    if (!_barCsvHeaderWritten)
                    {
                        // Column order: DateTimeUTC,Instrument,Granularity,Open,High,Low,Close,Spread(pips),Volume
                        snapshot = "DateTimeUTC,Instrument,Granularity,Open,High,Low,Close,Spread(pips),Volume\r\n" + snapshot;
                        _barCsvHeaderWritten = true;
                    }

                    if (AsyncWrites)
                    {
                        _writeQueue.Enqueue(snapshot);
                    }
                    else
                    {
                        WriteChunk(snapshot);
                    }
                    _lastBarFlush = Server.Time;
                }
                catch (Exception ex)
                {
                    Print($"Error flushing bar buffer: {ex.Message}");
                }
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
            var fileName = $"{prefix}_{provisionalStamp}_to_RUNNING_bars.csv"; // finalized later
            InitializeFile(fileName);
            _fileOpened = true;
        }

    // Convert server (broker) time to UTC using configured offset
    private DateTime GetUtc(DateTime serverTime) => serverTime - TimeSpan.FromHours(ServerTimeUtcOffsetHours);

        private void InitializeFile(string baseFileName)
        {
            // Use cTrader's LocalStorage path
            // LocalStorage provides methods to save/load data, but for direct file access we need the directory
            string baseFolder;
            
            // Get the Documents folder and construct cTrader's LocalStorage path
            var documents = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
            var localStorageRoot = Path.Combine(documents, "cAlgo", "LocalStorage", "HistoricalDataExtractor");
            
            if (string.IsNullOrWhiteSpace(OutputSubfolder))
            {
                // Default: Use robot's local storage directly
                baseFolder = localStorageRoot;
            }
            else
            {
                // User-specified subfolder within robot's local storage
                baseFolder = Path.Combine(localStorageRoot, OutputSubfolder);
            }
            
            try 
            { 
                Directory.CreateDirectory(baseFolder); 
                Print($"Output folder: {baseFolder}");
            } 
            catch (Exception ex) 
            { 
                Print($"Failed to create directory '{baseFolder}': {ex.Message}"); 
            }

            if (CompressionModeParam == CompressionModeOption.GZip)
                baseFileName += ".gz";

            _barCsvFilePath = Path.Combine(baseFolder, baseFileName);
            _outputFolder = baseFolder;
            var fileExists = File.Exists(_barCsvFilePath);
            _barCsvHeaderWritten = fileExists; // if exists assume header present
            try
            {
                if (CompressionModeParam == CompressionModeOption.GZip)
                {
                    // Create or open file for writing (Create overwrites existing file)
                    _fileStream = new FileStream(_barCsvFilePath, fileExists ? FileMode.Open : FileMode.Create, FileAccess.Write, FileShare.Read);
                    if (fileExists)
                    {
                        // If file exists, seek to end (but GZip should really be new each time)
                        _fileStream.Seek(0, SeekOrigin.End);
                    }
                    _gzipStream = new GZipStream(_fileStream, CompressionLevel.SmallestSize, leaveOpen: false);
                    _streamWriter = new StreamWriter(_gzipStream, Encoding.UTF8, 131072, leaveOpen: false); // 128KB StreamWriter buffer
                }
                else
                {
                    // For uncompressed files, use StreamWriter directly with larger buffer
                    _fileStream = new FileStream(_barCsvFilePath, fileExists ? FileMode.Append : FileMode.Create, FileAccess.Write, FileShare.Read, 131072); // 128KB FileStream buffer
                    _streamWriter = new StreamWriter(_fileStream, Encoding.UTF8, 131072, leaveOpen: false); // 128KB StreamWriter buffer
                }

                // Write header immediately for visibility (and avoid waiting for first flush)
                if (!_barCsvHeaderWritten)
                {
                    var header = "DateTimeUTC,Instrument,Granularity,Open,High,Low,Close,Spread(pips),Volume\r\n";
                    _streamWriter.Write(header);
                    _streamWriter.Flush();
                    _barCsvHeaderWritten = true;
                }
            }
            catch (Exception ex)
            {
                Print($"Failed to open file '{_barCsvFilePath}': {ex.Message}");
            }
        }

        private void CloseCurrentStreams()
        {
            try
            {
                // Flush and close StreamWriter first (will cascade to GZipStream and FileStream)
                _streamWriter?.Flush();
                _streamWriter?.Close();
                _streamWriter?.Dispose();
            }
            catch (Exception ex)
            {
                Print($"Error closing StreamWriter: {ex.Message}");
            }
            _streamWriter = null;
            
            try
            {
                _gzipStream?.Flush();
                _gzipStream?.Close();
                _gzipStream?.Dispose();
            }
            catch (Exception ex)
            {
                Print($"Error closing GZipStream: {ex.Message}");
            }
            _gzipStream = null;
            
            try
            {
                _fileStream?.Flush();
                _fileStream?.Close();
                _fileStream?.Dispose();
            }
            catch (Exception ex)
            {
                Print($"Error closing FileStream: {ex.Message}");
            }
            _fileStream = null;
        }

        private void WriteChunk(string chunk)
        {
            if (string.IsNullOrEmpty(chunk)) return;
            try
            {
                if (_streamWriter != null)
                {
                    _streamWriter.Write(chunk);
                    _streamWriter.Flush();
                    if (CompressionModeParam == CompressionModeOption.GZip)
                    {
                        // Also flush underlying streams for GZip
                        _gzipStream?.Flush();
                    }
                    _fileStream?.Flush();
                }
                else
                {
                    // Fallback if streams are not initialized
                    File.AppendAllText(_barCsvFilePath, chunk, Encoding.UTF8);
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
                    // Use shorter sleep for more responsive queue processing
                    Thread.Sleep(5);
                }
            }
            // Drain remaining queue after cancellation with progress feedback
            int remainingCount = 0;
            while (_writeQueue.TryDequeue(out var remaining))
            {
                WriteChunk(remaining);
                remainingCount++;
            }
            if (remainingCount > 0)
            {
                Print($"Flushed {remainingCount} remaining data chunks on shutdown");
            }
        }

        private void TryFinalizeFileName()
        {
            // No longer skip renaming for custom file name (option removed)
            if (string.IsNullOrEmpty(_barCsvFilePath) || !_firstBarUtc.HasValue || _lastBarUtc == DateTime.MinValue)
                return;
            try
            {
                var instrument = SymbolName;
                var granularity = GetShortTimeFrame(TimeFrame);
                var first = _firstBarUtc.Value;
                var last = _lastBarUtc;
                var prefix = $"{instrument}_{granularity}";
                var newName = string.Format("{0}_{1}_to_{2}_bars.csv", prefix,
                    first.ToString("yyyyMMdd_HHmmss"), last.ToString("yyyyMMdd_HHmmss"));
                if (CompressionModeParam == CompressionModeOption.GZip) newName += ".gz";
                var finalPath = Path.Combine(_outputFolder ?? Path.GetDirectoryName(_barCsvFilePath)!, newName);
                if (File.Exists(finalPath))
                {
                    int i = 1;
                    string candidate;
                    do
                    {
                        candidate = Path.Combine(_outputFolder ?? Path.GetDirectoryName(_barCsvFilePath)!,
                            Path.GetFileNameWithoutExtension(newName) + "_" + i + Path.GetExtension(newName));
                        i++;
                    } while (File.Exists(candidate) && i < 1000);
                    finalPath = candidate;
                }
                if (!string.Equals(finalPath, _barCsvFilePath, StringComparison.OrdinalIgnoreCase))
                {
                    File.Move(_barCsvFilePath, finalPath, overwrite: false);
                    _barCsvFilePath = finalPath;
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