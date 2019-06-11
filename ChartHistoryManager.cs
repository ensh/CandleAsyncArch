using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Core.Classes;
using Core.DataAggregation;
using AD.Common.DataStructures;
using AD.Common.Helpers;

namespace Core
{

    using HistoryInfo = HistoryCompletitionManager.HistoryInfo;
    using HistoryCall = KeyValuePair<AggregatorKey, Action<AggregatedItem>>;

    public delegate void DataRecievedHandler(ChartData data, int where);

    public interface IHistoryConsumer : IDisposable
    {
        int RequestId { get; set; }
        void OnDataReady(AggregatorKey key, DateTime from, DateTime to, int reqiredDays = 0);
        void OnProgress(ShortDate day, int count);
        string Name { get; }
    }

    [DebuggerDisplay("Interval = {ToString(),nq}")]
    public struct RequestInterval : IEquatable<RequestInterval>
    {
        public readonly ShortDate? Last;
        public readonly int DaysCount;

        public RequestInterval(int daysCount)
        {
            if (daysCount > 0)
                throw new ArgumentOutOfRangeException("daysCount mast be small zero!");
            DaysCount = daysCount;
            Last = null;
        }

        public RequestInterval(ShortDate from, ShortDate last)
            : this(last, from - last)
        {
        }

        public RequestInterval(ShortDate last, int daysCount)
        {
            if (daysCount > 0)
                throw new ArgumentOutOfRangeException("daysCount mast be small zero!");
            Last = last;
            DaysCount = daysCount;
        }

        public bool HasValue
        {
            get
            {
                return Last.HasValue;
            }
        }

        public ShortDate From
        {
            get
            {
                return Last.Value + DaysCount;
            }
        }

        public ShortDate To
        {
            get
            {
                return (Last.HasValue) ? Last.Value : ushort.MaxValue;
            }
        }

        public KeyValuePair<ShortDate, bool> FromKey
        {
            get { return new KeyValuePair<ShortDate, bool>(From, true); }
        }

        public KeyValuePair<ShortDate, bool> ToKey
        {
            get { return new KeyValuePair<ShortDate, bool>(To, false); }
        }

        public static void Normalize(ref DateTime? date, ref int daysCount)
        {
            if (date.HasValue)
            {
                if (daysCount > 0)
                {
                    date = date.Value.AddDays(daysCount);
                    daysCount = -daysCount;
                }
            }
        }

        public override string ToString()
        {
            if (HasValue)
                return String.Concat(((DateTime)From).ToShortDateString(), "-", ((DateTime)To).ToShortDateString());
            return String.Concat("Last: null, Days: ", DaysCount.ToString());
        }

        public override int GetHashCode()
        {
            return -DaysCount ^ ((Last.HasValue) ? Last.Value.GetHashCode() : 0);
        }

        public override bool Equals(object obj)
        {
            return Equals((RequestInterval)obj);
        }

        public bool Equals(RequestInterval other)
        {
            return (other.DaysCount == DaysCount) && ((!HasValue && !other.HasValue) || (HasValue && other.HasValue && Last.Value.Equals(other.Last.Value)));
        }
    }

    public class IntervalList : SortedSet<KeyValuePair<ShortDate, bool>>
    {
        internal class PointComparer : IComparer<KeyValuePair<ShortDate, bool>>
        {
            public int Compare(KeyValuePair<ShortDate, bool> x, KeyValuePair<ShortDate, bool> y)
            {
                return (int)x.Key.CompareTo(y.Key);
            }
        }

        public IntervalList() : base(new PointComparer())
        {
            LastDate = null;
        }

        ShortDate? lastDate;
        public ShortDate? LastDate { get { return lastDate; } set { lastDate = value; } }
        public bool HasValue { get { return LastDate.HasValue; } }

        public IEnumerable<RequestInterval> Intervals
        {
            get
            {
                using (var pe = GetEnumerator())
                {
                    if (pe.MoveNext() && pe.Current.Value)
                    {
                        var prev = pe.Current;
                        while (pe.MoveNext())
                        {
                            if (!pe.Current.Value)
                                yield return new RequestInterval(prev.Key, pe.Current.Key);
                            prev = pe.Current;
                        }
                    }
                }
            }
        }

        public override string ToString()
        {
            lock (this)
            {
                return String.Join(";", Intervals);
            }
        }

        public IEnumerable<RequestInterval> Get(RequestInterval interval)
        {
            if (!HasValue && !interval.HasValue)
                yield break;

            // обрезаем интервал или знаем дату окончания
            if (LastDate.HasValue)
            {
                if (!interval.HasValue)
                    interval = new RequestInterval(LastDate.Value, interval.DaysCount);
                else
                {
                    if (interval.To > LastDate.Value)
                    {
                        if (interval.From < LastDate.Value)
                            interval = new RequestInterval(interval.From, LastDate.Value);
                        else
                            yield break;
                    }
                }
            }

            SortedSet<KeyValuePair<ShortDate, bool>> points = GetViewBetween(interval.FromKey, interval.ToKey);

            using (var pe = ((IEnumerable<KeyValuePair<ShortDate, bool>>)points).GetEnumerator())
            {
                if (pe.MoveNext())
                {
                    var prev = pe.Current;
                    if (prev.Value && prev.Key > interval.From)
                        yield return new RequestInterval(interval.FromKey.Key, prev.Key);

                    while (pe.MoveNext())
                    {
                        if (!prev.Value)
                            yield return new RequestInterval(prev.Key, pe.Current.Key);
                        prev = pe.Current;
                    }

                    if (!prev.Value && prev.Key < interval.To)
                        yield return new RequestInterval(prev.Key, interval.To);
                }
                else
                {
                    if (Count > 0 && Min.Key <= interval.From && Max.Key >= interval.To)
                        yield break;

                    yield return interval;
                }
            }
        }

        public bool Add(RequestInterval interval)
        {
            if (!interval.HasValue)
                return false;

            var points = GetViewBetween(interval.FromKey, interval.ToKey);
            ShortDate? begin = null, end = null;
            var toRemove = points.ToArray();

            switch (toRemove.Length)
            {
                case 0:
                    if (Count > 0 && Min.Key <= interval.From && Max.Key >= interval.To)
                        return false;
                    begin = interval.From;
                    end = interval.To;
                    break;
                default:
                    var prev = toRemove[0];
                    if (prev.Value)
                        begin = interval.FromKey.Key;

                    prev = toRemove[toRemove.Length - 1];
                    if (!prev.Value)
                        end = interval.ToKey.Key;

                    break;
            }

            for (int i = 0; i < toRemove.Length; i++)
                Remove(toRemove[i]);

            if (begin.HasValue)
                Add(new KeyValuePair<ShortDate, bool>(begin.Value, true));

            if (end.HasValue)
            {
                Add(new KeyValuePair<ShortDate, bool>(end.Value, false));

                if (!HasValue || LastDate.Value.CompareTo(end.Value) < 0)
                    LastDate = end;
            }

            return true;
        }
    }

    public class HistoryConsumerJoin<TControl> where TControl : class
    {
        int reqired;
        int registred;
        long requestId;
        public long RequestId
        {
            get { return Interlocked.Read(ref requestId); }
            protected set
            {
                long id = Interlocked.Read(ref requestId);
                while (id < value && id != Interlocked.CompareExchange(ref requestId, value, id))
                    id = Interlocked.Read(ref requestId);
            }
        }

        object @lock;
        public HistoryConsumerJoin()
        {
            @lock = new object();
            requestId = 0;
            reqired = registred = 0;
            Update = null;
        }

        public void AddRef()
        {
            lock (@lock)
            {
                reqired++;
            }
        }
        Action<int, TControl> Update;
        public void Release(int requestId, TControl control) 
        {
            lock (@lock)
            {
                RequestId = requestId;
                if (reqired > 0)
                {
                    registred += reqired;
                    reqired = 0;
                }

                if (--registred == 0 && Update != null && control != null)
                {
                    Update((int)RequestId, control);
                }
            }
        }

        public void Start(Action<int, TControl> update, TControl control)
        {
            lock (@lock)
            {
                if ((registred += reqired) == 0)
                {
                    if (update != null && control != null)
                        update((int)RequestId, control);                        
                }
                else
                    Update = update;
                reqired = 0;
            }
        }
    }

    public class HistoryManager : IDisposable
    {
        public static HistoryManager Instance { get { return instance; } }
        static HistoryManager instance;
        static HistoryManager()
        {
            instance = new HistoryManager();
        }

        HistoryManager()
        {
            historyId = 0;
            activeRequests = new ConcurrentDictionary<int, KeyValuePair<ArchiveRequestEntity, RequestProcessor>>();
            activeIntervals = new ConcurrentDictionary<AggregatorKey, IntervalList>();
            candlesProvider = new HistoryDataProvider(new Core.Arch.HistoryStorage(ApplicationPaths.DataBase+"_hd.db"));

            ConnectionManager.Instance.ConnectionStatusChanged += ConnectionStatusChanged;

            SaveOffLineHistory();            
        }

        ~HistoryManager()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            candlesProvider.Dispose();
        }

        int historyId;
        public int NewRequestId
        {
            get
            {
                return Interlocked.Increment(ref historyId);
            }
        }

        public static DateTime ServerTime
        {
            get
            {
                if (IsOnline)
                    return ConnectionManager.Instance.GetServerTime();
                return DateTime.Now;
            }
        }

        public static bool IsOnline
        {
            get
            {
                return ConnectionManager.Instance.GetConnectionStatus(FrontEndType.BirzArchAndMediaServer) == ConnectionStatus.Authorized;
            }
        }

        public void AggregatorRegistration(AggregatedItem aggregatedItem, out ShortDate? lastDate)
        {
            // регистрация агрегатора
            HistoryCompletitionManager.Instance.RegisterHistory(aggregatedItem);
            IntervalList intervals = null;
            activeIntervals.GetOrAdd(aggregatedItem.Key, k => (intervals = GetOffLineIntervals(k)));
            lastDate = (intervals != null) ? intervals.LastDate : null;
            GetHistoryAsyncImpl(aggregatedItem, null, -1);
        }

        public IEnumerable<RequestInterval> PrepareRequest(AggregatedItem aggregatedItem, DateTime? date, int days, IHistoryConsumer consumer = null)
        {
            var key = aggregatedItem.Key;
            RequestInterval.Normalize(ref date, ref days);
            // для секундных графиков всегда запрашивается один день
            if (aggregatedItem.Key.TimeFrame == BaseTimeFrame.Second && date.HasValue)
                date = date.Value.AddDays(days);

            RequestInterval newInterval = (date.HasValue) ? new RequestInterval(date.Value, days) : new RequestInterval(days);

            var intervals = activeIntervals.GetOrAdd(key, _ => new IntervalList());

            lock (intervals)
            {
                if (intervals.Count == 0) { }
                else
                {
                    using (var ie = intervals.Get(newInterval).GetEnumerator())
                    {
                        if (ie.MoveNext())
                        {
                            // новый интервал. нужен запрос
                            do
                            {
                                yield return ie.Current;
                            } while (ie.MoveNext());
                        }
                        else
                        {
                            LoadOffLineHistory(aggregatedItem, date, days, consumer);
                        }
                        yield break;
                    }
                }
            }

            yield return newInterval;
        }

        public void GetHistoryAsync(AggregatedItem aggregatedItem, DateTime from, DateTime to, IHistoryConsumer consumer = null)
        {
            var days = (int)from.Subtract(to).TotalDays - 1;
            GetHistoryAsync(aggregatedItem, to, days, consumer);
        }

        public void GetHistoryAsync(AggregatedItem aggregatedItem, DateTime? from, int days, IHistoryConsumer consumer = null)
        {
            HistoryCompletitionManager.Instance.Post(
                new HistoryInfo(10,
                    new HistoryCall(default(AggregatorKey), _ => GetHistoryAsyncImpl(aggregatedItem, from, days, consumer))));
        }

        // для вызова из ContinuationDateConsumer
        void GetHistoryAsync(AggregatorKey key, DateTime? from, int days, IHistoryConsumer consumer = null)
        {
            HistoryCompletitionManager.Instance.Post(
                new HistoryInfo(10,
                    new HistoryCall(key, a => GetHistory(a, from, days, consumer))));
        }

        public void GetHistoryAsyncImpl(AggregatedItem aggregatedItem, DateTime? from, int days, IHistoryConsumer consumer = null)
        {
            var key = aggregatedItem.Key;

            using (var intervals = PrepareRequest(aggregatedItem, from, days, consumer).GetEnumerator())
            {
                if (intervals.MoveNext())
                {
                    Action<int, IHistoryConsumer> dataReady = null;
                    if (consumer != null)
                    { 
                        // формируем обрабочик для информирования подписчика данных
                        if (from.HasValue)
                            dataReady = (days < 0) ? (Action<int, IHistoryConsumer>)
                                ((i, c) => c.OnDataReady(key, from.Value.AddDays(days), from.Value)) :
                                ((i, c) => c.OnDataReady(key, from.Value, from.Value.AddDays(days)));
                        else
                            dataReady = (i, c) => c.OnDataReady(key, DateTime.MinValue, DateTime.MinValue);
                    }

                    HistoryConsumerJoin<IHistoryConsumer> consumerJoin = new HistoryConsumerJoin<IHistoryConsumer>();
                    do
                    {
                        if (intervals.Current.DaysCount == 0)
                            continue;

                        if (IsOnline)
                        {
                            //LogFileManager.AddInfo(String.Concat("consumer: ", (consumer != null)? consumer.Name : "null", "\tinterval: ", intervals.Current.ToString()), "hd");
                            var contConsumer = new ContinuationDateConsumer(this, intervals.Current, consumer);
                            consumerJoin.AddRef();
                            contConsumer.Start(key, rqId => consumerJoin.Release(rqId, consumer));
                        }
                        else
                        {
                            LoadOffLineHistory(aggregatedItem, from, days, consumer);

                            return;
                        }
                    } while (intervals.MoveNext());

                    consumerJoin.Start(dataReady, consumer);
                }
            }
        }

        public IntervalList GetOffLineIntervals(AggregatorKey key)
        {
            var result = new IntervalList();

            using (var dates = candlesProvider.GetIndex(key).Select(d => (DateTime)d).GetEnumerator())
            {
                if (dates.MoveNext())
                {
                    var dt = dates.Current;
                    var nextdt = dt.AddDays(1);
                    while (dates.MoveNext())
                    {
                        if (dates.Current != nextdt)
                        {                            
                            result.Add(new RequestInterval(nextdt, (int)dt.Subtract(nextdt).TotalDays));
                            dt = dates.Current;
                        }
                        nextdt = dates.Current.AddDays(1);
                    }

                    result.Add(new RequestInterval(nextdt, (int)dt.Subtract(nextdt).TotalDays));
                }
            }

            if (AppConfig.Common.IntervalsLogEnabled)
                LogFileManager.AddInfo(String.Concat("GetOffLineIntervals", Environment.NewLine, "key: ", key.ToString(), "\tresult", result.ToString()), "ints");

            return result;
        }

        public void SaveOffLineHistory()
        {            
            try
            {
                foreach (var c in CachedDate.LastChange)
                {
                    CachedDate cachedDate = c.Key;
                    CachedDate.LastChange.TryRemove(cachedDate, out var cd);

                    var key = cachedDate.Key;
                    var prefix = "|write" + key.ToString();
                    byte[] data = null;
                    using (var l = cachedDate.ReadLocker)
                    {
                        if (cachedDate.Candles.Count > 0)
                        {                            
                            using (MemoryStream ms = new MemoryStream(512))
                            {
                                BinaryWriterNullable bwriter = new BinaryWriterNullable(ms);
                                bwriter.Write(cachedDate.Candles.Count);
                                bwriter.Write(cachedDate.FullDay);
                                
                                switch (key.DataPointEnum)
                                {
                                    case DataPointsEnum.DpOhlcv:
                                        foreach (var x in cachedDate.Candles.Values.OfType<OhlcvDataPointEntity>())
                                        {                                            
                                            x.Serialize(bwriter, 0);
                                            if (AppConfig.Common.HDataLogEnabled)
                                                LogFileManager.AddInfo(String.Concat(prefix, x.ToString()), "hdata");
                                        }
                                        break;
                                    case DataPointsEnum.DpMpv:
                                        foreach (var x in cachedDate.Candles.Values.OfType<MpvDataPointEntity>())
                                        {
                                            x.Serialize(bwriter, 0);
                                            if (AppConfig.Common.HDataLogEnabled)
                                                LogFileManager.AddInfo(String.Concat(prefix, x.ToString()), "hdata");
                                        }
                                        break;
                                }

                                data = ms.ToArray();
                            }                           
                        }
                        else
                            data = new byte[0];
                    }

                    if (data != null)
                        candlesProvider.TryWrite(key, cachedDate.Date, new KeyValuePair<bool, byte[]>(cachedDate.FullDay, data));
                }
            }
            catch (Exception e)
            {
                LogFileManager.AddError("SaveOffLineHistory exception", e);
            }

            HistoryCompletitionManager.Instance.Post(
                new HistoryInfo(AppConfig.Common.ChartSaveDelay,
                    new HistoryCall(default(AggregatorKey), _ => SaveOffLineHistory())));
        }

        public void LoadOffLineHistory(AggregatedItem aggregatedItem, DateTime? date, int days, IHistoryConsumer consumer = null)
        {
            try
            {
                int dayCounter = 0;
                TimeDirection dir = (TimeDirection)Math.Sign(days);
                days = Math.Abs(days);
                days += days >> 2; // умножаем на 1.25 из-за выходных дней
                DateTime? from = null, to = null;

                if (TimeDirection.Forward == dir)
                {
                    candlesProvider.ScanIndex(aggregatedItem.Key,
                    dt =>
                    {
                        if (!from.HasValue)
                            from = dt;
                        to = dt;
                        aggregatedItem.GetOrAddCachedDay(dt, true);

                        return ++dayCounter <= days;
                    },
                    date, dir);
                }
                else
                {
                    candlesProvider.ScanIndex(aggregatedItem.Key,
                        dt =>
                        {
                            if (!to.HasValue)
                                to = dt;
                            from = dt;
                            aggregatedItem.GetOrAddCachedDay(dt, true);

                            return ++dayCounter <= days;
                        },
                        date, dir);
                }

                if (consumer != null)
                {
                    if (dayCounter > 0)
                    {
                        if (from.HasValue && to.HasValue)
                            consumer.OnDataReady(aggregatedItem.Key, from.Value, to.Value);
                        else
                            // ничего нет, прогружаем пустоту
                            consumer.OnDataReady(aggregatedItem.Key, DateTime.MinValue, DateTime.MinValue);
                    }
                    else
                        // ничего нет, прогружаем пустоту
                        consumer.OnDataReady(aggregatedItem.Key, DateTime.MinValue, DateTime.MinValue);
                }
            }
            catch (Exception e)
            {
                LogFileManager.AddError("GetOffLineHistory exception", e);
            }
        }

        public void GetLastHistory(AggregatorKey key)
        {
            // только для текущего дня PrepareRequest уже не нужен
            HistoryCompletitionManager.Instance.Post(
                new HistoryInfo(10,
                    new HistoryCall(key, a => GetHistory(a, null, -1))));
        }

        // без PrepareRequest не вызывать!!!
        public void GetHistory(AggregatedItem aggregatedItem, DateTime? date, int days, IHistoryConsumer consumer = null)
        {
            if (aggregatedItem == null)
                return;

            var requestProcessor = new RequestProcessor(this, consumer);
            var request = new ArchiveRequestEntity()
            {
                IdRequest = requestProcessor.RequestId,
                IdFi = aggregatedItem.iID,
                TimeFrame = aggregatedItem.Timeframe.ToBaseTimeFrame(),
                CandleType = aggregatedItem.Key.CandleType,
                DaysCount = days,
                MaximumDate = (date.HasValue) ? date.Value : DateTime.MaxValue,
            };

            activeRequests.TryAdd(requestProcessor.RequestId,
                new KeyValuePair<ArchiveRequestEntity, RequestProcessor>(request, requestProcessor));

            LogFileManager.AddInfo(String.Concat("RequestId: ", request.IdRequest.ToString(),
                "\tIdFI: ", request.IdFi,
                "\tTimeFrame: ", request.TimeFrame.ToString(),
                "\tCandleType: ", request.CandleType.ToString(),
                "\tDaysCount: ", request.DaysCount.ToString(),
                "\tMaximumDate: ", request.MaximumDate.ToShortDateString(),
                "\tName: ", (consumer != null) ? consumer.Name : "-", "."), "requests");

            request.Created = ServerTime;

            // запрашиваем сервер
            ConnectionManager.Instance.SendPacket(request,
                FrontEndType.BirzArchAndMediaServer);
        }

        public void CheckHistoryAsync(AggregatedItem aggregatedItem, KeyValuePair<ShortDate, long>[] dateHash)
        {
            HistoryCompletitionManager.Instance.Post(
                new HistoryInfo(10,
                    new HistoryCall(default(AggregatorKey), _ => CheckHistoryAsyncImpl(aggregatedItem, dateHash))));
        }

        // для вызова из ContinuationDateConsumer
        void CheckHistoryAsync(AggregatorKey key, List<IntervalInfoEntity> dateHash, IHistoryConsumer consumer)
        {
            HistoryCompletitionManager.Instance.Post(
                new HistoryInfo(10,
                    new HistoryCall(key, a => CheckHistory(a, dateHash, consumer))));
        }

        public void CheckHistoryAsyncImpl(AggregatedItem aggregatedItem, KeyValuePair<ShortDate, long>[] dateHash)
        {
            var consumer = new CheckDateConsumer(this, aggregatedItem.Key, dateHash);
            if (IsOnline)
                consumer.Start(dateHash.Length + 1);
            else
                consumer.Dispose();
        }

        // для вызова из ChechDataConsumer
        public void CheckHistory(AggregatedItem aggregatedItem, List<IntervalInfoEntity> dateHash, IHistoryConsumer consumer)
        {
            var requestProcessor = new RequestProcessor(this, consumer);
            var request = new ArchiveRequestEntity()
            {
                IdRequest = requestProcessor.RequestId,
                IdFi = aggregatedItem.iID,
                TimeFrame = aggregatedItem.Timeframe.ToBaseTimeFrame(),
                CandleType = aggregatedItem.Key.CandleType,
                DaysCount = -dateHash.Count,
                MaximumDate = dateHash.First().Date,
                IntervalInfo = dateHash.Where(i => i.Hash > 0).ToList(),
            };

            activeRequests.TryAdd(requestProcessor.RequestId,
                new KeyValuePair<ArchiveRequestEntity, RequestProcessor>(request, requestProcessor));

            LogFileManager.AddInfo(String.Concat("RequestId: ", request.IdRequest.ToString(),
                "\tIdFI: ", request.IdFi,
                "\tTimeFrame: ", request.TimeFrame.ToString(),
                "\tCandleType: ", request.CandleType.ToString(),
                "\tDaysCount: ", request.DaysCount.ToString(),
                "\tMaximumDate: ", request.MaximumDate.ToShortDateString(),
                "\tName: ", consumer.Name, "."), "requests");

            request.Created = ServerTime;

            // запрашиваем сервер
            ConnectionManager.Instance.SendPacket(request,
                FrontEndType.BirzArchAndMediaServer);
        }

        DataPointsEnum ToEnum(CandleType candleType)
        {
            switch (candleType)
            {
                case CandleType.MPV:
                    return DataPointsEnum.DpMpv;
                case CandleType.Standard:
                    return DataPointsEnum.DpOhlcv;
            }
            return DataPointsEnum.DpSimple;
        }

        public void DoRequest(int requestId)
        {
            KeyValuePair<ArchiveRequestEntity, RequestProcessor> request;
            if (activeRequests.TryGetValue(requestId, out request))
            {
                // не все подписчики еще получили данные, в очередь
                var key = new AggregatorKey(ToEnum(request.Key.CandleType), request.Key.TimeFrame, request.Key.IdFi);
                HistoryCompletitionManager.Instance.Post(
                    new HistoryInfo(10,
                    new HistoryCall(key, a => request.Value.Process(a))));
            }
        }

        public bool IsActiveRequest(int requestId)
        {
            return activeRequests.ContainsKey(requestId);
        }

        public void DoneRequest(AggregatorKey key, int requestId, ShortDate? from, ShortDate? to)
        {
            KeyValuePair<ArchiveRequestEntity, RequestProcessor> request;
            if (activeRequests.TryRemove(requestId, out request))
            {
                // проверка на частичный запрос
                if (requestId != request.Value.RequestId)
                {
                    if (request.Value.Release())
                    {
                        // все вложенные запросы выполнились - преход к обобщенному запросу
                        if (!activeRequests.TryRemove(request.Value.RequestId, out request))
                            return; // мало-ли...
                    }
                    else
                        return;
                }

                // обновить потребитель - данные готовы полностью
                if (request.Value.Consumer != null)
                {
                    // проверяем, что запрос был не на синхронизацию данных
                    if (request.Key.IntervalInfo == null || request.Key.IntervalInfo.Count == 0)
                    {
                        // запрос новых данных
                        if (from.HasValue && to.HasValue)
                        {
                            request.Value.Consumer.OnDataReady(key, from.Value, to.Value, request.Key.DaysCount);
                        }
                        else // from и to всегда инициализируются вместе!
                        {
                            // дошли до конца доступных данных, пишем в интервал заведомо большое число
                            IntervalList intervals;
                            if (activeIntervals.TryGetValue(key, out intervals))
                            {
                                lock (intervals)
                                {
                                    int days = (ShortDate)0 - intervals.Min.Key;
                                    intervals.Add(new RequestInterval(intervals.Min.Key, days)); // обновить интервал получения даты

                                    if (AppConfig.Common.IntervalsLogEnabled)
                                        LogFileManager.AddInfo(String.Concat("DoneRequest", Environment.NewLine, 
                                            "key: ", key.ToString(), "\trequest: ", requestId.ToString(), "\tintervals: ", intervals.ToString()), "ints");
                                }

                                request.Value.Consumer.OnDataReady(key, request.Key.MaximumDate.AddDays(request.Key.DaysCount), request.Key.MaximumDate, request.Key.DaysCount);
                            }
                        }
                    }
                    else
                    {
                        // синхронизация данных, DaysCount < 0!, нужно пометить список как синхронизированный
                        request.Value.Consumer.OnDataReady(key, request.Key.MaximumDate.AddDays(request.Key.DaysCount), request.Key.MaximumDate, request.Key.DaysCount);
                    }
                    
                    request.Value.Consumer.Dispose();
                }
            }
        }

        public void FreeCheck(AggregatorKey key, IEnumerable<ShortDate> uncheckedDates)
        {
            HistoryCompletitionManager.Instance.Post(
                new HistoryInfo(10,
                    new HistoryCall(key, a => FreeCheckImpl(a, uncheckedDates))));
        }

        public void FreeCheckImpl(AggregatedItem aggregatedItem, IEnumerable<ShortDate> uncheckedDates)
        {
            foreach (var cachedDate in uncheckedDates
                .Select(dt => aggregatedItem.GetCachedDay(dt, false))
                .OfType<CachedDate>())
            {
                CachedDate.LastLoaded.TryAdd(cachedDate, true);
            }
        }

        // запрошенные с сервера данные приходят сюда
        public void OnChartData(IEnumerable<IADSerializable> archive)
        {
            using (var archiveEntities = archive.OfType<ChartArchiveEntity>().GetEnumerator())
            {
                if (archiveEntities.MoveNext())
                {
                    KeyValuePair<ArchiveRequestEntity, RequestProcessor> requestProcessor;
                    if (!activeRequests.TryGetValue(archiveEntities.Current.IdRequest, out requestProcessor))
                        return;

                    requestProcessor.Value.Append(archiveEntities.Current);

                    while (archiveEntities.MoveNext())
                        requestProcessor.Value.Append(archiveEntities.Current);
                }
            }
        }

        public void SendLastDateRequest(AggregatorKey aggregatorKey)
        {
            var requestProcessor = new RequestProcessor(this,
                new ContinuationDateConsumer(this, new RequestInterval(-1)));
            var request = new ArchiveRequestEntity()
            {
                IdRequest = requestProcessor.RequestId,
                IdFi = aggregatorKey.IdFI,
                TimeFrame = aggregatorKey.TimeFrame,
                CandleType = aggregatorKey.CandleType,
                DaysCount = -1,
                MaximumDate = DateTime.MaxValue,
                Created = ServerTime,
            };

            activeRequests.TryAdd(requestProcessor.RequestId,
                new KeyValuePair<ArchiveRequestEntity, RequestProcessor>(request, requestProcessor));

            LogFileManager.AddInfo(String.Concat("RequestId: ", request.IdRequest.ToString(),
                "\tIdFI: ", request.IdFi,
                "\tTimeFrame: ", request.TimeFrame.ToString(),
                "\tCandleType: ", request.CandleType.ToString(),
                "\tDaysCount: ", request.DaysCount.ToString(),
                "\tMaximumDate: ", request.MaximumDate.ToShortDateString(),
                "\tName: getDateOnConnect"), "requests");

            ConnectionManager.Instance.SendPacket(request,
                FrontEndType.BirzArchAndMediaServer);
        }

        private void ConnectionStatusChanged(FrontEndType type, ConnectionStatus status)
        {
            switch (type)
            {
                case FrontEndType.BirzArchAndMediaServer:
                    {
                        switch (status)
                        {
                            case ConnectionStatus.Authorized:
                                {
                                    // отправляем запрос для определения последней даты графика, с блокировкой списка
                                    foreach (var aggregatorKey in activeIntervals.Keys)
                                        SendLastDateRequest(aggregatorKey);
                                    break;
                                }
                            case ConnectionStatus.Disconnected:
                                {
                                    foreach (var r in activeRequests.Values)
                                    {
                                        if (r.Value.Consumer != null)
                                        {
                                            r.Value.Consumer.OnDataReady(default(AggregatorKey), DateTime.MinValue, DateTime.MaxValue, r.Key.DaysCount);
                                            r.Value.Consumer.Dispose();
                                        }
                                    }
                                    activeRequests.Clear();
                                    break;
                                }
                        }
                        break;
                    }
            }
        }

        ConcurrentDictionary<int, KeyValuePair<ArchiveRequestEntity, RequestProcessor>> activeRequests;
        ConcurrentDictionary<AggregatorKey, IntervalList> activeIntervals;

        private readonly HistoryDataProvider candlesProvider;
        public KeyValuePair<bool, IEnumerable<DataPoint>> ReadOneDay(AggregatorKey key, DateTime day)
        {
            try
            {
                var data = candlesProvider.TryRead(key, day);
                var full = data.Key;
                if (data.Value != null)
                {
                    var result = new LinkedList<DataPoint>();

                    if (data.Value.Length > 0)
                    {
                        using (MemoryStream ms = new MemoryStream(data.Value))
                        {
                            BinaryReaderNullable breader = new BinaryReaderNullable(ms);
                            int cnt = breader.ReadInt32();
                            if (cnt > 0)
                            {
                                full = breader.ReadBoolean();
                                var prefix = "|read" + key.ToString();
                                for (int i = 0; i < cnt; i++)
                                {                                    
                                    switch (key.CandleType)
                                    {
                                        case CandleType.Standard:
                                            {
                                                var p = new OhlcvDataPointEntity();
                                                p.Deserialize(breader, 0);

                                                if (AppConfig.Common.HDataLogEnabled)
                                                    LogFileManager.AddInfo(String.Concat(prefix, p.ToString()), "hdata");

                                                if (p.Identity > 0)
                                                    result.AddLast(p);
                                                break;
                                            }
                                        case CandleType.MPV:
                                            {
                                                var p = new MpvDataPointEntity();
                                                p.Deserialize(breader, 0);

                                                if (AppConfig.Common.HDataLogEnabled)
                                                    LogFileManager.AddInfo(String.Concat(prefix, p.ToString()), "hdata");

                                                if (p.Identity > 0)
                                                    result.AddLast(p);
                                                break;
                                            }
                                    }
                                }
                            }
                        }
                    }

                    return new KeyValuePair<bool, IEnumerable<DataPoint>>(full, result);
                }
            }
            catch (Exception ex)
            {
                LogFileManager.AddError(String.Concat("HistoryManager.ReadOneDay exception Key: ", key.ToString(), "\tDay: ", day.ToString(), Environment.NewLine) , ex);
            }

            return new KeyValuePair<bool, IEnumerable<DataPoint>>(false, Enumerable.Empty<DataPoint>());
        }

        public void ClearHistory()
        {
            candlesProvider.Clear(ApplicationPaths.DataBase + "_hd.db");
        }

        internal class ContinuationDateConsumer : IHistoryConsumer
        {
            HistoryManager Owner;
            IHistoryConsumer Parent;
            RequestInterval Interval;

            public ContinuationDateConsumer(HistoryManager owner, RequestInterval interval, IHistoryConsumer parent = null)
            {
                requestId = 0;
                daysCount = 0;
                Owner = owner;
                Parent = parent;
                Interval = interval;
            }

            int requestId;
            public int RequestId
            {
                get
                {
                    return requestId;
                }
                set
                {
                    requestId = value;
                    if (Parent != null)
                        Parent.RequestId = value;
                }
            }

            volatile Action<int> DataReady;
            public void Start(AggregatorKey key, Action<int> dataReady)
            {
                DataReady = dataReady;
                // нужен дозапрос, с ограничением числа дней
                var days = Math.Max(Interval.DaysCount, -DataAggregator.DaysFromBase4Period(key.DataPointEnum, (int)key.TimeFrame.ToBaseFrameTypes()));
                Owner.GetHistoryAsync(key, Interval.Last, days, this);
            }

            int daysCount;
            public void OnDataReady(AggregatorKey key, DateTime from, DateTime to, int reqiredDays = 0)
            {
                //LogFileManager.AddInfo(
                //    String.Concat("key: ", key.ToString(), "\tfrom: ", from.ToString(), "\tto: ", to.ToString(), "\trqd: ", reqiredDays.ToString()), "hd");

                if (reqiredDays != 0) // получены данные, а не пустой ответ
                {
                    // не знали начало
                    if (!Interval.HasValue)
                    {
                        // зафиксировать первую дату запроса
                        Interval = new RequestInterval(to = to.AddDays(1), Interval.DaysCount);
                    }

                    var totalDays = (int)to.Subtract(from).TotalDays;
                    if (totalDays >= Math.Abs(reqiredDays)) // обычно так и должно быть...
                    {
                        daysCount += Math.Abs(reqiredDays);

                        // фактическое число запрошенных дней, может не совпадать с интервалом...
                        int days = daysCount + Interval.DaysCount;
                        if (days < 0)
                        {
                            // нужен дозапрос, с ограничением числа дней
                            days = Math.Max(Math.Min(days, -10), // меньше 10 дней не запрашиваем! 
                                -DataAggregator.DaysFromBase4Period(key.DataPointEnum, (int)key.TimeFrame.ToBaseFrameTypes()));
                            Owner.GetHistoryAsync(key, from, days, this);
                            return; // продолжить запрос
                        }
                    }

                    // больше данных нет или запросили сколько надо - конец запроса
                    if ((ShortDate)from.Date < Interval.From || (ShortDate)to.Date > Interval.To)
                    {
                        ushort t1 = default(ushort), t2 = default(ushort);
                        // из-за праздников начало интервала стало раньше или пришло обновление позже текущего интервала
                        Interval = new RequestInterval(
                            (ShortDate)(t1 = Math.Min((ushort)(ShortDate)from.Date, (ushort)Interval.From)),
                            (ShortDate)(t2 = Math.Max((ushort)(ShortDate)to.Date, (ushort)Interval.To)));
                    }
                    
                    IntervalList intervals;
                    if (Owner.activeIntervals.TryGetValue(key, out intervals))
                    {
                        lock (intervals)
                        {
                            intervals.Add(Interval); // обновить интервал получения даты

                            if (AppConfig.Common.IntervalsLogEnabled)
                                LogFileManager.AddInfo(String.Concat("OnDataReady", Environment.NewLine, 
                                    "key: ", key.ToString(), "\trequest: ", requestId.ToString(), "\tinterval: ", Interval.ToString(), 
                                    "\tintervals: ", intervals.ToString()), "ints");
                        }
                    }

                    // запрос закончен вызываем потребителя
                    if (DataReady != null)
                    {
                        DataReady(requestId);
                        DataReady = null;
                    }
                }
            }

            public string Name
            {
                get
                {
                    return (Parent != null) ? Parent.Name : "ContinuationData consumer.";
                }
            }
            
            public void Dispose()
            {
                //DataReady = null;
            }

            public void OnProgress(ShortDate day, int count)
            {
                if (Parent != null)
                    Parent.OnProgress(day, count);
            }
        }

        internal class CheckDateConsumer : IHistoryConsumer
        {
            HistoryManager Owner;
            AggregatorKey Key;
            KeyValuePair<ShortDate, long>[] Checks;

            public CheckDateConsumer(HistoryManager owner, AggregatorKey key, KeyValuePair<ShortDate, long>[] checks)
            {
                RequestId = 0;
                Checks = checks;
                Owner = owner;
                Key = key;
                Name = String.Concat(key.ToString(), " check date consumer");
            }

            public int RequestId { get; set; }

            public void Start(int checkCount)
            {
                // нужен дозапрос, с ограничением числа дней
                var days = Math.Min(checkCount, DataAggregator.DaysFromBase4Period(Key.DataPointEnum, (int)Key.TimeFrame.ToBaseFrameTypes()));
                var infos = new List<IntervalInfoEntity>(days);
                for (int i = daysCount; i < Checks.Length && infos.Count <= days; i++)
                    infos.Add(new IntervalInfoEntity() { Date = Checks[i].Key, Hash = Checks[i].Value });

                Owner.CheckHistoryAsync(Key, infos, this);
            }

            int daysCount;
            public void OnDataReady(AggregatorKey key, DateTime from, DateTime to, int reqiredDays = 0)
            {
                daysCount += Math.Abs(reqiredDays);

                // фактическое число запрошенных дней, может не совпадать с интервалом...
                int days = daysCount - Checks.Length;
                if (days < 0)
                    // нужен дозапрос, с ограничением числа дней
                    Start(Math.Abs(days));
                else
                    // все синхронизировали
                    Checks = new KeyValuePair<ShortDate, long>[0];
            }

            public string Name { get; protected set; }

            public void Dispose()
            {
                if (Checks != null && Checks.Length > 0)
                {
                    Owner.FreeCheck(Key, Checks.Skip(daysCount)
                        .Where(cd => cd.Value > 0)
                        .Select(c => c.Key));
                    Checks = new KeyValuePair<ShortDate, long>[0];
                }
            }

            public void OnProgress(ShortDate day, int count)
            {
                //throw new NotImplementedException();
            }
        }

        internal class RequestProcessor : IDisposable
        {
            public enum RequestState { Single, Polling, Break }

            public readonly int RequestId;
            public readonly RequestState State;
            public readonly HistoryManager Manager;
            public readonly IHistoryConsumer Consumer;
            int refCount;

            public RequestProcessor(HistoryManager manager, IHistoryConsumer consumer = null)
            {
                refCount = 0;
                State = RequestState.Single;
                Manager = manager;
                RequestId = Manager.NewRequestId;
                Consumer = consumer;
                requestData = new ConcurrentDictionary<ShortDate, RequestData>();
                requestDates = new ConcurrentDictionary<int, LinkedItem<ShortDate>>();
                initFinished = new ConcurrentQueue<int>();

                if (consumer != null)
                    consumer.RequestId = RequestId;
            }
            public RequestProcessor(RequestProcessor processor) : this(processor.Manager, processor.Consumer)
            {
                State = RequestState.Polling;
            }

            public int AddRef()
            {
                return Interlocked.Increment(ref refCount);
            }

            public bool Release()
            {
                return Interlocked.Decrement(ref refCount) <= 0;
            }

            public void Reset()
            {
                if (requestDates.Count > 0)
                    requestDates = new ConcurrentDictionary<int, LinkedItem<ShortDate>>();
                if (requestData.Count > 0)
                    requestData = new ConcurrentDictionary<ShortDate, RequestData>();
                if (initFinished.Count > 0)
                    initFinished = new ConcurrentQueue<int>();
            }

            public void Append(ChartArchiveEntity dataEntity)
            {
                if (dataEntity.Operation != Operation.InitFinished)
                {
                    if (AppConfig.Common.AppendArchLogEnabled)
                        LogFileManager.AddInfo(dataEntity.ToString(), "append");

                    requestData.AddOrUpdate(dataEntity.Date, new RequestData(dataEntity),
                        (day, old) => new RequestData(dataEntity, old.Points));

                    requestDates.AddOrUpdate(dataEntity.IdRequest,
                        new LinkedItem<ShortDate>(dataEntity.Date),
                        (day, old) => new LinkedItem<ShortDate>(dataEntity.Date, old));

                    if (Consumer != null)
                        Consumer.OnProgress(dataEntity.Date, dataEntity.Candles.Count);
                }
                else
                {
                    if (AppConfig.Common.AppendArchLogEnabled)
                        LogFileManager.AddInfo("initFinished" + dataEntity.IdRequest.ToString(), "append");

                    initFinished.Enqueue(dataEntity.IdRequest);
                    // поставить в очередь на обработку
                    Manager.DoRequest(RequestId);
                }
            }

            ShortDate? from = null, to = null;

            public ShortDate? From
            {
                get { return from; }
                private set
                {
                    if (!from.HasValue || !value.HasValue)
                        from = value;
                    else
                    {
                        if (from.Value > value.Value)
                            from = value;
                    }
                }
            }

            public ShortDate? To
            {
                get { return to; }
                private set
                {
                    if (!to.HasValue || !value.HasValue)
                        to = value;
                    else
                    {
                        if (to.Value < value.Value)
                            to = value;
                    }
                }
            }

            public void Process(AggregatedItem aggregatedItem)
            {
                int requestId;
                while (initFinished.TryDequeue(out requestId))
                {
                    LinkedItem<ShortDate> dateList;
                    if (requestDates.TryRemove(requestId, out dateList))
                    {
                        // даты в порядке возрастания!
                        using (var dates = dateList.GetEnumerator())
                        {
                            if (dates.MoveNext())
                            {
                                From = To = dates.Current;
                                do
                                {
                                    RequestData data;
                                    if (requestData.TryRemove(dates.Current, out data))
                                    {
                                        // заполняем пустые дни
                                        for (ShortDate i = To.Value + 1; i < dates.Current; i += 1)
                                            aggregatedItem.UpdateDay(i, Enumerable.Empty<DataPoint>(), 0, false, RequestId);

                                        aggregatedItem.UpdateDay(dates.Current, new LinkedItem<DataPoint>(data.Points), data.LastTradeNumber, data.FullDay, RequestId);
                                        To = dates.Current;
                                    }
                                } while (dates.MoveNext());
                            }
                        }
                    }

                    // освободить requestId
                    Manager.DoneRequest(aggregatedItem.Key, requestId, From, To);
                }
            }

            ConcurrentDictionary<ShortDate, RequestData> requestData;
            ConcurrentDictionary<int, LinkedItem<ShortDate>> requestDates;
            ConcurrentQueue<int> initFinished;

            public void Dispose()
            {
                if (Consumer != null)
                    Consumer.Dispose();
            }

            internal struct RequestData
            {
                public readonly bool FullDay;
                public readonly long LastTradeNumber;
                public readonly LinkedItem<DataPoint> Points;

                public RequestData(ChartArchiveEntity chartArchive, LinkedItem<DataPoint> lastPoints = null)
                {
                    FullDay = chartArchive.FullDay && chartArchive.Candles.Count > 0;
                    LastTradeNumber = chartArchive.LastTradeNo;
                    Points = new LinkedItem<DataPoint>(chartArchive.Candles.OfType<DataPoint>(), lastPoints);
                }
            }
        }
    }

    public class LinkedItem<T> : IEnumerable<T>, IEnumerable
    {
        public readonly T Value;
        public readonly LinkedItem<T> Prev;

        public LinkedItem(T value, LinkedItem<T> prev = null)
        {
            Value = value;
            Prev = prev;
        }

        public LinkedItem(IEnumerable<T> values, LinkedItem<T> prev = null)
        {
            Value = default(T);
            Prev = prev;
            using (var vals = values.GetEnumerator())
            {
                if (vals.MoveNext())
                {
                    Value = vals.Current;
                    while (vals.MoveNext())
                    {
                        Prev = new LinkedItem<T>(Value, Prev);
                        Value = vals.Current;
                    }
                }
            }
        }

        internal struct Enumerator : IEnumerator<T>, IDisposable, IEnumerator
        {
            public Enumerator(LinkedItem<T> root)
            {
                current = new LinkedItem<T>(default(T), root);
            }

            LinkedItem<T> current;
            public T Current { get { return current.Value; } }

            public void Dispose() { current = null; }

            object IEnumerator.Current { get { return Current; } }

            public bool MoveNext()
            {
                if (current == null)
                    return false;

                return (current = current.Prev) != null;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return new Enumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public sealed class HistoryCompletitionManager : QueuedCompletitionManager
    {
        static HistoryCompletitionManager _instance;
        public static HistoryCompletitionManager Instance
        {
            get
            {
                return _instance;
            }
        }

        static HistoryCompletitionManager()
        {
            _watch = new Stopwatch();
            _watch.Start();
            _Historys = new ConcurrentDictionary<AggregatorKey, AggregatedItem>();
            _instance = new HistoryCompletitionManager();
        }

        HistoryCompletitionManager() : this(8) { }

        public HistoryCompletitionManager(int count) : base(count)
        {
            _HistoryQueue = new SortedSet<HistoryInfo>(new HistoryComparer());
            Start();
        }

        static ConcurrentDictionary<AggregatorKey, AggregatedItem> _Historys;
        public void RegisterHistory(AggregatedItem pm)
        {
            _Historys.TryAdd(pm.Key, pm);
        }

        public void UnRegisterHistory(AggregatedItem pm)
        {
            _Historys.TryRemove(pm.Key, out pm);
        }

        public void RegisterHistory(AggregatorKey handle)
        {
            _Historys.TryAdd(handle, null);
        }

        public void UnRegisterHistory(AggregatorKey handle)
        {
            AggregatedItem pm;
            _Historys.TryRemove(handle, out pm);
        }

        public IEnumerable<AggregatedItem> AggregatedItems
        {
            get
            {
                return _Historys.Values;
            }
        }

        public static long TimeStamp
        {
            get
            {
                return _watch.ElapsedMilliseconds;
            }
        }

        public void Post(HistoryInfo p)
        {
            lock (_HistoryQueue)
            {
                while (!_HistoryQueue.Add(p))
                    p = p + 1;
                base.Post();
            }
        }

        static Stopwatch _watch;
        protected override int Delay
        {
            get
            {
                lock (_HistoryQueue)
                {
                    if (_HistoryQueue.Count > 0)
                    {
                        var result = 1 + (int)Math.Max(0L, _HistoryQueue.Min - _watch.ElapsedMilliseconds);
                        return (result < 20) ? 0 : result;
                    }
                }
                return base.Delay;
            }
        }
        protected override void Timeout()
        {
            HistoryInfo p;
            long now = _watch.ElapsedMilliseconds;
            while (true)
            {
                lock (_HistoryQueue)
                {
                    if (_HistoryQueue.Count > 0 && (p = _HistoryQueue.Min) <= now)
                        _HistoryQueue.Remove(p);
                    else
                        return;
                }

                AggregatedItem pm = null;
                HistoryCall call = p;
                if (call.Key == 0UL || _Historys.TryGetValue(call.Key, out pm))
                    call.Value(pm);
            }
        }
        public struct HistoryInfo
        {
            public long StartTime;
            public long Elapsed;
            HistoryCall Eval;
            public HistoryInfo(long msec, HistoryCall eval)
                : this(HistoryCompletitionManager.TimeStamp, msec, eval)
            {
            }
            public HistoryInfo(long ticks, long msec, HistoryCall eval)
            {
                Elapsed = ticks + msec;
                StartTime = Elapsed - msec;
                Eval = eval;
            }
            internal HistoryInfo(HistoryInfo info, long delta)
            {
                StartTime = info.StartTime;
                Eval = info.Eval;
                Elapsed = info.Elapsed + delta;
            }
            public static implicit operator long(HistoryInfo r)
            {
                return r.Elapsed;
            }
            public static implicit operator int(HistoryInfo r)
            {
                return (int)r.Elapsed;
            }
            public static implicit operator HistoryCall(HistoryInfo r)
            {
                return r.Eval;
            }
            public static HistoryInfo operator +(HistoryInfo pi, long d)
            {
                return new HistoryInfo(pi, d);
            }
        }

        internal class HistoryComparer : IComparer<HistoryInfo>
        {
            #region IComparer<HistoryInfo> Members

            public int Compare(HistoryInfo r1, HistoryInfo r2)
            {
                int x = r1, y = r2;
                int xy = x - y;
                int yx = y - x;
                return (xy >> 31) | (int)((uint)yx >> 31);
            }

            #endregion
        }

        SortedSet<HistoryInfo> _HistoryQueue;
    }

    internal class HistoryDataProvider : IDisposable
    {
        public HistoryDataProvider(Core.Arch.HistoryStorage storage)
        {
            m_storage = storage;
            m_index = new ConcurrentDictionary<AggregatorKey, DateIndex>();
        }

        public void Clear(string fileName)
        {
            m_storage.Clear(fileName);
        }

        private DateIndex GetOrAddIndex(AggregatorKey aggregatorKey)
        {
            return m_index.GetOrAdd(aggregatorKey, k => 
            {
                var di = new DateIndex(m_storage.Streams(k).Select(i => (ShortDate)(ushort)i));
                if (AppConfig.Common.HindexLogEnabled)
                    LogFileManager.AddInfo(String.Concat(aggregatorKey.ToString(), Environment.NewLine, di.Text), "hidx");
                return di;
            });
        }

        public ShortDate[] GetIndex(AggregatorKey aggregatorKey, ShortDate? from = null, TimeDirection dir = TimeDirection.Forward)
        {
            var index = GetOrAddIndex(aggregatorKey);
            ShortDate[] result = new ShortDate[] { };
            lock (index)
            {
                result = index.GetDates(dir, from).ToArray();
            }

            return result;
        }

        public void ScanIndex(AggregatorKey aggregatorKey, Func<ShortDate, bool> onScan, ShortDate? from = null, TimeDirection dir = TimeDirection.Backward)
        {
            var index = GetOrAddIndex(aggregatorKey);
            lock(index)
            {
                using (var dts = index.GetDates(dir, from).GetEnumerator())
                {
                    while (dts.MoveNext() && onScan(dts.Current)) ;
                }
            }
        }

        public KeyValuePair<bool, byte[]> TryRead(AggregatorKey aggregatorKey, ShortDate day)
        {
            var index = GetOrAddIndex(aggregatorKey);
            lock (index)
            {
                if (index.Contains(day))
                {
                    if (m_storage.ReadStream(aggregatorKey, day.GetHashCode(), out var result))
                        return result;
                }
            }
            return default(KeyValuePair<bool, byte[]>);
        }

        public bool TryWrite(AggregatorKey aggregatorKey, ShortDate day, KeyValuePair<bool, byte[]> data)
        {
            var index = GetOrAddIndex(aggregatorKey);
            lock (index)
            {
                if (!index.Contains(day))
                    index.Add(day);
                return m_storage.WriteStream(aggregatorKey, day.GetHashCode(), data);
            }
        }

        private Core.Arch.HistoryStorage m_storage;
        private ConcurrentDictionary<AggregatorKey, DateIndex> m_index;

        public void Dispose()
        {
            if (m_storage != null)
            {
                m_storage.Dispose();
                m_storage = null;
                m_index = null;
            }
            GC.SuppressFinalize(this);
        }

        internal class DateIndex
        {
            public DateIndex()
            {
                m_dates = new SortedList<ShortDate, bool>();
            }

            public DateIndex(IEnumerable<ShortDate> dates) : this()
            {
                AddRange(dates);
            }

            public void Add(ShortDate date)
            {
                m_dates[date] = true;
            }

            public void AddRange(IEnumerable<ShortDate> dates)
            {
                foreach (var dt in dates)
                    m_dates[dt] = true;
            }
            public int Count { get { return m_dates.Count; } }
            //with lock!
            private int GetIndex(ShortDate date)
            {
                var keys = m_dates.Keys;
                int low = 0, high = m_dates.Count - 1, mid = -1;

                if (keys[0] < date)
                {
                    while (low <= high)
                    {
                        mid = (low + high) >> 1;
                        if (keys[mid] == date)
                            return mid;
                        if (keys[mid] > date)
                            high = mid - 1;
                        else
                            low = mid + 1;
                    }
                }
                return Math.Max(Math.Min(low, m_dates.Count - 1), 0);
            }
            public bool Contains(ShortDate date)
            {
                return m_dates.ContainsKey(date);
            }
            public IEnumerable<ShortDate> GetDates(TimeDirection dir, ShortDate? from)
            {
                int i = (dir == TimeDirection.Backward) ? m_dates.Count -1 : 0;
                if (from.HasValue && m_dates.Count > 0)
                {
                    if (from.Value >= m_dates.Keys[0] && from.Value <= m_dates.Keys[m_dates.Count - 1])
                        i = GetIndex(from.Value);
                    else
                        yield break;
                }

                if (TimeDirection.Forward == dir)
                    for (; i < m_dates.Count; i++)
                        yield return m_dates.Keys[i];
                else
                    for (; i >= 0; i--)
                        yield return m_dates.Keys[i];
            }

            public string Text
            {
                get
                {
                    return String.Join("|", m_dates.Keys.Select(k => k.ToString()));
                }
            }

            private SortedList<ShortDate, bool> m_dates;
        }
    }

}

namespace Core.Arch
{
    using System.Runtime;
    using System.Runtime.InteropServices;

    using AD.Common.Helpers;

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 8)]
    internal struct BlockKey
    {
        [FieldOffset(0)] public int Key;
        [FieldOffset(0)] public int Size;
        [FieldOffset(4)] public int Ptr;

        public static implicit operator BlockKey(int id)
        {
            return new BlockKey() { Key = id };
        }
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 256)]
    internal struct Block
    {
        public const int KEYCNT = 32;
        public const int DATASIZE = 256;
        public const int STREAMSIZE = 248;
        public const int NEXTKEY = 31;

        [FieldOffset(0), MarshalAs(UnmanagedType.LPArray, SizeConst = Block.DATASIZE)]
        public byte[] Data;
        [FieldOffset(0), MarshalAs(UnmanagedType.LPArray, SizeConst = Block.KEYCNT)]
        public BlockKey[] Index;

        public BlockKey Next
        {
            [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
            get { return Index[NEXTKEY]; }
            set { Index[NEXTKEY] = value; }
        }
    }

    internal class Storage : IDisposable
    {
        public Storage(Func<int, byte[]> read, Func<int, byte[], int> write, Action dispose)
        {
            m_read = read;
            m_write = write;
            m_dispose = dispose;
            m_blocks = new ConcurrentDictionary<int, Block>();
        }

        public void Dispose()
        {
            m_blocks = null;
            if (m_dispose != null)
                m_dispose();
        }

        public int GetBlock(int n = -1, bool keep = false)
        {
            if (n < 0)
            {
                n = m_write(n, s_nullBytes);

                if (keep)
                    KeepBlock(n);

                return n;
            }

            if (keep)
                KeepBlock(n);

            var b = ReadBlock(n);
            return (b.Data == null || b.Data.Length == 0) ? 0 : (int)b.Next.Ptr;
        }

        [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
        public int WriteBlock(int n, byte[] data)
        {
            if (m_blocks.ContainsKey(n))
                m_blocks[n] = new Block() { Data = data };

            return m_write(n, data);
        }

        [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
        public Block KeepBlock(int n)
        {
            return m_blocks.GetOrAdd(n, i => ReadBlock(i));
            //return ReadBlock(n);
        }

        [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
        public Block ReadBlock(int n)
        {
            if (m_blocks.TryGetValue(n, out var b))
                return b;
            return new Block() { Data = m_read(n) };
        }

        private Action m_dispose;
        private Func<int, byte[]> m_read;
        private Func<int, byte[], int> m_write;
        private ConcurrentDictionary<int, Block> m_blocks;

        static Storage()
        {
            s_nullBytes = new byte[Block.DATASIZE];
        }
        private static byte[] s_nullBytes;
    }

    internal class Index
    {
        public const int MAXDEPTH = 512;

        public Index(BlockKey bk, Storage arch)
        {
            m_bk = bk; m_arch = arch;
            if (m_bk.Ptr == -1) // создаем корневой блок, вызываем один раз!!!!
                m_arch.GetBlock();
        }
        public int Key
        {
            [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
            get { return m_bk.Key; }
        }
        public int Ptr
        {
            [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
            get { return m_bk.Ptr; }
        }
        public IEnumerable<BlockKey> Keys
        {
            get
            {
                int ptr = m_bk.Ptr;
                Block block;
                for (int j = 0; j < Index.MAXDEPTH && (j == 0 || ptr != 0); j++, ptr = block.Next.Ptr)
                {
                    block = m_arch.KeepBlock(ptr);

                    for (int i = 0; i < Block.NEXTKEY; i++)
                    {
                        if (block.Index[i].Key == 0) // неинициализированный указатель - конец списка
                            break;
                        yield return block.Index[i];
                    }
                }
            }
        }

        public Index this[int key]
        {
            get
            {
                int ptr = m_bk.Ptr;
                Block block;
                for (int j = 0; j < Index.MAXDEPTH; j++, ptr = block.Next.Ptr)
                {
                    block = m_arch.KeepBlock(ptr);

                    for (int i = 0; i < Block.NEXTKEY; i++)
                    {
                        if (block.Index[i].Key == key)
                            return new Index(block.Index[i], m_arch); // индекс уже добавлен

                        if (block.Index[i].Key == 0) // неинициализированный указатель - конец списка
                        {
                            // индекс не нашли - добавляем
                            block.Index[i] = new BlockKey() { Key = key, Ptr = m_arch.GetBlock() };

                            m_arch.WriteBlock(ptr, block.Data); // сохраняем блок
                            return new Index(block.Index[i], m_arch);
                        }
                    }

                    if (block.Next.Ptr == 0) // последний блок последовательности
                    {
                        // расширяем последовательность новым блоком
                        block.Next = new BlockKey() { Ptr = m_arch.GetBlock(-1, true) };
                        m_arch.WriteBlock(ptr, block.Data); // сохраняем блок
                    }
                }

                return null;
            }
        }

        [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
        public static implicit operator Stream(Index index)
        {
            return new Stream(index.m_bk, index.m_arch);
        }

        private BlockKey m_bk;
        private Storage m_arch;
    }

    internal class Stream
    {
        public const int MAXDEPTH = 1024;

        public Stream(BlockKey bk, Storage arch)
        {
            m_bk = bk; m_arch = arch;
        }
        public int Key
        {
            [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
            get { return m_bk.Key; }
        }
        public int Ptr
        {
            [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
            get { return m_bk.Ptr; }
        }
        public int Size
        {
            [TargetedPatchingOptOut("Performance critical to inline across NGen image boundaries")]
            get { return m_arch.ReadBlock(m_bk.Ptr).Next.Size; }
        }

        public IEnumerable<byte[]> DataBlocks
        {
            get
            {
                int ptr = m_bk.Ptr;
                Block block;
                for (int i = 0; i < Stream.MAXDEPTH && ptr != 0; i++, ptr = block.Next.Ptr)
                {
                    block = m_arch.ReadBlock(ptr);
                    yield return block.Data;
                }
            }
        }

        public byte[] Data
        {
            get
            {
                var result = new byte[Size];

                if (result.Length > 0)
                {
                    var count = result.Length;
                    foreach (var block in DataBlocks)
                    {
                        Array.Copy(block, 0, result, result.Length - count, Math.Min(Block.STREAMSIZE, count));
                        if ((count -= Block.STREAMSIZE) <= 0)
                            break;
                    }
                }

                return result;
            }

            set
            {
                int ptr = m_bk.Ptr;
                Block block;
                for (int j = 0, count = value.Length; j < Stream.MAXDEPTH && count > 0; j++, count -= Block.STREAMSIZE, ptr = block.Next.Ptr)
                {
                    block = m_arch.ReadBlock(ptr);

                    var bNeedWrite = true;
                    if (j == 0) // превый блок в него всегда пишем данные и размер
                    {
                        if (!Cmp(value, value.Length - count, block.Data, 0, Math.Min(Block.STREAMSIZE, count)))
                            Array.Copy(value, value.Length - count, block.Data, 0, Math.Min(Block.STREAMSIZE, count));

                        block.Next = new BlockKey() { Size = value.Length, Ptr = block.Next.Ptr }; // пишем размер в первый блок

                        if (count > Block.STREAMSIZE && block.Next.Ptr == 0) // последний блок последовательности, нужны еще блоки
                            block.Next = new BlockKey() { Size = value.Length, Ptr = m_arch.GetBlock() }; // новый блок в конец
                    }
                    else
                    {
                        bNeedWrite = false;
                        if (bNeedWrite = (count > Block.STREAMSIZE && block.Next.Ptr == 0)) // последний блок последовательности, нужны еще блоки
                            block.Next = new BlockKey() { Size = value.Length, Ptr = m_arch.GetBlock() }; // новый блок в конец

                        // проверить нужно ли писать блок, чаще всего нет...
                        if (!Cmp(value, value.Length - count, block.Data, 0, Math.Min(Block.STREAMSIZE, count)))
                        {
                            Array.Copy(value, value.Length - count, block.Data, 0, Math.Min(Block.STREAMSIZE, count));
                            bNeedWrite = true;
                        }
                    }

                    if (bNeedWrite)
                        m_arch.WriteBlock(ptr, block.Data);
                }
            }
        }

        private bool Cmp(byte[] arr1, int idx1, byte[] arr2, int idx2, int length)
        {
            for (int i = idx1, j = idx2, k = 0; i < arr1.Length && j < arr2.Length && k < length; i++, j++, k++)
                if (arr1[i] != arr2[j])
                    return false;
            return true;
        }

        private BlockKey m_bk;
        private Storage m_arch;
    }

    internal class AsyncArchProvider : IDisposable
    {
        public AsyncArchProvider(string fileName, int blockSize)
        {
            m_file = new AsyncLogFile(fileName);
            m_reads = new ConcurrentDictionary<int, bool>();
            m_blockSize = blockSize;
        }

        public void Dispose()
        {
            m_file.Dispose();
            m_reads = null;
        }

        public void Flush() { m_file.Flush(); }

        public byte[] Read(int offset)
        {            
            var result = new byte[m_blockSize];
            if (!m_reads.TryGetValue(offset, out var b))
                m_file.Read(result, offset);
            return result;
        }

        public int FirstWrite(int offset, byte[] data)
        {
            if (m_file.Offset == 0)
                m_file.Write(data);

            return 0;
        }

        public int Write(int offset, byte[] data)
        {
            if (offset < 0)
            {
                m_file.WriteSync(data, o => (offset = o) == o);
                var result = offset - data.Length;
                m_reads[result] = true;
                return result;
            }
            else
            {
                m_file.WriteSync(data, offset);
                m_reads.TryRemove(offset, out var b);
                return offset;
            }
        }

        private int m_blockSize;
        private AsyncLogFile m_file;
        private ConcurrentDictionary<int, bool> m_reads;
        
    }

    public class HistoryStorage : IDisposable
    {
        Storage m_storage;
        ConcurrentDictionary<AggregatorKey, Index> m_aggregatorStorage;

        public HistoryStorage(string fileName) : this(GetStorage(fileName)) { }

        private HistoryStorage(Storage storage)
        {
            m_storage = storage;
            m_aggregatorStorage = new ConcurrentDictionary<AggregatorKey, Index>();
        }

        private Index Root
        {
            get { return new Index(new BlockKey(), m_storage); }
        }

        private static Storage GetStorage(string fileName)
        {
            var provider = new AsyncArchProvider(fileName, Block.DATASIZE);

            using (var st = new Storage(provider.Read, provider.FirstWrite, null))
            {
                // создать корневой элемент при первом открытии файла
                new Index(new BlockKey() { Key = 0, Ptr = -1 }, st);
            }

            return new Storage(provider.Read, provider.Write, provider.Dispose);            
        }

        public void Clear(string fileName)
        {
            lock (this)
            {
                m_aggregatorStorage = new ConcurrentDictionary<AggregatorKey, Index>();
                m_storage.Dispose();

                if (File.Exists(fileName + ".bak"))
                    File.Delete(fileName + ".bak");
                File.Move(fileName, fileName + ".bak");

                m_storage = GetStorage(fileName);
            }
        }

        private bool TryOpenStorage(AggregatorKey aggregatorKey, out Index storage)
        {
            lock (this)
            {
                if (!m_aggregatorStorage.TryGetValue(aggregatorKey, out storage))
                {
                    storage = Root[(int)aggregatorKey.CandleType << 8 ^ (int)aggregatorKey.TimeFrame][aggregatorKey.IdFI];
                    m_aggregatorStorage[aggregatorKey] = storage;
                }
                return true;
            }
        }

        public IEnumerable<int> Streams(AggregatorKey aggregatorKey)
        {
            if (TryOpenStorage(aggregatorKey, out var aggreagatorStorage))
                return aggreagatorStorage.Keys.Select(k => k.Key);
            else
                return Enumerable.Empty<int>();
        }

        public bool ReadStream(AggregatorKey aggregatorKey, int day, out KeyValuePair<bool, byte[]> result)
        {
            result = default(KeyValuePair<bool, byte[]>);

            if (!TryOpenStorage(aggregatorKey, out var aggreagatorStorage))
                return false;

            var stream = (Stream)aggreagatorStorage[day];
            var size = stream.Size;
            if (size == 0)
                return false;

            result = new KeyValuePair<bool, byte[]>(true, stream.Data);
            return true;
        }

        public bool WriteStream(AggregatorKey aggregatorKey, int day, KeyValuePair<bool, byte[]> data)
        {
            if (!TryOpenStorage(aggregatorKey, out var aggreagatorStorage))
                return false;

            var stream = (Stream)aggreagatorStorage[day];
            stream.Data = data.Value;
            
            return true;
        }

        ~HistoryStorage()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            try
            {
                if (disposing)
                {
                    m_aggregatorStorage = null;
                    if (m_storage != null)
                        m_storage.Dispose();
                    m_storage = null;
                }
                GC.SuppressFinalize(this);
            }
            finally
            {
                m_aggregatorStorage = null;
                m_storage = null;
            }
        }
    }

}
