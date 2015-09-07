namespace NEventStore.Persistence.MongoDB
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using global::MongoDB.Bson;
    using global::MongoDB.Driver;
    using NEventStore.Logging;
    using NEventStore.Serialization;
    using ALinq;
    using System.Threading.Tasks;

    public static class IAsyncEnumerableExtensions
    {
        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IAsyncCursorSource<T> asyncCursorSource)
        {
            return AsyncEnumerable.Create<T>(producer => asyncCursorSource.ForEachAsync(producer.Yield));
        }

        public static async Task<T> FirstOrDefault<T>(this Task<IAsyncCursor<T>> asyncCursorProvider)
        {
            using (var cursor = await asyncCursorProvider)
            {
                if (await cursor.MoveNextAsync())
                    return cursor.Current.FirstOrDefault();
                return default(T);
            }
        }
    }

    public class MongoPersistenceEngine : IPersistStreams
    {
        private const string ConcurrencyException = "E1100";
        private const int ConcurrencyExceptionCode = 11000;
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(MongoPersistenceEngine));
        private readonly MongoCollectionSettings _commitSettings;
        private readonly IDocumentSerializer _serializer;
        private readonly MongoCollectionSettings _snapshotSettings;
        private readonly IMongoDatabase _store;
        private readonly MongoCollectionSettings _streamSettings;
        private bool _disposed;
        private int _initialized;
        private readonly Func<Task<LongCheckpoint>> _getNextCheckpointNumber;
        private readonly Func<Task<long>> _getLastCheckPointNumber;
        private readonly MongoPersistenceOptions _options;
        private readonly WriteConcern _insertCommitWriteConcern;
        private readonly BsonJavaScript _updateScript;
        private readonly LongCheckpoint _checkpointZero;

        public MongoPersistenceEngine(IMongoDatabase store, IDocumentSerializer serializer, MongoPersistenceOptions options)
        {
            if (store == null)
            {
                throw new ArgumentNullException("store");
            }

            if (serializer == null)
            {
                throw new ArgumentNullException("serializer");
            }

            if (options == null)
            {
                throw new ArgumentNullException("options");
            }

            _store = store;
            _serializer = serializer;
            _options = options;

            // set config options
            _commitSettings = _options.GetCommitSettings();
            _snapshotSettings = _options.GetSnapshotSettings();
            _streamSettings = _options.GetStreamSettings();
            _insertCommitWriteConcern = _options.GetInsertCommitWriteConcern();

            _getLastCheckPointNumber = () => TryMongo(async () =>
            {
                var max = await PersistedCommits
                    .Find(o => true)
                    .Project(Builders<BsonDocument>.Projection.Include(MongoCommitFields.CheckpointNumber))
                    .Sort(Builders<BsonDocument>.Sort.Descending(MongoCommitFields.CheckpointNumber))
                    .Limit(1)
                    .FirstOrDefaultAsync();

                return max != null ? max[MongoCommitFields.CheckpointNumber].AsInt64 : 0L;
            });

            _getNextCheckpointNumber = async () => new LongCheckpoint(await _getLastCheckPointNumber() + 1L);

            _updateScript = new BsonJavaScript("function (x){ return insertCommit(x);}");
            _checkpointZero = new LongCheckpoint(0);
        }

        protected virtual IMongoCollection<BsonDocument> PersistedCommits
        {
            get { return _store.GetCollection<BsonDocument>("Commits", _commitSettings); }
        }

        protected virtual IMongoCollection<BsonDocument> PersistedStreamHeads
        {
            get { return _store.GetCollection<BsonDocument>("Streams", _streamSettings); }
        }

        protected virtual IMongoCollection<BsonDocument> PersistedSnapshots
        {
            get { return _store.GetCollection<BsonDocument>("Snapshots", _snapshotSettings); }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual async Task Initialize()
        {
            if (Interlocked.Increment(ref _initialized) > 1)
            {
                return;
            }

            Logger.Debug(Messages.InitializingStorage);

            await TryMongo(async () =>
            {
                await PersistedCommits.Indexes.CreateOneAsync(
                    Builders<BsonDocument>.IndexKeys
                        .Ascending(MongoCommitFields.Dispatched)
                        .Ascending(MongoCommitFields.CommitStamp),
                    new CreateIndexOptions { Name = MongoCommitIndexes.Dispatched, Unique = false }
                );

                await PersistedCommits.Indexes.CreateOneAsync(
                    Builders<BsonDocument>.IndexKeys
                        .Ascending(MongoCommitFields.BucketId)
                        .Ascending(MongoCommitFields.StreamId)
                        .Ascending(MongoCommitFields.StreamRevisionFrom)
                        .Ascending(MongoCommitFields.StreamRevisionTo)
                    //,MongoCommitFields.FullqualifiedStreamRevision
                    ,
                        new CreateIndexOptions() { Name = MongoCommitIndexes.GetFrom, Unique = true }
                    );

                await PersistedCommits.Indexes.CreateOneAsync(
                    Builders<BsonDocument>.IndexKeys
                        .Ascending(MongoCommitFields.BucketId)
                        .Ascending(MongoCommitFields.StreamId)
                        .Ascending(MongoCommitFields.CommitSequence)
                    ,
                    new CreateIndexOptions { Name = MongoCommitIndexes.LogicalKey, Unique = true }
                );

                await PersistedCommits.Indexes.CreateOneAsync(
                    Builders<BsonDocument>.IndexKeys.Ascending(MongoCommitFields.CommitStamp),
                    new CreateIndexOptions { Name = MongoCommitIndexes.CommitStamp, Unique = false }
                );

                await PersistedCommits.Indexes.CreateOneAsync(
                    Builders<BsonDocument>.IndexKeys.Ascending(MongoStreamHeadFields.Unsnapshotted),
                    new CreateIndexOptions { Name = MongoStreamIndexes.Unsnapshotted, Unique = false }
                );

                //if (_options.ServerSideOptimisticLoop)
                //{
                //    await PersistedCommits.Database.GetCollection<BsonDocument>("system.js").InsertOneAsync(new BsonDocument{
                //        {"_id" , "insertCommit"},
                //        {"value" , new BsonJavaScript(_options.GetInsertCommitScript())}
                //    });
                //}

                await EmptyRecycleBin();
            });
        }

        public virtual IAsyncEnumerable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
        {
            Logger.Debug(Messages.GettingAllCommitsBetween, streamId, bucketId, minRevision, maxRevision);

            return TryMongo(() =>
           {
               var query = Builders<BsonDocument>.Filter.And(
                   Builders<BsonDocument>.Filter.Eq(MongoCommitFields.BucketId, bucketId),
                   Builders<BsonDocument>.Filter.Eq(MongoCommitFields.StreamId, streamId),
                   Builders<BsonDocument>.Filter.Gte(MongoCommitFields.StreamRevisionTo, minRevision),
                   Builders<BsonDocument>.Filter.Lte(MongoCommitFields.StreamRevisionFrom, maxRevision));

               return PersistedCommits
                   .Find(query)
                   .Sort(MongoCommitFields.StreamRevisionFrom)
                   .Project(mc => mc.ToCommit(_serializer));
           })
           .ToAsyncEnumerable();
        }

        public virtual IAsyncEnumerable<ICommit> GetFrom(string bucketId, DateTime start)
        {
            Logger.Debug(Messages.GettingAllCommitsFrom, start, bucketId);

            return TryMongo(() =>
                PersistedCommits
                .Find(
                    Builders<BsonDocument>.Filter.And(
                        Builders<BsonDocument>.Filter.Eq(MongoCommitFields.BucketId, bucketId),
                        Builders<BsonDocument>.Filter.Gte(MongoCommitFields.CommitStamp, start)
                    )
                )
                .Sort(MongoCommitFields.CheckpointNumber)
                .Project(x => x.ToCommit(_serializer)))
                .ToAsyncEnumerable();
        }

        public IAsyncEnumerable<ICommit> GetFrom(string bucketId, string checkpointToken)
        {
            var intCheckpoint = LongCheckpoint.Parse(checkpointToken);
            Logger.Debug(Messages.GettingAllCommitsFromBucketAndCheckpoint, bucketId, intCheckpoint.Value);

            return TryMongo(() => 
                PersistedCommits
               .Find(
                   Builders<BsonDocument>.Filter.And(
                       Builders<BsonDocument>.Filter.Eq(MongoCommitFields.BucketId, bucketId),
                       Builders<BsonDocument>.Filter.Gt(MongoCommitFields.CheckpointNumber, intCheckpoint.LongValue)
                   )
               )
               .Sort(MongoCommitFields.CheckpointNumber)
               .Project(x => x.ToCommit(_serializer)))
               .ToAsyncEnumerable();
        }

        public IAsyncEnumerable<ICommit> GetFrom(string checkpointToken)
        {
            var intCheckpoint = LongCheckpoint.Parse(checkpointToken);
            Logger.Debug(Messages.GettingAllCommitsFromCheckpoint, intCheckpoint.Value);

            return TryMongo(() => PersistedCommits
                .Find(
                    Builders<BsonDocument>.Filter.And(
                        Builders<BsonDocument>.Filter.Ne(MongoCommitFields.BucketId, MongoSystemBuckets.RecycleBin),
                        Builders<BsonDocument>.Filter.Gt(MongoCommitFields.CheckpointNumber, intCheckpoint.LongValue)
                    )
                )
                .Sort(MongoCommitFields.CheckpointNumber)
                .Project(x => x.ToCommit(_serializer))
            )
            .ToAsyncEnumerable();
        }

        public Task<ICheckpoint> GetCheckpoint(string checkpointToken = null)
        {
            return Task.FromResult<ICheckpoint>(LongCheckpoint.Parse(checkpointToken));
        }

        public virtual IAsyncEnumerable<ICommit> GetFromTo(string bucketId, DateTime start, DateTime end)
        {
            Logger.Debug(Messages.GettingAllCommitsFromTo, start, end, bucketId);

            return TryMongo(() => (PersistedCommits
               .Find(Builders<BsonDocument>.Filter.And(
                   Builders<BsonDocument>.Filter.Eq(MongoCommitFields.BucketId, bucketId),
                   Builders<BsonDocument>.Filter.Gte(MongoCommitFields.CommitStamp, start),
                   Builders<BsonDocument>.Filter.Lt(MongoCommitFields.CommitStamp, end))
               )
               .Sort(MongoCommitFields.CheckpointNumber)
               .Project(x => x.ToCommit(_serializer)))
                .ToAsyncEnumerable());
        }

        public virtual Task<ICommit> Commit(CommitAttempt attempt)
        {
            Logger.Debug(Messages.AttemptingToCommit, attempt.Events.Count, attempt.StreamId, attempt.CommitSequence);

            //return _options.ServerSideOptimisticLoop ?
            //    PersistWithServerSideOptimisticLoop(attempt) :
            //    PersistWithClientSideOptimisticLoop(attempt);

            return PersistWithClientSideOptimisticLoop(attempt);
        }

        //private  Task<ICommit> PersistWithServerSideOptimisticLoop(CommitAttempt attempt)
        //{
        //    BsonDocument commitDoc = attempt.ToMongoCommit(_checkpointZero, _serializer);

        //    return TryMongo(async () =>
        //    {
        //        var result = PersistedCommits.Database.Eval(new EvalArgs()
        //        {
        //            Code = _updateScript,
        //            Lock = false,
        //            Args = new BsonValue[] { commitDoc }
        //        });

        //        if (!result.IsBsonDocument)
        //            throw new Exception("Invalid response. Check server side js");

        //        if (result.AsBsonDocument.Contains("id"))
        //        {
        //            commitDoc["_id"] = result["id"];
        //            await UpdateStreamHeadAsync(attempt.BucketId, attempt.StreamId, attempt.StreamRevision, attempt.Events.Count);
        //            Logger.Debug(Messages.CommitPersisted, attempt.CommitId);
        //        }
        //        else if (result.AsBsonDocument.Contains("err"))
        //        {
        //            var errorDocument = result.AsBsonDocument;

        //            if (errorDocument["code"] != ConcurrencyExceptionCode)
        //            {
        //                throw new Exception(errorDocument["err"].AsString);
        //            }

        //            var savedCommit = (await PersistedCommits.FindAsync(attempt.ToMongoCommitIdQuery()).ToAsyncEnumerable().FirstOrDefault()).ToCommit(_serializer);

        //            if (savedCommit.CommitId == attempt.CommitId)
        //            {
        //                throw new DuplicateCommitException(String.Format(
        //                    "Duplicated Commit: bucketId [{0}]: commitId [{1}] - streamId [{2}] - streamRevision [{3}]",
        //                     attempt.BucketId, attempt.CommitId, attempt.StreamId, attempt.StreamRevision));
        //            }
        //            Logger.Debug(Messages.ConcurrentWriteDetected);
        //            throw new ConcurrencyException(String.Format(
        //                "Concurrency exception forbucketId [{0}]: commitId [{1}] - streamId [{2}] - streamRevision [{3}]\n Inner provider error: {4}",
        //                attempt.BucketId, attempt.CommitId, attempt.StreamId, attempt.StreamRevision, errorDocument["err"].AsString));
        //        }
        //        else
        //        {
        //            throw new Exception("Invalid response. Check server side js");
        //        }

        //        return commitDoc.ToCommit(_serializer);
        //    });
        //}


        private Task<ICommit> PersistWithClientSideOptimisticLoop(CommitAttempt attempt)
        {
            return TryMongo(async () =>
            {
                BsonDocument commitDoc = attempt.ToMongoCommit(
                    await _getNextCheckpointNumber(),
                    _serializer
                );

                bool retry = true;
                while (retry)
                {
                    try
                    {
                        // for concurrency / duplicate commit detection safe mode is required
                        await PersistedCommits.InsertOneAsync(commitDoc);

                        retry = false;
                        await UpdateStreamHeadAsync(attempt.BucketId, attempt.StreamId, attempt.StreamRevision, attempt.Events.Count);
                        Logger.Debug(Messages.CommitPersisted, attempt.CommitId);
                    }
                    catch (MongoException e)
                    {
                        if (!e.Message.Contains(ConcurrencyException))
                        {
                            throw;
                        }

                        // checkpoint index? 
                        if (e.Message.Contains(MongoCommitIndexes.CheckpointNumber))
                        {
                            commitDoc[MongoCommitFields.CheckpointNumber] = (await _getNextCheckpointNumber()).LongValue;
                        }
                        else
                        {
                            ICommit savedCommit = (await PersistedCommits.Find(attempt.ToMongoCommitIdQuery()).FirstOrDefaultAsync()).ToCommit(_serializer);

                            if (savedCommit.CommitId == attempt.CommitId)
                            {
                                throw new DuplicateCommitException();
                            }
                            Logger.Debug(Messages.ConcurrentWriteDetected);
                            throw new ConcurrencyException();
                        }
                    }
                }

                return commitDoc.ToCommit(_serializer);
            });
        }

        public virtual IAsyncEnumerable<ICommit> GetUndispatchedCommits()
        {
            Logger.Debug(Messages.GettingUndispatchedCommits);

            return TryMongo(() => PersistedCommits
                    .Find(Builders<BsonDocument>.Filter.Eq("Dispatched", false))
                    .Sort(MongoCommitFields.CheckpointNumber)
                    .Project(mc => mc.ToCommit(_serializer)))
                    .ToAsyncEnumerable();
        }

        public virtual Task MarkCommitAsDispatched(ICommit commit)
        {
            Logger.Debug(Messages.MarkingCommitAsDispatched, commit.CommitId);

            return TryMongo(() =>
            {
                var query = commit.ToMongoCommitIdQuery();
                var update = Builders<BsonDocument>.Update.Set(MongoCommitFields.Dispatched, true);
                return PersistedCommits.UpdateOneAsync(query, update);
            });
        }

        public virtual IAsyncEnumerable<IStreamHead> GetStreamsToSnapshot(string bucketId, int maxThreshold)
        {
            Logger.Debug(Messages.GettingStreamsToSnapshot);

            return TryMongo(() =>
            {
                var query = Builders<BsonDocument>.Filter.Gte(MongoStreamHeadFields.Unsnapshotted, maxThreshold);
                return PersistedStreamHeads
                    .Find(query)
                    .Sort(Builders<BsonDocument>.Sort.Descending(MongoStreamHeadFields.Unsnapshotted))
                    .Project(x => x.ToStreamHead());
            })
            .ToAsyncEnumerable();
        }

        public virtual Task<ISnapshot> GetSnapshot(string bucketId, string streamId, int maxRevision)
        {
            Logger.Debug(Messages.GettingRevision, streamId, maxRevision);

            return TryMongo(() => PersistedSnapshots
                .Find(ExtensionMethods.GetSnapshotQuery(bucketId, streamId, maxRevision))
                .Sort(Builders<BsonDocument>.Sort.Descending(MongoShapshotFields.Id))
                .Limit(1)
                .Project(mc => (ISnapshot)mc.ToSnapshot(_serializer))
                .FirstOrDefaultAsync());
        }

        public virtual async Task<bool> AddSnapshot(ISnapshot snapshot)
        {
            if (snapshot == null)
            {
                return false;
            }
            Logger.Debug(Messages.AddingSnapshot, snapshot.StreamId, snapshot.BucketId, snapshot.StreamRevision);
            try
            {
                BsonDocument mongoSnapshot = snapshot.ToMongoSnapshot(_serializer);
                var query = Builders<BsonDocument>.Filter.Eq(MongoShapshotFields.Id, mongoSnapshot[MongoShapshotFields.Id]);
                var update = Builders<BsonDocument>.Update.Set(MongoShapshotFields.Payload, mongoSnapshot[MongoShapshotFields.Payload]);

                // Doing an upsert instead of an insert allows us to overwrite an existing snapshot and not get stuck with a
                // stream that needs to be snapshotted because the insert fails and the SnapshotRevision isn't being updated.
                await PersistedSnapshots.UpdateOneAsync(query, update, new UpdateOptions { IsUpsert = true });

                // More commits could have been made between us deciding that a snapshot is required and writing it so just
                // resetting the Unsnapshotted count may be a little off. Adding snapshots should be a separate process so
                // this is a good chance to make sure the numbers are still in-sync - it only adds a 'read' after all ...
                BsonDocument streamHeadId = GetStreamHeadId(snapshot.BucketId, snapshot.StreamId);
                StreamHead streamHead = (await PersistedStreamHeads.FindAsync((o) => o["_id"] == streamHeadId).FirstOrDefault()).ToStreamHead();
                int unsnapshotted = streamHead.HeadRevision - snapshot.StreamRevision;
                await PersistedStreamHeads.UpdateOneAsync(
                    Builders<BsonDocument>.Filter.Eq(MongoStreamHeadFields.Id, streamHeadId),
                    Builders<BsonDocument>.Update.Set(MongoStreamHeadFields.SnapshotRevision, snapshot.StreamRevision).Set(MongoStreamHeadFields.Unsnapshotted, unsnapshotted));

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public virtual async Task Purge()
        {
            Logger.Warn(Messages.PurgingStorage);
            await PersistedCommits.DeleteManyAsync(o => true);
            await PersistedStreamHeads.DeleteManyAsync(o => true);
            await PersistedSnapshots.DeleteManyAsync(o => true);
        }

        public Task Purge(string bucketId)
        {
            Logger.Warn(Messages.PurgingBucket, bucketId);
            return TryMongo(async () =>
            {
                await PersistedStreamHeads.DeleteManyAsync(Builders<BsonDocument>.Filter.Eq(MongoStreamHeadFields.FullQualifiedBucketId, bucketId));
                await PersistedSnapshots.DeleteManyAsync(Builders<BsonDocument>.Filter.Eq(MongoShapshotFields.FullQualifiedBucketId, bucketId));
                await PersistedCommits.DeleteManyAsync(Builders<BsonDocument>.Filter.Eq(MongoStreamHeadFields.FullQualifiedBucketId, bucketId));
            });

        }

        public Task Drop()
        {
            return Purge();
        }

        public Task DeleteStream(string bucketId, string streamId)
        {
            Logger.Warn(Messages.DeletingStream, streamId, bucketId);
            return TryMongo(async () =>
            {
                await PersistedStreamHeads.DeleteManyAsync(
                    Builders<BsonDocument>.Filter.Eq(MongoStreamHeadFields.Id, new BsonDocument{
                        {MongoStreamHeadFields.BucketId, bucketId},
                        {MongoStreamHeadFields.StreamId, streamId}
                    })
                );

                await PersistedSnapshots.DeleteManyAsync(
                    Builders<BsonDocument>.Filter.Eq(MongoShapshotFields.Id, new BsonDocument{
                        {MongoShapshotFields.BucketId, bucketId},
                        {MongoShapshotFields.StreamId, streamId}
                    })
                );

                await PersistedCommits.UpdateManyAsync(
                    Builders<BsonDocument>.Filter.And(
                        Builders<BsonDocument>.Filter.Eq(MongoCommitFields.BucketId, bucketId),
                        Builders<BsonDocument>.Filter.Eq(MongoCommitFields.StreamId, streamId)
                    ),
                    Builders<BsonDocument>.Update.Set(MongoCommitFields.BucketId, MongoSystemBuckets.RecycleBin)
                );
            });
        }

        public bool IsDisposed
        {
            get { return _disposed; }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || _disposed)
            {
                return;
            }

            Logger.Debug(Messages.ShuttingDownPersistence);
            _disposed = true;
        }

        private Task UpdateStreamHeadAsync(string bucketId, string streamId, int streamRevision, int eventsCount)
        {
            return TryMongo(async () =>
            {
                BsonDocument streamHeadId = GetStreamHeadId(bucketId, streamId);
                await PersistedStreamHeads.UpdateOneAsync(
                    Builders<BsonDocument>.Filter.Eq(MongoStreamHeadFields.Id, streamHeadId),
                    Builders<BsonDocument>.Update
                        .Set(MongoStreamHeadFields.HeadRevision, streamRevision)
                        .Inc(MongoStreamHeadFields.SnapshotRevision, 0)
                        .Inc(MongoStreamHeadFields.Unsnapshotted, eventsCount),
                    new UpdateOptions { IsUpsert = true });
            });
        }


        // Ensure this i cached
        private static Func<Action, bool> _tryMongo = callback => { callback(); return true; };

        protected virtual void TryMongo(Action callback)
        {
            TryMongoCore(callback, _tryMongo);
        }

        protected virtual T TryMongoCore<TState, T>(TState state, Func<TState, T> callback)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("Attempt to use storage after it has been disposed.");
            }
            try
            {
                return callback(state);
            }
            catch (MongoConnectionException e)
            {
                Logger.Warn(Messages.StorageUnavailable);
                throw new StorageUnavailableException(e.Message, e);
            }
            catch (MongoException e)
            {
                Logger.Error(Messages.StorageThrewException, e.GetType());
                throw new StorageException(e.Message, e);
            }
        }
        protected virtual T TryMongo<T>(Func<T> callback)
        {
            return TryMongoCore(callback, cb => cb());
        }

        private static BsonDocument GetStreamHeadId(string bucketId, string streamId)
        {
            var id = new BsonDocument();
            id[MongoStreamHeadFields.BucketId] = bucketId;
            id[MongoStreamHeadFields.StreamId] = streamId;
            return id;
        }

        public Task EmptyRecycleBin()
        {
            var lastCheckpointNumber = _getLastCheckPointNumber();
            return TryMongo(() =>
            {
                return PersistedCommits.DeleteManyAsync(Builders<BsonDocument>.Filter.And(
                    Builders<BsonDocument>.Filter.Eq(MongoCommitFields.BucketId, MongoSystemBuckets.RecycleBin),
                    Builders<BsonDocument>.Filter.Lt(MongoCommitFields.CheckpointNumber, lastCheckpointNumber)
                ));
            });
        }

        public IAsyncEnumerable<ICommit> GetDeletedCommits()
        {
            return TryMongo(() => 
                PersistedCommits
                    .Find(Builders<BsonDocument>.Filter.Eq(MongoCommitFields.BucketId, MongoSystemBuckets.RecycleBin))
                    .Sort(MongoCommitFields.CheckpointNumber)
                    .Project(mc => mc.ToCommit(_serializer))
            )
            .ToAsyncEnumerable();
        }


        
    }
}