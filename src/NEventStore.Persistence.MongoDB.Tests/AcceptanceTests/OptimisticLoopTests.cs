
namespace NEventStore.Persistence.MongoDB.Tests.AcceptanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Diagnostics;
    using NEventStore.Client;
    using NEventStore.Diagnostics;
    using NEventStore.Persistence.AcceptanceTests;
    using NEventStore.Persistence.AcceptanceTests.BDD;
    using Xunit;

    using System.Threading.Tasks;
    using ALinq;
    using FluentAssertions;

    public class Observer : IObserver<ICommit>
    {
        private int _counter;

        public int Counter
        {
            get { return _counter; }
        }

        private long _lastCheckpoint;

        public void OnNext(ICommit value)
        {
            var chkpoint = LongCheckpoint.Parse(value.CheckpointToken).LongValue;
            if (chkpoint  > _lastCheckpoint)
                _counter++;

            _lastCheckpoint = chkpoint;
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }

    public class when_a_reader_observe_commits_from_a_lot_of_writers : SpecificationBase
    {
        protected const int IterationsPerWriter = 40;
        protected const int ParallelWriters = 60;
        protected const int PollingInterval = 1;
        readonly IList<IPersistStreams> _writers = new List<IPersistStreams>();
        private PollingClient _client;
        private Observer _observer;
        private IObserveCommits _observeCommits;
        private IDisposable _subscription;

        protected override async Task Context()
        {
            for (int c = 1; c <= ParallelWriters; c++)
            {
                var client = new AcceptanceTestMongoPersistenceFactory().Build();

                if (c == 1)
                {
                    await client.Drop();
                    await client.Initialize();
                }

                _writers.Add(client);
            }

            _observer = new Observer();

            var reader = new AcceptanceTestMongoPersistenceFactory().Build();
            _client = new PollingClient(reader, PollingInterval);

            _observeCommits = _client.ObserveFrom(null);
            _subscription = _observeCommits.Subscribe(_observer);
            await _observeCommits.Start();
        }

        protected override async Task Because()
        {
            var start = new SemaphoreSlim(0);
            //long counter = 0;
            var rnd = new Random(DateTime.Now.Millisecond);

            var runners = Enumerable
                .Range(0, ParallelWriters)
                .Select(i => Task.Run(async () =>
                {
                    await start.WaitAsync();
                    for (int c = 0; c < IterationsPerWriter; c++)
                    {
                        try
                        {
                            await _writers[i].Commit(Guid.NewGuid().ToString().BuildAttempt());
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine(ex.Message);
                            throw;
                        }
                        await Task.Delay(rnd.Next(2));
                    }
                }));


            start.Release(ParallelWriters);
            await Task.WhenAll(runners);

            await Task.Delay(1500);
            _subscription.Dispose();
        }

        [Fact]
        public void should_never_miss_a_commit()
        {
            _observer.Counter.Should().Be(IterationsPerWriter * ParallelWriters);
        }

        protected override async Task Cleanup()
        {
            for (int c = 0; c < ParallelWriters; c++)
            {
                if (c == ParallelWriters - 1)
                    await _writers[c].Drop();

                _writers[c].Dispose();
            }
        }
    }

    public class when_first_commit_is_persisted : PersistenceEngineConcern
    {
        ICommit _commit;
        protected override Task Context()
        {
            return Task.FromResult(true);
        }

        protected override async Task Because()
        {
            _commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
        }

        [Fact]
        public void should_have_checkpoint_equal_to_one()
        {
            LongCheckpoint.Parse(_commit.CheckpointToken).LongValue.Should().Be(1);
        }
    }

    public class when_second_commit_is_persisted : PersistenceEngineConcern
    {
        ICommit _commit;
        protected override Task Context()
        {
            return Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
        }

        protected override async Task Because()
        {
            _commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
        }

        [Fact]
        public void should_have_checkpoint_equal_to_two()
        {
            LongCheckpoint.Parse(_commit.CheckpointToken).LongValue.Should().Be(2);
        }

    }

    public class when_commit_is_persisted_after_a_stream_deletion : PersistenceEngineConcern
    {
        ICommit _commit;
        protected override async Task Context()
        {
            var commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            await Persistence.DeleteStream(commit.BucketId, commit.StreamId);
        }

        protected override async Task Because()
        {
            _commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
        }

        [Fact]
        public void should_have_checkpoint_equal_to_two()
        {
            LongCheckpoint.Parse(_commit.CheckpointToken).LongValue.Should().Be(2);
        }
    }

    public class when_commit_is_persisted_after_concurrent_insertions_and_deletions : PersistenceEngineConcern
    {
        const int Iterations = 10;
        const int Clients = 10;
        string _checkpointToken;

        protected override async Task Context()
        {
            var lazyInitializer = Persistence;

            var start = new SemaphoreSlim(0);
            
            var runners = Enumerable
                .Range(0, Clients)
                .Select(c => Task.Run(async () =>
                {
                    await start.WaitAsync();
                    for (int i = 0; i < Iterations; i++)
                    {
                        var commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
                        await Persistence.DeleteStream(commit.BucketId, commit.StreamId);
                    }
                }));

            

            start.Release(Clients);
            await Task.WhenAll(runners);
        }

        protected override async Task Because()
        {
            _checkpointToken = (await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt())).CheckpointToken;
        }

        [Fact]
        public void should_have_correct_checkpoint()
        {
            LongCheckpoint.Parse(_checkpointToken).LongValue.Should().Be(Clients * Iterations + 1);
        }
    }

    public class when_a_stream_is_deleted : PersistenceEngineConcern
    {
        ICommit _commit;

        protected override async Task Context()
        {
            _commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
        }

        protected override Task Because()
        {
            return Persistence.DeleteStream(_commit.BucketId, _commit.StreamId);
        }

        [Fact]
        public async Task the_commits_cannot_be_loaded_from_the_stream()
        {
            (await Persistence.GetFrom(_commit.StreamId, int.MinValue, int.MaxValue).ToList()).Should().BeEmpty();
        }

        [Fact]
        public async Task the_commits_cannot_be_loaded_from_the_bucket()
        {
            (await Persistence.GetFrom(_commit.BucketId,DateTime.MinValue).ToList()).Should().BeEmpty();
        }

        [Fact]
        public async Task the_commits_cannot_be_loaded_from_the_checkpoint()
        {
            const string origin = null;
            (await Persistence.GetFrom(origin).ToList()).Should().BeEmpty();
        }

        [Fact]
        public async Task the_commits_cannot_be_loaded_from_bucket_and_start_date()
        {
            (await Persistence.GetFrom(_commit.BucketId,DateTime.MinValue).ToList()).Should().BeEmpty();
        }

        [Fact]
        public async Task the_commits_cannot_be_loaded_from_bucket_and_date_range()
        {
            (await Persistence.GetFromTo(_commit.BucketId, DateTime.MinValue, DateTime.MaxValue).ToList()).Should().BeEmpty();
        }
    }

    public class when_deleted_streams_are_purged_and_last_commit_is_marked_as_deleted : PersistenceEngineConcern
    {
        ICommit[] _commits;

        protected override async Task Context()
        {
            await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            var commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            await Persistence.DeleteStream(commit.BucketId, commit.StreamId);
            await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            await Persistence.DeleteStream(commit.BucketId, commit.StreamId);
        }

        protected override async Task Because()
        {
            var mongoEngine = (MongoPersistenceEngine)(((PerformanceCounterPersistenceEngine)Persistence).UnwrapPersistenceEngine());
            await mongoEngine.EmptyRecycleBin();
            _commits = await mongoEngine.GetDeletedCommits().ToArray();
        }

        [Fact]
        public void last_deleted_commit_is_not_purged_to_preserve_checkpoint_numbering()
        {
            _commits.Length.Should().Be(1);
        }

        [Fact]
        public void last_deleted_commit_has_the_higher_checkpoint_number()
        {
            LongCheckpoint.Parse(_commits[0].CheckpointToken).LongValue.Should().Be(4);
        }
    }

    public class when_deleted_streams_are_purged : PersistenceEngineConcern
    {
        ICommit[] _commits;

        protected override async Task Context()
        {
            await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            var commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            await Persistence.DeleteStream(commit.BucketId, commit.StreamId);
            commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            await Persistence.DeleteStream(commit.BucketId, commit.StreamId);
            await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
        }

        protected override async Task Because()
        {
            var mongoEngine = (MongoPersistenceEngine)(((PerformanceCounterPersistenceEngine)Persistence).UnwrapPersistenceEngine());
            await mongoEngine.EmptyRecycleBin();
            _commits = await mongoEngine.GetDeletedCommits().ToArray();
        }

        [Fact]
        public void all_deleted_commits_are_purged()
        {
            _commits.Length.Should().Be(0);
        }
    }

    public class when_stream_is_added_after_a_bucket_purge : PersistenceEngineConcern
    {
        LongCheckpoint _checkpointBeforePurge;
        LongCheckpoint _checkpointAfterPurge;

        protected override async Task Context()
        {
            var commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            _checkpointBeforePurge = LongCheckpoint.Parse(commit.CheckpointToken);
            await Persistence.DeleteStream(commit.StreamId);
            await Persistence.Purge("default");
        }

        protected override async Task Because()
        {
            var commit = await Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt());
            _checkpointAfterPurge = LongCheckpoint.Parse(commit.CheckpointToken);
        }

        [Fact]
        public void checkpoint_number_must_be_greater_than ()
        {
            _checkpointAfterPurge.LongValue.Should().BeGreaterThan(_checkpointBeforePurge.LongValue);
        }
    }

    public class when_a_stream_with_two_or_more_commits_is_deleted : PersistenceEngineConcern
    {
        private string _streamId;
        private string _bucketId;

        protected override async Task Context()
        {
            _streamId = Guid.NewGuid().ToString();
            var commit = await Persistence.Commit(_streamId.BuildAttempt());
            _bucketId = commit.BucketId;

            await Persistence.Commit(commit.BuildNextAttempt());
        }

        protected override Task Because()
        {
            return Persistence.DeleteStream(_bucketId, _streamId);
        }

        [Fact]
        public async Task all_commits_are_deleted()
        {
            var commits = await Persistence.GetFrom(_bucketId, _streamId, int.MinValue, int.MaxValue).ToArray();

            Assert.Equal(0, commits.Length);
        }
    }
}
