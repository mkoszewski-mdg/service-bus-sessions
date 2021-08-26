using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Newtonsoft.Json;

namespace ServiceBusSessions
{
    internal static class Config
    {
        public static string ServiceBusConnectionString = "FILL ME";

        public static string QueueName = "FILL ME";
    }

    internal sealed class SessionMessage
    {
        public string Id { get; set; }
    }

    internal sealed class MessageProcessor
    {
        private static int InstanceCount = 0;

        private readonly int identifier;

        private readonly HashSet<string> sessionIds = new HashSet<string>();

        public MessageProcessor()
        {
            this.identifier = MessageProcessor.InstanceCount++;
        }

        public int Identifier => this.identifier;

        public HashSet<string> SessionIds => this.sessionIds;

        public async Task StartProcessingAsync()
        {
            var serviceBusClient = new ServiceBusClient(Config.ServiceBusConnectionString);
            var sessionProcessor = serviceBusClient.CreateSessionProcessor(Config.QueueName, new ServiceBusSessionProcessorOptions
            {
                SessionIdleTimeout = TimeSpan.FromSeconds(1),
                MaxConcurrentSessions = 2,
            });

            sessionProcessor.ProcessMessageAsync += this.SessionProcessorOnProcessMessageAsync;
            sessionProcessor.ProcessErrorAsync += SessionProcessorOnProcessErrorAsync;
            await sessionProcessor.StartProcessingAsync().ConfigureAwait(false);
        }

        private static SessionMessage DeserializeMessage(BinaryData body)
        {
            var content = Encoding.UTF8.GetString(body);
            return JsonConvert.DeserializeObject<SessionMessage>(content);
        }

        private Task SessionProcessorOnProcessErrorAsync(ProcessErrorEventArgs arg)
        {
            return Task.CompletedTask;
        }

        private async Task SessionProcessorOnProcessMessageAsync(ProcessSessionMessageEventArgs arg)
        {
            var message = MessageProcessor.DeserializeMessage(arg.Message.Body);
            if (!this.sessionIds.Contains(arg.SessionId))
            {
                this.sessionIds.Add(arg.SessionId);
            }

            Console.WriteLine($"Processor {this.identifier} received message with id {message.Id} and session {arg.SessionId}");
            //await Task.Delay(new Random().Next(3000, 6000)).ConfigureAwait(false);
            await arg.CompleteMessageAsync(arg.Message).ConfigureAwait(false);
            //Console.WriteLine(arg.SessionLockedUntil - DateTimeOffset.Now);
        }
    }

    internal sealed class SessionMessageSender
    {
        private static int InstanceCount = 0;
        private static int MessageCount = 0;

        private readonly int identifier;

        public SessionMessageSender()
        {
            this.identifier = SessionMessageSender.InstanceCount++;
        }

        public async Task SendMessagesAsync(CancellationToken token)
        {
            var serviceBusClient = new ServiceBusClient(Config.ServiceBusConnectionString);
            var serviceBusSender = serviceBusClient.CreateSender(Config.QueueName);
            var random = new Random();

            while (!token.IsCancellationRequested)
            {
                var message = SessionMessageSender.CreateMessage();
                var messageBody = JsonConvert.SerializeObject(message);
                var serviceBusMessage = new ServiceBusMessage(messageBody)
                {
                    SessionId = this.identifier.ToString(),
                };
                await serviceBusSender.SendMessageAsync(serviceBusMessage).ConfigureAwait(false);
                await Task.Delay(random.Next(2000, 5000)).ConfigureAwait(false);
            }
        }

        private static SessionMessage CreateMessage()
        {
            var number = SessionMessageSender.MessageCount++;
            return new SessionMessage
            {
                Id = number.ToString(),
            };
        }
    }

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var cancellationTokenSource = new CancellationTokenSource();

            var keyBoardTask = Task.Run(() =>
            {
                var key = Console.ReadKey();
                cancellationTokenSource.Cancel();
            });

            var processors = new List<MessageProcessor>
            {
                new MessageProcessor(),
                new MessageProcessor(),
            };

            var tasks = new List<Task>
            {
                keyBoardTask,
            };

            foreach (var processor in processors)
            {
                tasks.Add(processor.StartProcessingAsync());
            }

            var senders = new List<SessionMessageSender>
            {
                new SessionMessageSender(),
                new SessionMessageSender(),
                new SessionMessageSender(),
                new SessionMessageSender()
            };

            foreach (var sender in senders)
            {
                tasks.Add(sender.SendMessagesAsync(cancellationTokenSource.Token));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            foreach (var processor in processors)
            {
                Console.WriteLine($"Processor {processor.Identifier} received messages from sessions: {string.Join(", ", processor.SessionIds)}");
            }
        }
    }
}
