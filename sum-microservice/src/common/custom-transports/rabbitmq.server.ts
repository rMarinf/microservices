import { Server, CustomTransportStrategy, MicroserviceOptions } from '@nestjs/microservices';
import { Channel, Connection, Options, connect } from 'amqplib';
import { Observable } from 'rxjs';

// CONSTANTS
export const RQM_DEFAULT_URL = 'amqp://localhost';
export const RQM_DEFAULT_QUEUE = 'default';
export const RQM_DEFAULT_PREFETCH_COUNT = 0;
export const RQM_DEFAULT_IS_GLOBAL_PREFETCH_COUNT = false;
export const RQM_DEFAULT_QUEUE_OPTIONS = {};

// INTERFACE
export interface RmqOptions {
  transport?: 'RMQ';
  options?: {
    url?: string;
    queue?: string;
    prefetchCount?: number;
    isGlobalPrefetchCount?: boolean;
    queueOptions?: Options.AssertQueue;
  };
}

export class ServerRMQ extends Server implements CustomTransportStrategy {
  private server: Connection = null;
  private channel: Channel = null;
  private readonly url: string;
  private readonly queue: string;
  private readonly prefetchCount: number;
  private readonly queueOptions: Options.AssertQueue;
  private readonly isGlobalPrefetchCount: boolean;

  constructor(private readonly options: MicroserviceOptions) {
    super();
    this.url =
      this.getOptionsProp<RmqOptions>(this.options, 'url') || RQM_DEFAULT_URL;
    this.queue =
      this.getOptionsProp<RmqOptions>(this.options, 'queue') || RQM_DEFAULT_QUEUE;
    this.prefetchCount =
      this.getOptionsProp<RmqOptions>(this.options, 'prefetchCount') || RQM_DEFAULT_PREFETCH_COUNT;
    this.isGlobalPrefetchCount =
      this.getOptionsProp<RmqOptions>(this.options, 'isGlobalPrefetchCount') || RQM_DEFAULT_IS_GLOBAL_PREFETCH_COUNT;
    this.queueOptions =
      this.getOptionsProp<RmqOptions>(this.options, 'queueOptions') || RQM_DEFAULT_QUEUE_OPTIONS;
  }

  public async listen(callback: () => void): Promise<void> {
    await this.start(callback);
    this.channel.consume(this.queue, (msg) => this.handleMessage(msg), {
      noAck: true,
    });
  }

  private async start(callback?: () => void) {
    try {
      this.server = await connect(this.url);
      this.channel = await this.server.createChannel();
      this.channel.assertQueue(this.queue, this.queueOptions);
      await this.channel.prefetch(this.prefetchCount, this.isGlobalPrefetchCount);
      callback();
    } catch (err) {
      this.logger.error(err);
    }
  }

  public close(): void {
    this.channel && this.channel.close();
    this.server && this.server.close();
  }

  private async handleMessage(message): Promise<void> {
    const { content, properties } = message;
    const messageObj = JSON.parse(content.toString());
    const handlers = this.getHandlers();
    const pattern = JSON.stringify(messageObj.pattern);
    if (!this.messageHandlers[pattern]) {
      return;
    }
    const handler = this.messageHandlers[pattern];
    const response$ = this.transformToObservable(await handler(messageObj.data)) as Observable<any>;
    response$ && this.send(response$, (data) => this.sendMessage(data, properties.replyTo, properties.correlationId));
  }

  private sendMessage(message, replyTo, correlationId): void {
    const buffer = Buffer.from(JSON.stringify(message));
    this.channel.sendToQueue(replyTo, buffer, { correlationId: correlationId });
  }
}