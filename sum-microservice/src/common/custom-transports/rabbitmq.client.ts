import { Logger } from "@nestjs/common";
import { ClientOptions, ClientProxy, WritePacket } from '@nestjs/microservices';
import { Channel, Connection, Options, connect } from "amqplib";
import { EventEmitter } from "events";

// CONSTANTS
export const RQM_DEFAULT_URL = 'amqp://localhost';
export const RQM_DEFAULT_QUEUE = 'default';
export const RQM_DEFAULT_PREFETCH_COUNT = 0;
export const RQM_DEFAULT_IS_GLOBAL_PREFETCH_COUNT = false;
export const RQM_DEFAULT_QUEUE_OPTIONS = {};
export const ERROR_EVENT = 'error';

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

export class ClientRMQ extends ClientProxy {
  private readonly logger = new Logger(ClientProxy.name);
  private client: Connection = null;
  private publisherChannel: Channel = null;
  private workerChannel: Channel = null;
  private url: string;
  private exchange: string = 'delayed-exchange';
  private queue: string;
  private prefetchCount: number;
  private isGlobalPrefetchCount: boolean;
  private queueOptions: Options.AssertQueue
  private replyQueue: string;
  private responseEmitter: EventEmitter;

  constructor(
    private readonly options: ClientOptions) {
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
    this.connect();
  }

  protected publish(messageObj, callback: (packet: WritePacket) => any) {
    try {
      const correlationId = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
      this.responseEmitter.on(correlationId, msg => {
        console.log('[AMQP] Received msg...');
        const { content } = msg;
        this.handleMessage(content, callback);
      });
      this.publisherChannel.publish(this.exchange, this.queue, Buffer.from(JSON.stringify(messageObj)), {
        replyTo: this.replyQueue,
        correlationId,
        headers: {'x-delay': 10000 },
      });
    } catch (err) {
      console.log(err);
      callback({ err });
    }
  }

  public handleMessage(
    msg: WritePacket,
    callback: (packet: WritePacket) => any,
  ) {
    const { err, response, isDisposed } = JSON.parse(msg.toString());
    if (isDisposed || err) {
      callback({
        err,
        response: null,
        isDisposed: true,
      });
    }
    callback({
      err,
      response,
    });
  }

  public close(): void {
    this.publisherChannel && this.publisherChannel.close();
    this.workerChannel && this.workerChannel.close();
    this.client && this.client.close();
  }

  public handleError(client: Connection): void {
    client.addListener(ERROR_EVENT, err => this.logger.error(err));
  }

  public async start() {
    this.client = await connect(this.url);
    console.log('[AMQP] connected');
  }

  public async configPublisher() {
    this.publisherChannel = await this.client.createConfirmChannel();
    // assert the exchange: 'my-delay-exchange' to be a x-delayed-message,
    await this.publisherChannel.assertExchange(
      this.exchange,
      'x-delayed-message',
      {autoDelete: false, durable: true, arguments: {'x-delayed-type':  'direct'}})
    // Bind the queue: "jobs" to the exchnage: "my-delay-exchange" with the binding key "jobs"
    await this.publisherChannel.bindQueue(this.queue, this.exchange , this.queue);
    console.log('[AMQP] Publisher is started');
  }

  public async configWorker() {
    this.workerChannel = await this.client.createChannel();
    await this.workerChannel.prefetch(this.prefetchCount, this.isGlobalPrefetchCount);
    this.replyQueue = (await this.workerChannel.assertQueue(this.queue, { durable: true })).queue;
    this.listen();
  }

  public listen() {
    this.workerChannel.consume(this.replyQueue, (msg) => {
      console.log('[AMQP] Emit msg...');
      this.responseEmitter.emit(msg.properties.correlationId, msg);
    }, { noAck: true });
  }

  public connect(): Promise<any> {
    if (this.client && this.publisherChannel && this.workerChannel) {
      return Promise.resolve();
    }

    return new Promise(async (resolve, reject) => {
      try {
        // Start RabbitMQ
        await this.start();

        // Config event emitter
        this.responseEmitter = new EventEmitter();
        this.responseEmitter.setMaxListeners(0);

        // Config publisher channel
        await this.configPublisher();

        // Config worker channel
        await this.configWorker();

        resolve();
      }catch (e) {
        reject(e);
      }
    });
  }
}