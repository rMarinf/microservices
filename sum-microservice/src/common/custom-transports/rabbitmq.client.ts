import * as amqp from 'amqplib';
import { ClientProxy, ReadPacket, WritePacket } from '@nestjs/microservices';

export class RabbitMQClient extends ClientProxy {
  private server;
  private channel;

  constructor(
    private readonly host: string,
    private readonly queue: string) {
    super();
  }

  public connect(): Promise<any> {
    return this.connectWithRabbitMq();
  }

  public async close() {
    await this.channel.close();
    await this.server.close();
  }

  protected publish(packet: ReadPacket, callback: (packet: WritePacket) => void): void {
    // console.log(packet, '<<<< packet');
    return this.sendSingleMessage(packet, callback);
  }

  private sendSingleMessage(messageObj, callback: (err, result, disposed?: boolean) => void) {
    const { sub, pub } = this.getQueues();
    this.channel.consume(pub, (message) => this.handleMessage(message, callback), { noAck: true });
    this.channel.sendToQueue(sub, Buffer.from(JSON.stringify(messageObj)));
  }

  private handleMessage(message, callback: (err, result, disposed?: boolean) => void) {
    const { content } = message;
    const { err, response, disposed } = JSON.parse(content.toString());
    if (disposed) {
      this.server.close();
    }
    callback(err, response, disposed);
  }

  private getQueues() {
    return { pub: `${this.queue}_pub`, sub: `${this.queue}_sub` };
  }

  private async connectWithRabbitMq(){
    try {
      // Create server
      this.server = await amqp.connect(this.host);
      // Create channel
      this.channel = await this.server.createChannel();

      const { sub, pub } = this.getQueues();
      // Create queues
      this.channel.assertQueue(sub, { durable: false });
      this.channel.assertQueue(pub, { durable: false });
      return true;
    }catch (e) {
      return Promise.reject(e);
    }
  }
}