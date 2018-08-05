import { Controller, Get } from '@nestjs/common';
import {
  ClientProxy,
  Client,
  Transport,
  MessagePattern,
} from '@nestjs/microservices';
import { Observable } from 'rxjs';

interface Divide {
  divide: number;
}

@Controller()
export class MathController {
  // client: ClientProxy = new RabbitMQClient('amqp://localhost:5672', 'channel');

  // TODO : CODIGO PARA CONECTARLOS VIA REDIS
  @Client({
    transport: Transport.REDIS,
    options: {
      url: 'redis://localhost:6379',
    },
  })
  client: ClientProxy;

  // TODO: CODIGO PARA CONECTARLOS VIA TCP
  // @Client({
  //   transport: Transport.TCP,
  //   options: {
  //     port: 3002,
  //   },
  // })
  // client: ClientProxy;

  @Get()
  call(): Observable<number> {
    const pattern = { cmd: 'sum' };
    const data = [1, 2, 3, 4, 5];
    return this.client.send<number>(pattern, data);
  }

  @MessagePattern({ cmd: 'divide' })
  sum(data): Divide {
    const divide = data.sum / 3;
    console.log(divide, '<<<<< SUMAR LLAMA A DIVIDIR ???');
    return { divide };
  }
}
